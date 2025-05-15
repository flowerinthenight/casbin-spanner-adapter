package spanneradapter

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"

	"cloud.google.com/go/spanner"
	dbv1 "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/persist"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type Option interface {
	Apply(*Adapter)
}

type withTableName string

func (w withTableName) Apply(a *Adapter) { a.table = string(w) }

// WithTableName sets adapter's internal table name. Default is 'casbin_rule'.
func WithTableName(v string) Option { return withTableName(v) }

type withSkipDatabaseCreation bool

func (w withSkipDatabaseCreation) Apply(a *Adapter) { a.skipDbCreate = bool(w) }

// WithSkipDatabaseCreation allows caller to skip the database creation.
func WithSkipDatabaseCreation(v bool) Option { return withSkipDatabaseCreation(v) }

type withSkipTableCreation bool

func (w withSkipTableCreation) Apply(a *Adapter) { a.skipTableCreate = bool(w) }

// WithSkipTableCreation allows caller to skip the table creation.
func WithSkipTableCreation(v bool) Option { return withSkipTableCreation(v) }

type withDatabaseAdminClient struct{ c *dbv1.DatabaseAdminClient }

func (w withDatabaseAdminClient) Apply(a *Adapter) { a.admin = w.c }

// WithDatabaseAdminClient sets the adapter's database client. If not provided, an internal client is
// created using the environment's default credentials.
func WithDatabaseAdminClient(c *dbv1.DatabaseAdminClient) Option { return withDatabaseAdminClient{c} }

type withSpannerClient struct{ c *spanner.Client }

func (w withSpannerClient) Apply(a *Adapter) { a.client = w.c }

// WithSpannerClient sets the adapter's Spanner client. If not provided, an
// internal client is created using the environment's default credentials.
func WithSpannerClient(c *spanner.Client) Option { return withSpannerClient{c} }

type withLogger struct{ l *log.Logger }

func (w withLogger) Apply(a *Adapter) { a.logger = w.l }

// WithLogger sets the adapter's logger object.
func WithLogger(v *log.Logger) Option { return withLogger{v} }

// Adapter represents a Cloud Spanner-based adapter for policy storage.
type Adapter struct {
	database        string
	table           string
	skipDbCreate    bool
	skipTableCreate bool
	filtered        bool
	admin           *dbv1.DatabaseAdminClient
	client          *spanner.Client
	internalAdmin   bool // in finalizer, close 'admin' only when internal
	internalClient  bool // in finalizer, close 'client' only when internal
	logger          *log.Logger
}

// NewAdapter creates an Adapter instance. Use the "projects/{project}/instances/{instance}/databases/{db}"
// format for 'db'. Instance creation is not supported. If database creation is not skipped, it will attempt
// to create the database. If table creation is not skipped, it will attempt to create the table as well.
func NewAdapter(db string, opts ...Option) (*Adapter, error) {
	if db == "" {
		return nil, fmt.Errorf("database cannot be empty")
	}

	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return nil, fmt.Errorf("invalid database format: %v", db)
	}

	a := &Adapter{database: db}
	for _, opt := range opts {
		opt.Apply(a)
	}

	if a.table == "" {
		a.table = "casbin_rule"
	}

	if a.logger == nil {
		prefix := "[casbin-spanner-adapter] "
		a.logger = log.New(os.Stdout, prefix, log.LstdFlags)
	}

	var err error
	ctx := context.Background()
	if a.admin == nil {
		a.internalAdmin = true
		a.admin, err = dbv1.NewDatabaseAdminClient(ctx)
		if err != nil {
			return nil, err
		}
	}

	if a.client == nil {
		a.internalClient = true
		a.client, err = spanner.NewClient(ctx, a.database)
		if err != nil {
			return nil, err
		}
	}

	if err = func() error { // create db if needed
		if a.skipDbCreate {
			return nil
		}

		_, err := a.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: a.database})
		if err != nil {
			var q strings.Builder
			fmt.Fprintf(&q, "CREATE DATABASE %s", matches[2])
			op, err := a.admin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
				Parent:          matches[1],
				CreateStatement: q.String(),
			})

			if err != nil {
				return err
			}

			if _, err := op.Wait(ctx); err != nil {
				return err
			}
		}

		return nil
	}(); err != nil {
		return nil, err
	}

	tableExists := func() bool {
		var q strings.Builder
		fmt.Fprintf(&q, "select t.table_name ")
		fmt.Fprintf(&q, "from information_schema.tables as t ")
		fmt.Fprintf(&q, "where t.table_catalog = '' ")
		fmt.Fprintf(&q, "and t.table_schema = '' ")
		fmt.Fprintf(&q, "and t.table_name = @name")

		stmt := spanner.Statement{
			SQL:    q.String(),
			Params: map[string]interface{}{"name": a.table},
		}

		var found bool
		iter := a.client.Single().Query(ctx, stmt)
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}

			if err != nil {
				a.logger.Println(err)
				break
			}

			var tbl string
			err = row.Columns(&tbl)
			if err != nil {
				a.logger.Println(err)
				break
			}

			if tbl == a.table {
				found = true
			}
		}

		return found
	}

	if err = func() error { // create table if needed
		if a.skipTableCreate || tableExists() {
			return nil
		}

		op, err := a.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   a.database,
			Statements: []string{a.createTableSql()},
		})

		if err != nil {
			return err
		}

		if err := op.Wait(ctx); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		return nil, err
	}

	// Call this destructor when adapter is released.
	runtime.SetFinalizer(a, func(adapter *Adapter) {
		if adapter.client != nil && adapter.internalClient {
			adapter.client.Close()
		}

		if adapter.admin != nil && adapter.internalAdmin {
			adapter.admin.Close()
		}
	})

	return a, nil
}

// LoadPolicy loads policy from database. Implements casbin Adapter interface.
func (a *Adapter) LoadPolicy(cmodel model.Model) error {
	casbinRules := []CasbinRule{}
	var q strings.Builder
	fmt.Fprintf(&q, "select ptype, v0, v1, v2, v3, v4, v5 from %s", a.table)
	ctx := context.Background()
	stmt := spanner.Statement{SQL: q.String()}
	iter := a.client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			a.logger.Println(err)
			break
		}

		var v CasbinRule
		err = row.ToStruct(&v)
		if err != nil {
			a.logger.Println(err)
			break
		}

		casbinRules = append(casbinRules, v)
	}

	for _, cr := range casbinRules {
		persist.LoadPolicyLine(cr.ToString(), cmodel)
	}

	return nil
}

// SavePolicy saves policy to database. Implements casbin Adapter interface.
func (a *Adapter) SavePolicy(cmodel model.Model) error {
	err := a.recreateTable()
	if err != nil {
		return err
	}

	casbinRules := []CasbinRule{}
	for ptype, ast := range cmodel["p"] {
		for _, rule := range ast.Policy {
			casbinRule := a.genPolicyLine(ptype, rule)
			casbinRules = append(casbinRules, casbinRule)
		}
	}

	for ptype, ast := range cmodel["g"] {
		for _, rule := range ast.Policy {
			casbinRule := a.genPolicyLine(ptype, rule)
			casbinRules = append(casbinRules, casbinRule)
		}
	}

	type mut_t struct {
		limit int
		mut   *spanner.Mutation
	}

	ctx := context.Background()
	done := make(chan error, 1)
	ch := make(chan *mut_t)

	// Start loading to Spanner. Let's do batch write in case data
	// is way more than Spanner's mutation limits.
	go func() {
		muts := []*spanner.Mutation{}
		var cnt int
		for {
			m := <-ch
			cnt++
			if m == nil {
				var err error
				if cnt > 0 {
					_, err = a.client.Apply(ctx, muts)
				}

				done <- err
				return
			}

			muts = append(muts, m.mut)
			if cnt >= m.limit {
				_, err := a.client.Apply(ctx, muts)
				if err != nil {
					done <- err
					return
				}

				muts = []*spanner.Mutation{}
				cnt = 0
			}
		}
	}()

	func() {
		defer func() { ch <- nil }() // terminate receiver
		cols := []string{"ptype", "v0", "v1", "v2", "v3", "v4", "v5"}
		limit := (20000 / len(cols)) - 3
		for _, cr := range casbinRules {
			ch <- &mut_t{
				limit: limit,
				mut: spanner.InsertOrUpdate(a.table, cols, []interface{}{
					cr.PType, cr.V0, cr.V1, cr.V2, cr.V3, cr.V4, cr.V5,
				}),
			}
		}
	}()

	return <-done
}

// AddPolicy adds a policy rule to the storage. Part of the auto-save feature.
func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	casbinRule := a.genPolicyLine(ptype, rule)
	_, err := a.client.ReadWriteTransaction(context.Background(),
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var q strings.Builder
			fmt.Fprintf(&q, "insert %s ", a.table)
			fmt.Fprintf(&q, "(ptype, v0, v1, v2, v3, v4, v5) ")
			fmt.Fprintf(&q, "values (@ptype, @v0, @v1, @v2, @v3, @v4, @v5)")

			stmt := spanner.Statement{
				SQL: q.String(),
				Params: map[string]interface{}{
					"ptype": casbinRule.PType,
					"v0":    casbinRule.V0,
					"v1":    casbinRule.V1,
					"v2":    casbinRule.V2,
					"v3":    casbinRule.V3,
					"v4":    casbinRule.V4,
					"v5":    casbinRule.V5,
				},
			}

			_, err := txn.Update(ctx, stmt)
			return err
		},
	)
	if err != nil {
		a.logger.Printf("Failed to add policy: %v", err)
	} else {
		a.logger.Printf("Successfully added policy: %v", casbinRule.ToString())
	}

	return err
}

// RemovePolicy removes a policy rule from the storage. Part of the auto-save feature.
func (a *Adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	casbinRule := a.genPolicyLine(ptype, rule)
	_, err := a.client.ReadWriteTransaction(context.Background(),
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			var q strings.Builder
			stmt := spanner.Statement{
				SQL:    q.String(),
				Params: map[string]interface{}{},
			}
			fmt.Fprintf(&q, "delete from %s ", a.table)
			if casbinRule.PType.Valid {
				fmt.Fprintf(&q, "where ptype = @ptype ")
				stmt.Params["ptype"] = casbinRule.PType
			} else {
				fmt.Fprintf(&q, "where ptype is null ")
			}

			if casbinRule.V0.Valid {
				fmt.Fprintf(&q, "and v0 = @v0 ")
				stmt.Params["v0"] = casbinRule.V0
			} else {
				fmt.Fprintf(&q, "and v0 is null ")
			}

			if casbinRule.V1.Valid {
				fmt.Fprintf(&q, "and v1 = @v1 ")
				stmt.Params["v1"] = casbinRule.V1
			} else {
				fmt.Fprintf(&q, "and v1 is null ")
			}

			if casbinRule.V2.Valid {
				fmt.Fprintf(&q, "and v2 = @v2 ")
				stmt.Params["v2"] = casbinRule.V2
			} else {
				fmt.Fprintf(&q, "and v2 is null ")
			}

			if casbinRule.V3.Valid {
				fmt.Fprintf(&q, "and v3 = @v3 ")
				stmt.Params["v3"] = casbinRule.V3
			} else {
				fmt.Fprintf(&q, "and v3 is null ")
			}

			if casbinRule.V4.Valid {
				fmt.Fprintf(&q, "and v4 = @v4 ")
				stmt.Params["v4"] = casbinRule.V4
			} else {
				fmt.Fprintf(&q, "and (v4 is null or v4 = '') ")
			}

			if casbinRule.V5.Valid {
				fmt.Fprintf(&q, "and v5 = @v5 ")
				stmt.Params["v5"] = casbinRule.V5
			} else {
				fmt.Fprintf(&q, "and (v5 is null or v5 = '') ")
			}

			stmt.SQL = q.String()
			a.logger.Printf("Delete statement: %v", stmt.SQL)
			r, err := txn.Update(ctx, stmt)
			a.logger.Printf("Deleted %v rows", r)
			return err
		},
	)
	if err != nil {
		a.logger.Printf("Failed to remove policy: %v", err)
	} else {
		a.logger.Printf("Successfully removed policy rule: %v", casbinRule.ToString())
	}

	return err
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
// Part of the auto-save feature.
func (a *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	selector := make(map[string]interface{})
	if fieldIndex <= 0 && 0 < fieldIndex+len(fieldValues) {
		if fieldValues[0-fieldIndex] != "" {
			selector["v0"] = fieldValues[0-fieldIndex]
		}
	}

	if fieldIndex <= 1 && 1 < fieldIndex+len(fieldValues) {
		if fieldValues[1-fieldIndex] != "" {
			selector["v1"] = fieldValues[1-fieldIndex]
		}
	}

	if fieldIndex <= 2 && 2 < fieldIndex+len(fieldValues) {
		if fieldValues[2-fieldIndex] != "" {
			selector["v2"] = fieldValues[2-fieldIndex]
		}
	}

	if fieldIndex <= 3 && 3 < fieldIndex+len(fieldValues) {
		if fieldValues[3-fieldIndex] != "" {
			selector["v3"] = fieldValues[3-fieldIndex]
		}
	}

	if fieldIndex <= 4 && 4 < fieldIndex+len(fieldValues) {
		if fieldValues[4-fieldIndex] != "" {
			selector["v4"] = fieldValues[4-fieldIndex]
		}
	}

	if fieldIndex <= 5 && 5 < fieldIndex+len(fieldValues) {
		if fieldValues[5-fieldIndex] != "" {
			selector["v5"] = fieldValues[5-fieldIndex]
		}
	}

	sql := `delete from ` + a.table + ` where ptype = @ptype`
	params := map[string]interface{}{"ptype": ptype}
	for k, v := range selector {
		sql = sql + fmt.Sprintf(" and %s = @%s", k, k)
		params[k] = v
	}

	_, err := a.client.ReadWriteTransaction(context.Background(),
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err := txn.Update(ctx, spanner.Statement{SQL: sql, Params: params})
			return err
		},
	)
	if err != nil {
		a.logger.Printf("Failed to remove filtered policy: %v", err)
	} else {
		a.logger.Printf("Successfully removed filtered policy: %v", selector)
	}

	return err
}

func (a *Adapter) recreateTable() error {
	ctx := context.Background()
	op, err := a.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: a.database,
		Statements: []string{
			fmt.Sprintf("DROP TABLE %s", a.table),
			a.createTableSql(),
		},
	})

	if err != nil {
		return err
	}

	if err := op.Wait(ctx); err != nil {
		return err
	}

	return nil
}

func (a *Adapter) createTableSql() string {
	var q strings.Builder
	fmt.Fprintf(&q, "create table %s (", a.table)
	fmt.Fprintf(&q, "ptype string(MAX), ")
	fmt.Fprintf(&q, "v0 string(MAX), ")
	fmt.Fprintf(&q, "v1 string(MAX), ")
	fmt.Fprintf(&q, "v2 string(MAX), ")
	fmt.Fprintf(&q, "v3 string(MAX), ")
	fmt.Fprintf(&q, "v4 string(MAX), ")
	fmt.Fprintf(&q, "v5 string(MAX)) ")
	fmt.Fprintf(&q, "primary key (ptype, v0, v1, v2, v3, v4, v5)")
	return q.String()
}

func (a *Adapter) genPolicyLine(ptype string, rule []string) CasbinRule {
	line := CasbinRule{PType: spanner.NullString{StringVal: ptype, Valid: true}}
	l := len(rule)
	if l > 0 {
		line.V0 = spanner.NullString{StringVal: rule[0], Valid: true}
	}

	if l > 1 {
		line.V1 = spanner.NullString{StringVal: rule[1], Valid: true}
	}

	if l > 2 {
		line.V2 = spanner.NullString{StringVal: rule[2], Valid: true}
	}

	if l > 3 {
		line.V3 = spanner.NullString{StringVal: rule[3], Valid: true}
	}

	if l > 4 {
		line.V4 = spanner.NullString{StringVal: rule[4], Valid: true}
	}

	if l > 5 {
		line.V5 = spanner.NullString{StringVal: rule[5], Valid: true}
	}

	a.logger.Printf("Generated policy line: %v", line.ToString())
	return line
}

type CasbinRule struct {
	PType spanner.NullString `spanner:"ptype"`
	V0    spanner.NullString `spanner:"v0"`
	V1    spanner.NullString `spanner:"v1"`
	V2    spanner.NullString `spanner:"v2"`
	V3    spanner.NullString `spanner:"v3"`
	V4    spanner.NullString `spanner:"v4"`
	V5    spanner.NullString `spanner:"v5"`
}

func (c CasbinRule) ToString() string {
	var sb strings.Builder
	sep := ", "

	sb.WriteString(c.PType.StringVal)
	if len(c.V0.StringVal) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V0.StringVal)
	}

	if len(c.V1.StringVal) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V1.StringVal)
	}

	if len(c.V2.StringVal) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V2.StringVal)
	}

	if len(c.V3.StringVal) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V3.StringVal)
	}

	if len(c.V4.StringVal) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V4.StringVal)
	}

	if len(c.V5.StringVal) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V5.StringVal)
	}

	return sb.String()
}
