package spanneradapter

import (
	"context"
	"fmt"
	"log"
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

type CasbinRule struct {
	PType string `spanner:"ptype"`
	V0    string `spanner:"v0"`
	V1    string `spanner:"v1"`
	V2    string `spanner:"v2"`
	V3    string `spanner:"v3"`
	V4    string `spanner:"v4"`
	V5    string `spanner:"v5"`
}

func (c CasbinRule) ToString() string {
	var sb strings.Builder
	sep := ", "

	sb.WriteString(c.PType)
	if len(c.V0) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V0)
	}

	if len(c.V1) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V1)
	}

	if len(c.V2) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V2)
	}

	if len(c.V3) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V3)
	}

	if len(c.V4) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V4)
	}

	if len(c.V5) > 0 {
		sb.WriteString(sep)
		sb.WriteString(c.V5)
	}

	return sb.String()
}

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
}

// NewAdapterOptions is the options you provide to NewAdapter().
type NewAdapterOptions struct {
	TableName            string                    // if not provided, default table will be 'casbin_rule'
	SkipDatabaseCreation bool                      // if true, skip the database creation part in NewAdapter
	SkipTableCreation    bool                      // if true, skip the table creation part in NewAdapter
	AdminClient          *dbv1.DatabaseAdminClient // if non-nil, will use as the database admin client
	Client               *spanner.Client           // if provided, will use this connection instead
}

// NewAdapter is the constructor for Adapter. Use the "projects/{project}/instances/{instance}/databases/{db}"
// format for 'db'. Instance creation is not supported. If database creation is not skipped, it will attempt
// to create the database. If table creation is not skipped, it will attempt to create the table specified in
// 'opts[0].TableName', or 'casbin_rule' if not provided.
func NewAdapter(db string, opts ...NewAdapterOptions) (*Adapter, error) {
	if db == "" {
		return nil, fmt.Errorf("database cannot be empty")
	}

	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return nil, fmt.Errorf("invalid database format: %v", db)
	}

	a := &Adapter{database: db}
	if len(opts) > 0 {
		a.table = opts[0].TableName
		a.skipDbCreate = opts[0].SkipDatabaseCreation
		a.skipTableCreate = opts[0].SkipTableCreation
		a.admin = opts[0].AdminClient
		a.client = opts[0].Client
	}

	if a.table == "" {
		a.table = "casbin_rule"
	}

	var err error
	ctx := context.Background()
	if a.admin == nil {
		a.internalAdmin = true
		a.admin, err = dbv1.NewDatabaseAdminClient(ctx)
		if err != nil {
			log.Printf("NewDatabaseAdminClient failed: %v", err)
			return nil, err
		}
	}

	if a.client == nil {
		a.internalClient = true
		a.client, err = spanner.NewClient(ctx, a.database)
		if err != nil {
			log.Printf("spanner.NewClient failed: %v", err)
			return nil, err
		}
	}

	if !a.skipDbCreate {
		_, err := a.admin.GetDatabase(ctx, &adminpb.GetDatabaseRequest{Name: a.database})
		if err != nil {
			op, err := a.admin.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
				Parent:          matches[1],
				CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
			})

			if err != nil {
				log.Printf("CreateDatabase failed: %v", err)
				return nil, err
			}

			if _, err := op.Wait(ctx); err != nil {
				log.Printf("Wait on CreateDatabase failed: %v", err)
				return nil, err
			}
		}
	}

	tableExists := func() bool {
		sql := `
select t.table_name
from information_schema.tables as t
where t.table_catalog = ''
  and t.table_schema = ''
  and t.table_name = @name`

		stmt := spanner.Statement{
			SQL:    sql,
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
				log.Println(err)
				break
			}

			var tbl string
			err = row.Columns(&tbl)
			if err != nil {
				log.Println(err)
				break
			}

			if tbl == a.table {
				found = true
			}
		}

		return found
	}

	if !a.skipTableCreate {
		if !tableExists() {
			log.Printf("create table %v", a.table)
			op, err := a.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
				Database:   a.database,
				Statements: []string{a.createTableSql()},
			})

			if err != nil {
				log.Printf("UpdateDatabaseDdl failed: %v", err)
				return nil, err
			}

			if err := op.Wait(ctx); err != nil {
				log.Printf("Wait on UpdateDatabaseDdl failed: %v", err)
				return nil, err
			}
		}
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

// LoadPolicy(model model.Model) error
// SavePolicy(model model.Model) error

// This is part of the Auto-Save feature.
// AddPolicy(sec string, ptype string, rule []string) error
// RemovePolicy(sec string, ptype string, rule []string) error
// RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error

// LoadPolicy loads policy from database.
// Implements casbin Adapter interface.
func (a *Adapter) LoadPolicy(cmodel model.Model) error {
	casbinRules := []CasbinRule{}
	stmt := spanner.Statement{
		SQL: `select ptype, v0, v1, v2, v3, v4, v5 from ` + a.table,
	}

	ctx := context.Background()
	iter := a.client.Single().Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Println(err)
			break
		}

		var v CasbinRule
		err = row.ToStruct(&v)
		if err != nil {
			log.Println(err)
			break
		}

		casbinRules = append(casbinRules, v)
	}

	for _, cr := range casbinRules {
		persist.LoadPolicyLine(cr.ToString(), cmodel)
	}

	return nil
}

// SavePolicy saves policy to database.
// Implements casbin Adapter interface.
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
		mut   *spanner.Mutation
		limit int
	}

	ctx := context.Background()
	done := make(chan error, 1)
	ch := make(chan *mut_t)

	// Start loading to Spanner. Let's do batch write in case data is way more
	// than Spanner's mutation limits.
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

// AddPolicy adds a policy rule to the storage.
func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	casbinRule := a.genPolicyLine(ptype, rule)
	_, err := a.client.ReadWriteTransaction(context.Background(),
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			sql := `
insert ` + a.table + `
  (ptype, v0, v1, v2, v3, v4, v5)
values (
  '` + casbinRule.PType + `',
  '` + casbinRule.V0 + `',
  '` + casbinRule.V1 + `',
  '` + casbinRule.V2 + `',
  '` + casbinRule.V3 + `',
  '` + casbinRule.V4 + `',
  '` + casbinRule.V5 + `')`

			_, err := txn.Update(ctx, spanner.Statement{SQL: sql})
			return err
		},
	)

	return err
}

// AddPolicies adds multiple policy rule to the storage.
// func (a *Adapter) AddPolicies(sec string, ptype string, rules [][]string) error {
// 	log.Printf("AddPolicies entry: sec=%v, ptype=%v, rules=%v", sec, ptype, rules)
// 	return nil
// }

// RemovePolicy removes a policy rule from the storage.
func (a *Adapter) RemovePolicy(sec string, ptype string, rule []string) error {
	casbinRule := a.genPolicyLine(ptype, rule)
	_, err := a.client.ReadWriteTransaction(context.Background(),
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			sql := `
delete from ` + a.table + `
where ptype = @ptype
  and v0 = @v0
  and v1 = @v1
  and v2 = @v2
  and v3 = @v3
  and v4 = @v4
  and v5 = @v5`

			stmt := spanner.Statement{
				SQL: sql,
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

	return err
}

// RemovePolicies removes multiple policy rule from the storage.
// func (a *Adapter) RemovePolicies(sec string, ptype string, rules [][]string) error {
// 	log.Printf("RemovePolicies entry: sec=%v, ptype=%v, rules=%v", sec, ptype, rules)
// 	return nil
// }

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
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
		sql = sql + fmt.Sprintf(" and %v = @val", k)
		params["val"] = v
	}

	_, err := a.client.ReadWriteTransaction(context.Background(),
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			_, err := txn.Update(ctx, spanner.Statement{SQL: sql, Params: params})
			return err
		},
	)

	return err
}

// LoadFilteredPolicy loads only policy rules that match the filter.
// func (a *Adapter) LoadFilteredPolicy(model model.Model, filter interface{}) error {
// 	log.Println("LoadFilteredPolicy entry")
// 	return nil
// }

// IsFiltered returns true if the loaded policy has been filtered.
// func (a *Adapter) IsFiltered() bool { return a.filtered }

// UpdatePolicy update oldRule to newPolicy permanently
// func (a *Adapter) UpdatePolicy(sec string, ptype string, oldRule, newPolicy []string) error {
// 	log.Println("UpdatePolicy entry")
// 	return nil
// }

// UpdatePolicies updates some policy rules to storage, like db, redis.
// func (a *Adapter) UpdatePolicies(sec string, ptype string, oldRules, newRules [][]string) error {
// 	log.Println("UpdatePolicies entry")
// 	return nil
// }

func (a *Adapter) recreateTable() error {
	ctx := context.Background()
	op, err := a.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: a.database,
		Statements: []string{
			`DROP TABLE ` + a.table,
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
	return `
create table ` + a.table + ` (
    ptype string(MAX),
    v0 string(MAX),
    v1 string(MAX),
    v2 string(MAX),
    v3 string(MAX),
    v4 string(MAX),
    v5 string(MAX),
) primary key (ptype, v0, v1, v2, v3, v4, v5)`
}

func (a *Adapter) genPolicyLine(ptype string, rule []string) CasbinRule {
	line := CasbinRule{PType: ptype}
	l := len(rule)
	if l > 0 {
		line.V0 = rule[0]
	}

	if l > 1 {
		line.V1 = rule[1]
	}

	if l > 2 {
		line.V2 = rule[2]
	}

	if l > 3 {
		line.V3 = rule[3]
	}

	if l > 4 {
		line.V4 = rule[4]
	}

	if l > 5 {
		line.V5 = rule[5]
	}

	return line
}
