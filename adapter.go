package spanneradapter

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"runtime"
	"strings"

	"cloud.google.com/go/spanner"
	databasev1 "cloud.google.com/go/spanner/admin/database/apiv1"
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

func (c CasbinRule) String() string {
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
	admin           *databasev1.DatabaseAdminClient
	client          *spanner.Client
	internalAdmin   bool // // in finalizer, close 'admin' only when internal
	internalClient  bool // in finalizer, close 'client' only when internal
}

// NewAdapterOptions is the options you provide to NewAdapter().
type NewAdapterOptions struct {
	TableName            string                          // if not provided, default table will be 'casbin_rule'
	SkipDatabaseCreation bool                            // if true, skip the database creation part in NewAdapter
	SkipTableCreation    bool                            // if true, skip the table creation part in NewAdapter
	AdminClient          *databasev1.DatabaseAdminClient // if non-nil, will use as the database admin client
	Client               *spanner.Client                 // if provided, will use this connection instead
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
		a.admin, err = databasev1.NewDatabaseAdminClient(ctx)
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
			log.Println("close client")
			adapter.client.Close()
		}

		if adapter.admin != nil && adapter.internalAdmin {
			log.Println("close admin")
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
	log.Printf("LoadPolicy entry: cmodel=%+v", cmodel)
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
		log.Printf("load %v", cr)
		persist.LoadPolicyLine(cr.String(), cmodel)
	}

	return nil
}

// SavePolicy saves policy to database.
// Implements casbin Adapter interface.
func (a *Adapter) SavePolicy(cmodel model.Model) error {
	log.Printf("SavePolicy entry: cmodel=%+v", cmodel)
	return nil
}

// AddPolicy adds a policy rule to the storage.
func (a *Adapter) AddPolicy(sec string, ptype string, rule []string) error {
	log.Printf("AddPolicy entry: sec=%v, ptype=%v rule=%v", sec, ptype, rule)
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
	log.Printf("RemovePolicy entry: sec=%v, ptype=%v, rule=%v", sec, ptype, rule)
	return nil
}

// RemovePolicies removes multiple policy rule from the storage.
// func (a *Adapter) RemovePolicies(sec string, ptype string, rules [][]string) error {
// 	log.Printf("RemovePolicies entry: sec=%v, ptype=%v, rules=%v", sec, ptype, rules)
// 	return nil
// }

// RemoveFilteredPolicy removes policy rules that match the filter from the storage.
func (a *Adapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	log.Println("RemoveFilteredPolicy entry")
	return nil
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
	log.Println("recreateTable entry")
	ctx := context.Background()
	op, err := a.admin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database: a.database,
		Statements: []string{
			`DROP TABLE ` + a.table,
			a.createTableSql(),
		},
	})

	if err != nil {
		log.Printf("UpdateDatabaseDdl failed: %v", err)
		return err
	}

	if err := op.Wait(ctx); err != nil {
		log.Printf("Wait on UpdateDatabaseDdl failed: %v", err)
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

func (a *Adapter) genPolicyLine(ptype string, rule []string) *CasbinRule {
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

	return &line
}
