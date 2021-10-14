package main

import (
	"flag"
	"log"
	"time"

	"github.com/casbin/casbin/v2"
	spanneradapter "github.com/flowerinthenight/casbin-spanner-adapter"
)

var (
	dbstr = flag.String("db", "", "fmt: projects/{v}/instances/{v}/databases/{v}")
)

func main() {
	defer func(begin time.Time) {
		log.Printf("duration=%v", time.Since(begin))
	}(time.Now())

	flag.Parse()
	a, err := spanneradapter.NewAdapter(
		*dbstr,
		spanneradapter.NewAdapterOptions{
			SkipDatabaseCreation: true,
			SkipTableCreation:    false,
		},
	)

	if err != nil {
		log.Println(err)
		return
	}

	e, err := casbin.NewEnforcer("rbac_model.conf", a)
	if err != nil {
		log.Println(err)
		return
	}

	// Load the policy from DB.
	ok, err := e.AddPolicy("alice", "data1", "write")
	log.Println(ok, err)

	e.LoadPolicy()

	// Check the permission.
	e.Enforce("alice", "data1", "read")

	// Modify the policy.
	// e.AddPolicy(...)
	// e.RemovePolicy(...)

	// Save the policy back to DB.
	e.SavePolicy()

	time.Sleep(time.Second * 60)
}
