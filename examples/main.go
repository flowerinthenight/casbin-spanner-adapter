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
	log.Println("AddPolicy:", ok, err)
	ok, err = e.AddPolicy("bob", "data2", "read")
	log.Println("AddPolicy:", ok, err)

	ok, err = e.AddPolicy("todel", "data", "read")
	log.Println("AddPolicy:", ok, err)
	ok, err = e.RemovePolicy("todel", "data", "read")
	log.Println("RemovePolicy:", ok, err)

	ok, err = e.AddPolicy("todelf", "data", "read")
	log.Println("AddPolicy:", ok, err)
	ok, err = e.RemoveFilteredPolicy(0, "todelf")
	log.Println("RemoveFilteredPolicy:", ok, err)

	e.LoadPolicy()

	// Check the permission.
	ok, err = e.Enforce("alice", "data1", "write")
	log.Println("Enforce:", ok, err)

	// Modify the policy.
	// e.AddPolicy(...)
	// e.RemovePolicy(...)

	// Save the policy back to DB.
	err = e.SavePolicy()
	log.Println("SavePolicy:", err)
}
