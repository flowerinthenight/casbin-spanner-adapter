This library implements a [Casbin](https://casbin.org/) adapter for [Cloud Spanner](https://cloud.google.com/spanner).

To install:

```sh
$ go get github.com/flowerinthenight/casbin-spanner-adapter
```

Example usage:

```go
package main

import (
  "database/sql"
  "os"

  "github.com/casbin/casbin/v2"
  "github.com/cychiuae/casbin-pg-adapter"
)

func main() {
  connectionString := "postgresql://postgres:@localhost:5432/postgres?sslmode=disable"
  db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
  if err != nil {
    panic(err)
  }

  tableName := "casbin"
  adapter, err := casbinpgadapter.NewAdapter(db, tableName)
  // If you are using db schema
  // myDBSchema := "mySchema"
  // adapter, err := casbinpgadapter.NewAdapterWithDBSchema(db, myDBSchema, tableName)
  if err != nil {
    panic(err)
  }

  enforcer, err := casbin.NewEnforcer("./examples/model.conf", adapter)
  if err != nil {
    panic(err)
  }

  // Load stored policy from database
  enforcer.LoadPolicy()

  // Do permission checking
  enforcer.Enforce("alice", "data1", "write")

  // Do some mutations
  enforcer.AddPolicy("alice", "data2", "write")
  enforcer.RemovePolicy("alice", "data1", "write")

  // Persist policy to database
  enforcer.SavePolicy()
}
```
