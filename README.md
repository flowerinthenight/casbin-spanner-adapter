This library implements a [Casbin](https://casbin.org/) adapter for [Cloud Spanner](https://cloud.google.com/spanner).

To install:

```sh
$ go get github.com/flowerinthenight/casbin-spanner-adapter
```

Example usage:

```go
package main

import (
    "flag"
    "log"
    "time"

    "github.com/casbin/casbin/v2"
    spanneradapter "github.com/flowerinthenight/casbin-spanner-adapter"
)

func main() {
    a, _ := spanneradapter.NewAdapter(
        "projects/{v}/instances/{v}/databases/{v}",
        spanneradapter.NewAdapterOptions{
            SkipDatabaseCreation: true,
            SkipTableCreation:    false,
        },
    )

    e, _ := casbin.NewEnforcer("rbac_model.conf", a)

    // Load stored policy from database.
    e.LoadPolicy()

    // Do permission checking.
    e.Enforce("alice", "data1", "write")

    // Do some mutations.
    e.AddPolicy("alice", "data2", "write")
    e.RemovePolicy("alice", "data1", "write")

    // Persist policy to database.
    e.SavePolicy()
}
```
