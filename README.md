# ParticleDB for Go

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/particledb-ai/particledb-go"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    pool, err := particledb.NewPool(ctx, os.Getenv("PARTICLEDB_URL"))
    if err != nil { log.Fatal(err) }
    defer pool.Close()
    if err := pool.KVSet(ctx, []byte("session:123"), []byte("active")); err != nil { log.Fatal(err) }
    value, err := pool.KVGet(ctx, []byte("session:123"))
    if err != nil { log.Fatal(err) }
    fmt.Println(string(value))
}
```

## Start a local server

A server started without options requires authentication but has no password
set, so start it in one of these two ways. For local development on a trusted
machine, turn authentication off and keep every listener on loopback:

```sh
particledb start --no-auth --data-dir ./pdb-dev \
  --http-addr 127.0.0.1:7173 --pg-addr 127.0.0.1:5432
```

To require a password, create the login role from the command line. The
password file holds only the password; keep it readable by you alone.
SCRAM authenticates without TLS; use TLS for remote access.

```sh
particledb start --data-dir ./pdb-dev \
  --http-addr 127.0.0.1:7173 --pg-addr 127.0.0.1:5432 \
  --auth-method scram-sha-256 --pg-username app --pg-password-file ./pdb-password
```

The SDK connects to `localhost:7173` by default. With authentication on,
pass the same user and password:

```go
cfg, err := particledb.ParseConfig("")
if err != nil { log.Fatal(err) }
cfg.User = "app"
cfg.Password = os.Getenv("PARTICLEDB_PASSWORD")
pool, err := particledb.NewPoolConfig(ctx, cfg)
if err != nil { log.Fatal(err) }
defer pool.Close()
```

The default address is `localhost:7173`. Keep a pool for the application's
lifetime and pass a context to each operation. Use a supported, patched Go
release; the module's dependencies require Go 1.26.10 or newer. The current
validation toolchain is Go 1.27.3.

## Install

The module source is published at
[github.com/particledb-ai/particledb-go](https://github.com/particledb-ai/particledb-go).
Once `v0.2.0` is tagged there, install it with
`go get github.com/particledb-ai/particledb-go@v0.2.0`. Until then, use a local
replacement for the source directory in a consumer module:

```sh
go mod edit -require=github.com/particledb-ai/particledb-go@v0.0.0
go mod edit -replace=github.com/particledb-ai/particledb-go=/path/to/sdk/go
go mod tidy
```

See [Go SDK documentation](https://particledb.ai/sdks/go/) for the matching server.

## Remote connections

Pass a connection URL to `NewPool` or `Connect`:

```text
particledb://app:password@db.example.com:7173?sslmode=verify-full&sslrootcert=ca.pem
```

Use a configuration object for credentials loaded from your environment.
`verify-full` checks the certificate and hostname. Omit `sslrootcert` to use
system roots. Client certificates use `sslcert` and `sslkey`. The default
TLS negotiation uses the database listener. A dedicated native listener can
use `sslnegotiation=upgrade`. Neither mode falls back to plaintext.

## Queries and transactions

```go
_, err := pool.Exec(ctx, "INSERT INTO items (id, name) VALUES ($1, $2)", 1, "first")
if err != nil { return err }
var name string
if err := pool.QueryRow(ctx, "SELECT name FROM items WHERE id = $1", 1).Scan(&name); err != nil {
    return err
}
err = particledb.BeginFunc(ctx, pool, particledb.TxOptions{}, func(tx *particledb.Tx) error {
    _, err := tx.Exec(ctx, "UPDATE items SET name = $1 WHERE id = $2", "updated", 1)
    return err
})
```

Parameters are bound separately from SQL. `QueryRow.Scan` returns `ErrNoRows`
for an empty result. Close `Rows` when finished, and check `Rows.Err` after
iteration. `CollectRows` and `RowToStructByName` provide typed collection.
Exact decimals use `particledb.Decimal`; timestamps use `time.Time`.

`BeginFunc` pins one connection, commits on success and rolls back on an
error. `SendBatch` runs an ordered statement batch; outside an existing
transaction it is atomic. `CopyFrom` provides native bulk insertion from
row slices or a streaming source. Check every returned error.

## database/sql

```go
import (
    "database/sql"
    _ "github.com/particledb-ai/particledb-go"
)

db, err := sql.Open("particledb", "particledb://localhost:7173")
if err != nil { return err }
defer db.Close()
var answer int
if err := db.QueryRowContext(ctx, "SELECT $1::int", 42).Scan(&answer); err != nil {
    return err
}
```

Importing the package registers the `particledb` driver. If you only use
`database/sql`, a blank import is sufficient. `sql.OpenDB(NewConnector(cfg))`
accepts a parsed configuration. Use `RETURNING` instead of `LastInsertId`.

## Cancellation, errors and scope

A context deadline cancels the running statement on the server. The
connection remains usable when the server confirms cancellation. If the
server does not answer within the cancellation grace period, the connection
closes because the outcome is unknown. Requests are not automatically replayed.
`Error` exposes SQLSTATE and `IsRetryable` identifies transaction conflicts;
retry the complete application transaction only when that is appropriate.
Connection-string parse errors omit the original URL, malformed fragments
and underlying parser exceptions. `config_redaction_test.go` checks error
formatting and the complete unwrap chain.

The native API is the default. Older HTTP, PostgreSQL and gRPC helpers live
under `legacy`. The server hosts one database per instance; the database
component in a URL does not select another database. Binary KV frame writes
are not part of a SQL transaction.

## Validate

```sh
PDB_TEST_DSN=particledb://localhost:PORT \
  PDB_TLS_PORT=TLS_PORT PDB_TLS_UPGRADE_PORT=UPGRADE_PORT PDB_TLS_CA=ca.pem \
  go test -race -count=1 ./...
govulncheck ./...
```

Configuration path tests use the operating system's path separators.
Unix socket fixtures keep short paths on Unix and use the Windows temporary
directory on Windows.

Use disposable listeners. The full SDK conformance suite, including server
crash and restart cases, is separate. See [SUPPORT.md](SUPPORT.md) and
[Go SDK documentation](https://particledb.ai/sdks/go/).

MIT licensed. See [LICENSE](LICENSE).

`time.Time` binds an instant as timestamptz. Use `Timestamp{Time: value}`
for wall-clock timestamp fields and `Date{Time: value}` for calendar dates.
The client does not infer a different parameter type from each SQL column.
