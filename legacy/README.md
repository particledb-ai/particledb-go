# ParticleDB Go SDK: legacy client (package `legacy`)

The previous Go SDK, kept unchanged apart from its package name
(`github.com/particledb-ai/particledb-go/legacy`). It speaks the protocol 1
native wire (and gRPC, and the PostgreSQL wire through a `database/sql`
driver the application registers). New code should use the root package
(`github.com/particledb-ai/particledb-go`): protocol 2 multiplexed, a
`database/sql` driver, a native `Pool`, `Batch` and `CopyFrom`; see
`../README.md`. The text below is the legacy client's own documentation.

A Go client library for [ParticleDB](https://particledb.ai): OLTP, OLAP, vector search, key-value, RAG, and AI operations.

**Transport.** `Client` speaks the **PDB native wire** (port **5440**) by
default: binary framing, Arrow IPC results, native prepared statements
(`MSG_PREPARE` / `MSG_EXECUTE` / `MSG_CLOSE_STMT`) and batch execution
(`MSG_EXECUTE_BATCH`) with parameters sent as a typed Arrow batch. The
PostgreSQL wire (port 5432, `database/sql`) is an explicit opt-in for legacy
setups (`Protocol: particledb.ProtocolPG` or `ConnectPG`). The SDK itself never
imports a PG driver; only an application that opts into the PG wire imports
one (e.g. `_ "github.com/lib/pq"`). A native gRPC client is also included.

## Installation

```bash
go get github.com/particledb-ai/particledb-go
```

## Quick Start

```go
package main

import (
    "fmt"
    "log"

    particledb "github.com/particledb-ai/particledb-go"
)

func main() {
    // Native wire, localhost:5440. No driver import needed.
    client, err := particledb.New(particledb.DefaultConfig())
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // $1..$N are bound natively (PREPARE + EXECUTE), never spliced into SQL.
    result, err := client.Query("SELECT * FROM users WHERE id = $1", 42)
    if err != nil {
        log.Fatal(err)
    }
    for _, row := range result.Rows {
        fmt.Println(row)
    }
}
```

## Configuration

```go
cfg := particledb.Config{
    Host:     "db.example.com",
    Port:     5440,      // 0 = protocol default (5440 native, 5432 PG)
    User:     "admin",
    Password: "secret",  // native: sent with MSG_AUTH when non-empty
    PoolSize: 20,
    // Protocol: "" or particledb.ProtocolNative (default) | particledb.ProtocolPG
}
client, err := particledb.New(cfg)
```

Or connect with a connection string:

```go
client, err := particledb.Connect("pdb://admin:secret@db.example.com:5440?pool_size=20")
client, err := particledb.Connect("host=localhost port=5440 user=particledb") // native
```

The native client has no TLS: `SSLMode` `require` / `verify-ca` /
`verify-full` make `New` / `Connect` return an error rather than connect in
plaintext. `Database` is used on the PG wire only.

### PostgreSQL wire (explicit opt-in)

```go
import _ "github.com/lib/pq" // any database/sql driver registered as "postgres"

client, err := particledb.New(particledb.DefaultPGConfig()) // localhost:5432
client, err := particledb.ConnectPG("host=localhost port=5432 user=particledb dbname=particledb sslmode=disable")
client, err := particledb.Connect("postgres://particledb@localhost:5432/particledb") // a postgres:// URL selects PG
client, err := particledb.Connect("host=localhost port=5432 protocol=pg")
```

## Prepared statements and batch insert (native wire)

```go
// One round trip for 10,000 rows: MSG_EXECUTE_BATCH with an Arrow batch.
rows := make([][]interface{}, 0, 10000)
for i := 1; i <= 10000; i++ {
    rows = append(rows, []interface{}{i, fmt.Sprintf("user-%d", i), nil})
}
res, err := client.ExecuteBatch("INSERT INTO users (id, name, email) VALUES ($1, $2, $3)", rows)
var be *particledb.PdbBatchError
if errors.As(err, &be) {
    for _, e := range be.Result.Errors {
        log.Printf("row %d failed: %s", e.Row, e.Message)
    }
}
fmt.Println(res.TotalAffected())

// An explicit statement handle on the native client.
st, err := client.Native().Prepare(ctx, "SELECT name FROM users WHERE id = $1")
rows, err := st.Query(ctx, 7)
st.Close()
```

* **Parameter types** (Go -> Arrow): all signed/unsigned integers -> Int64
  (`uint64` above `MaxInt64` is refused), `float32`/`float64` -> Float64,
  `string` -> Utf8, `[]byte` -> Binary, `bool` -> Boolean, `time.Time` ->
  Timestamp(us, "+00:00"), `nil` (and nil pointers) -> NULL, `driver.Valuer`
  -> its value. In a batch a column mixing integers and floats is sent as
  Float64; any other mix, or an unsupported type, is a `*PdbParamError` and
  nothing is sent.
* **Batch semantics:** outside a transaction every row autocommits
  independently, so a failed row does not stop the others; inside
  `Transaction` the server stops at the first failed row and aborts the
  transaction. Any failed row returns the per-row `*BatchResult` together with
  a `*PdbBatchError` (which also unwraps to `*PdbQueryError`). Frames are split
  above ~12 MiB of parameters (the server limit is 16 MiB per frame).
* **Statement cache:** each pooled connection prepares a SQL string on first
  use and reuses the handle (up to 256 per connection, oldest closed with
  `MSG_CLOSE_STMT`). A statement deallocated server-side (`DISCARD ALL`) is
  re-prepared once.
* **Result types** (Arrow -> Go): integers -> `int64` (`uint64` above
  `MaxInt64` stays `uint64`), floats -> `float64`, text -> `string`, bytea ->
  `[]byte`, boolean -> `bool`, date/timestamp -> `time.Time` (UTC), numeric ->
  decimal `string` (e.g. `sum(bigint)`), time -> `string`, lists ->
  `[]interface{}`, structs -> `map[string]interface{}`, NULL -> `nil`. An
  unsupported or malformed Arrow stream is an error, never a guessed value.

## Vector Search

```go
vec := &particledb.VectorOps{}
// Handled via client.Vector()

// ANN search
results, err := client.Vector().Search("documents", "embedding",
    []float32{0.1, 0.2, 0.3},
    &particledb.VectorSearchOptions{K: 5},
)

// Insert with vector
err = client.Vector().Insert("documents",
    map[string]interface{}{"title": "My Doc", "content": "..."},
    "embedding", []float32{0.1, 0.2, 0.3},
)

// Create HNSW index
err = client.Vector().CreateIndex("documents", "embedding", "hnsw",
    map[string]interface{}{"m": "24", "ef_construction": "200"},
)
```

## Key-Value Operations

```go
kv := client.KV()

// Set / Get
kv.Set("user:1", "alice")
val, _ := kv.Get("user:1") // "alice"

// Set with TTL (seconds)
kv.SetWithTTL("session:abc", "data", 3600)

// Batch operations
kv.MSet(map[string]string{"a": "1", "b": "2"})
results, _ := kv.MGet([]string{"a", "b"})

// Atomic increment
newVal, _ := kv.Incr("counter", 1)

// Check existence
exists, _ := kv.Exists("user:1")

// Delete
deleted, _ := kv.Delete("user:1")
```

## RAG (Retrieval-Augmented Generation)

```go
rag := client.RAG()

// Create a pipeline
rag.CreatePipeline("qa", "documents", "embedding", "content", 5, "hybrid")

// Query using pipeline
answer, _ := rag.Query("What is ParticleDB?", "qa")

// Query with explicit options
answer, _ = rag.QueryWithOptions("What is ParticleDB?",
    "documents", "embedding", "content", 5,
)
```

## AI Functions

```go
ai := client.AI()

// Generate embeddings
embedding, _ := ai.Embed("Hello world", "text-embedding-ada-002")

// Batch embeddings
embeddings, _ := ai.EmbedBatch([]string{"Hello", "World"}, "text-embedding-ada-002")

// Text generation
result, _ := ai.Generate("Explain quantum computing", "gpt-4")

// Classification
label, _ := ai.Classify("I love this product!", []string{"positive", "negative", "neutral"})

// Summarization
summary, _ := ai.Summarize("Long text to summarize...")
```

## gRPC Client

The SDK includes a native gRPC client that connects to the ParticleDB gRPC
endpoint (default port 26257). This is an alternative to the PG wire protocol
client and provides access to both the `ParticleDB` service (SQL execution,
batch, health) and the `QueryService` (query, streaming query, DML).

### Connecting

```go
package main

import (
    "context"
    "fmt"
    "log"

    particledb "github.com/particledb-ai/particledb-go"
)

func main() {
    client, err := particledb.NewGRPCClient("localhost", particledb.DefaultGRPCPort)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Health check
    health, err := client.Health(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("status=%s version=%s\n", health.Status, health.Version)
}
```

### ExecuteSQL (ParticleDB service)

Execute a single SQL statement with optional parameters:

```go
// DDL
resp, err := client.ExecuteSQL(ctx, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

// INSERT with parameters
resp, err = client.ExecuteSQL(ctx,
    "INSERT INTO users (id, name) VALUES ($1, $2)",
    particledb.IntValue(1),
    particledb.StringValue("alice"),
)
fmt.Println("rows affected:", resp.RowsAffected)

// SELECT
resp, err = client.ExecuteSQL(ctx, "SELECT id, name FROM users")
for _, row := range resp.Rows {
    for _, val := range row.Values {
        fmt.Println(val)
    }
}

// Convert to []map[string]interface{} (same format as PG wire client)
rows := particledb.ResultToMaps(resp)
```

### Query (QueryService)

Use the `QueryService` for read queries with richer metadata and pagination:

```go
resp, err := client.Query(ctx, "SELECT * FROM users LIMIT 100")
if resp.Status != particledb.PBStatusOK {
    log.Fatal("query failed")
}
for _, col := range resp.ResultSet.Columns {
    fmt.Printf("column: %s (type %d)\n", col.Name, col.DataType)
}
```

For full control over the request (transaction, mode, pagination):

```go
resp, err := client.QueryWithOptions(ctx, &particledb.PBQueryRequest{
    Sql:     "SELECT * FROM users",
    Mode:    particledb.PBQueryModeProfile,
    MaxRows: 50,
})
fmt.Println("execution time:", resp.Stats.ExecutionTimeUs, "us")
```

### StreamQuery

Stream large result sets via the server-streaming `QueryStream` RPC:

```go
results, errCh := client.StreamQuery(ctx, "SELECT * FROM large_table")
for resp := range results {
    fmt.Println("batch:", len(resp.ResultSet.Rows), "rows")
}
if err := <-errCh; err != nil {
    log.Fatal(err)
}
```

### BatchExecute

Execute multiple SQL statements in a single round-trip:

```go
resp, err := client.BatchExecute(ctx, []string{
    "INSERT INTO users (id, name) VALUES (1, 'alice')",
    "INSERT INTO users (id, name) VALUES (2, 'bob')",
    "UPDATE users SET name = 'charlie' WHERE id = 1",
})
for i, result := range resp.Results {
    fmt.Printf("statement %d: affected=%d error=%q\n",
        i, result.RowsAffected, result.Error)
}
```

### Execute (QueryService DML)

For DML statements via the QueryService:

```go
resp, err := client.Execute(ctx, "DELETE FROM users WHERE id = 1")
fmt.Println("rows affected:", resp.RowsAffected)
```

### Connection Options

```go
import "time"

client, err := particledb.NewGRPCClient("db.example.com", 26257,
    particledb.WithConnectTimeout(5 * time.Second),
    particledb.WithDialOptions(
        // add TLS, interceptors, etc.
    ),
)
```

## Transactions

```go
err := client.Transaction(func(tx *particledb.Tx) error {
    _, err := tx.Execute("INSERT INTO users (name) VALUES ($1)", "alice")
    if err != nil {
        return err // triggers rollback
    }
    _, err = tx.Execute("UPDATE counters SET val = val + 1 WHERE name = $1", "users")
    return err // nil = commit
})
```

On the native wire `Transaction` pins one pooled connection for
`BEGIN` .. `COMMIT`/`ROLLBACK`; every `tx` call runs on it. A statement that
fails inside it aborts the transaction (the client then sends `ROLLBACK`,
even if `fn` returns nil, and `Transaction` returns an error). A COMMIT whose
reply is lost returns "commit outcome unknown" and the connection is discarded.
If `fn` panics, the connection is discarded (the server rolls the transaction
back) rather than returned to the pool inside the open transaction, and the
panic propagates.

**Isolation.** `Transaction` sends a plain `BEGIN` and never names an isolation
level, so it runs at the server's default: READ COMMITTED (as in
PostgreSQL) unless the server was started with `--default-isolation
repeatable-read|serializable`. For snapshot isolation or serializable, make
`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` (or `SERIALIZABLE`) the
first statement of the transaction; at those levels a conflicting
transaction fails with SQLSTATE `40001` and must be retried.

## Native wire client (`PdbWireClient`): batch and transaction errors

`Batch` sends one `MSG_BATCH` frame and `Transaction` sends one batch of
`BEGIN, <statements>, COMMIT`. The server answers with one `BATCH_RESULT` and
stops at the first failing statement. `sendBatchWithRetry` decodes every entry
(`decodeBatchResult`): a failed statement returns `*PdbQueryError` carrying the
server message (code 0: batch entries carry no code), and a reply that does not
account for every statement sent returns `*PdbError` (protocol error) and
discards that connection. Neither is retried. After a failed statement the
client sends `ROLLBACK` on the same connection (a no-op outside a transaction)
before returning it to the pool, and discards the connection if the rollback
does not succeed, so a failed transaction never leaves a pooled connection
inside an aborted transaction.

## Native wire client (`PdbWireClient`): failures and retries

* **Retried automatically:** only a failure that provably happened before any
  byte of the request was written -- the TCP/Unix dial, or the HELLO handshake
  on a fresh connection (`acquireWithRetry`). `MaxRetries` / `RetryDelayMs`
  apply to this phase only.
* **Never retried:** once a request has started to be sent, a socket error,
  EOF, reset or `ctx` deadline expiry returns `*PdbConnectionError` and is NOT
  retried -- for `Query`, `Execute`, `Batch` and `Transaction` alike. The
  server may have executed, and committed, the request before the failure, so
  a resend could apply it twice (a `SELECT` is not necessarily read-only:
  `SELECT KV_SET(...)`). Decide in your application whether the operation is
  safe to repeat.
* The failed (or timed-out) connection is closed, never returned to the pool:
  its late reply may still arrive, or it may be inside a transaction.

## Running Tests

```bash
cd sdk/go
go build ./... && go vet ./... && go test ./...
```

Live tests (each skipped unless its env var is set; start the server with
`--no-auth`):

```bash
PDB_NATIVE_PORT=5440 go test -count=1 -v -run 'TestNativeClientLive|TestNativeBatchErrorsLive' .
PARTICLEDB_TEST_PORT=5432 go test -count=1 -v .                                   # integration suite, PG wire
PARTICLEDB_TEST_PROTOCOL=native PARTICLEDB_TEST_PORT=5440 go test -count=1 -v .  # same suite, native wire
```

## Files

| File | What it is for |
|------|----------------|
| `particledb.go` | `Client`: `New` / `Connect` / `ConnectPG`, transport selection (native default, PG opt-in), `Query` / `QueryOne` / `Execute` / `ExecuteBatch`, `Tx`, `Transaction` (PG path), accessors for the helper namespaces. |
| `types.go` | `Config`, `Protocol` (`ProtocolNative`, `ProtocolPG`), `DefaultConfig` (native, 5440), `DefaultPGConfig` (5432), `Config.DSN` (PG wire only), result/option types. |
| `pdb_wire.go` | `PdbWireClient`: framing, HELLO / AUTH handshake, connection pool, retries (dial/HELLO only), `dialWire` (a local host - `IsLocalHost`: `localhost`, `127.x`, `::1`, no DNS - dials `UnixSocketPath(UnixSocketDir, port)` when the file exists unless `DisableUnixSocket`; absent or failing socket = TCP), `Query` / `Execute` / `Batch` / `Transaction` (statement-list batch), `decodeBatchResult`. |
| `pdb_wire_prepared.go` | Native prepared statements: `MSG_PREPARE` / `MSG_EXECUTE` / `MSG_EXECUTE_BATCH` / `MSG_CLOSE_STMT`, the per-connection `stmtCache`, `PdbStmt`, `ExecuteBatch`, `BatchResult` / `PdbBatchError` / `PdbParamError`, `withConn` (connection fate after each kind of error). `execPreparedBatch` encodes every frame before sending the first and refuses a row over the server's 16 MiB frame cap (`maxFramePayload`) with nothing sent. |
| `native_tx.go` | `Client.Transaction` on the native wire: one pinned connection for BEGIN .. COMMIT/ROLLBACK (`nativeTx`). |
| `arrow_ipc.go` | Dependency-free Arrow IPC stream codec: writer for parameter batches (`encodeArrowParams`, hand-written flatbuffers) and reader for results (`decodeArrowStream`). |
| `kv.go`, `vector.go`, `rag.go`, `ai.go`, `ops.go` | Helper namespaces (KV, vector search, RAG, AI functions, extended ops) built on `Client.Query` / `Execute`, so they use whichever transport the client speaks. |
| `grpc_client.go`, `grpc_service.go`, `pb_types.go`, `proto/` | Native gRPC client, hand-written service stubs and message types, and the proto definitions. |
| `particledb_test.go`, `ops_namespace_test.go` | SQL builders, config and ops helpers (unit). |
| `injection_test.go` | SQL injection regressions: classify labels, vector-index options and the RAG pipeline mode stay inside their escaped literals, `BuildInsertSQL` binds data values as `$1..$N` (unit); `TestLiveLegacyInjectionStoredAsData` (needs `PDB_NATIVE_PORT`) stores hostile strings through bound parameters and checks a canary table survives. |
| `native_client_test.go` | Transport selection (`New` / `Connect` forms, TLS refusal) and scripted-server tests for `ExecuteBatch` per-row errors, the statement cache and client-side parameter errors (unit). |
| `arrow_ipc_test.go` | Arrow codec: decodes a pyarrow-written reference stream, byte-checks the writer's IPC framing, round-trips every parameter type and NULLs, rejects bad input (unit). |
| `unix_socket_test.go` | Platform-native paths and a Windows temporary directory, with short Unix paths for macOS socket limits. Unix-socket selection: `IsLocalHost` literals (no DNS), `UnixSocketPath`, `dialWire` over a real Unix + TCP fixture (local host rides the socket on Unix, while Windows deliberately keeps TCP; `DisableUnixSocket` / another dir ride TCP; absent and stale socket fall back to TCP), the `unix_socket` / `unix_socket_dir` URL and DSN parameters. `TestUnixSocketLive` (needs `PDB_NATIVE_PORT`): `Health().Protocol` per option. |
| `pdb_wire_test.go` | `PdbWireClient` against a scripted `net.Pipe` server (unit). Batch replies are real `BATCH_RESULT` frames (`writeBatchResultFrame`); parameterised queries go PREPARE + EXECUTE. |
| `pdb_wire_ambiguous_test.go` | Real-TCP fake server counting requests and accepts: a batch / transaction / statement whose connection drops (or whose `ctx` deadline expires) after the request was sent returns `*PdbConnectionError`, is received exactly once, and its connection is not reused; a refused dial and a dropped HELLO are still retried. No ParticleDB server. |
| `integration_test.go` | Integration suite (incl. `TestExecuteBatch_Integration`); skipped unless `PARTICLEDB_TEST_PORT` is set. PG wire by default (imports `lib/pq`, test-only), native wire with `PARTICLEDB_TEST_PROTOCOL=native`. |
| `native_batch_errors_live_test.go` | Live native-wire test: a failed statement inside `Batch` / `Transaction` must return the server's `duplicate key` error, the pooled connection must be usable afterwards, and successful batches / transactions must still succeed. Skipped unless `PDB_NATIVE_PORT` is set. Prints `NBE_PASS go`. |
| `native_client_live_test.go` | Live test of the default `Client` over the native wire: query types, bound parameters (incl. NULL, bytea, timestamp, a quote-laden string), a 10,000-row `ExecuteBatch` insert checked by count + sum(id) + sum(score) + a spot row, typed errors (bad SQL, duplicate keys in a batch with and without a transaction), commit visibility, `PdbStmt`, KV helpers. Skipped unless `PDB_NATIVE_PORT` is set. Prints `NCL_PASS go`. |
| `native_tx_regress_live_test.go` | Live regressions: a panic inside `Client.Transaction` discards the pinned connection instead of pooling it inside the open transaction (pool size 1, the next borrower's COMMIT must not persist the row); a statement error caught inside `fn` makes `Transaction` fail with nothing committed; a batch whose last row exceeds the 16 MiB frame cap is refused with `*PdbParamError` before any frame is sent (count stays 0). Skipped unless `PDB_NATIVE_PORT` is set. |
| `go.mod`, `go.sum` | Module definition. `github.com/lib/pq` is required only by `integration_test.go`. |

## Proto Definitions

The proto files are in `proto/`. If you want to regenerate Go code from them
instead of using the hand-written types, install `protoc-gen-go` and
`protoc-gen-go-grpc`, then run:

```bash
protoc --go_out=. --go-grpc_out=. \
    -I proto proto/common.proto proto/particledb.proto proto/query.proto
```

## Requirements

- Go 1.21+
- **Native wire (default)**: no extra dependency.
- **PG wire (opt-in)**: a `database/sql` driver registered as `postgres` (e.g. `github.com/lib/pq`), imported by the application.
- **gRPC client**: `google.golang.org/grpc` (pulled in automatically via `go mod tidy`)
- A ParticleDB server with the native wire (default port 5440) and, if used, the PG wire or gRPC enabled.
