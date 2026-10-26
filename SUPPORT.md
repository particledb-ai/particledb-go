# Go client support matrix

What an application can rely on in `github.com/particledb-ai/particledb-go`,
what it cannot, and the test behind each claim.

| Status | Meaning |
|---|---|
| **SUPPORTED** | Implemented and covered by a test. |
| **PARTIAL** | Works within the limit the row states. |
| **ABSENT** | Not implemented; the call returns an error. |
| **NO TEST** | What the code does, with no test asserting it. |

Test names are Go test functions. Files: `codec_test.go` and
`fakeserver_test.go` need no server; `native_live_test.go`,
`stdlib_live_test.go`, `pool_live_test.go` and `tls_live_test.go` run
against a server (`PDB_TEST_DSN`, `PDB_TLS_*`); `compare/differential_test.go`
also needs a PostgreSQL 17 oracle (`PG_TEST_DSN`) and checks pgx / lib/pq
there. `CONF:ID` is a case of the shared conformance suite in
`conformance/` (69 cases in default mode, 44 in strict).

## Connection and protocol

| Surface | Status | Proof |
|---|---|---|
| A connection with no options is protocol 2, multiplexed, with every capability (`v2_mux`, `prepared`, `typed_params`, `array_params`, `batch_sqlstate`, `statement_notice`, `numeric_text`, `bulk_ingest`), as the server records it | SUPPORTED | `TestLiveHandshakeDefaults`; CONF:DEF-001, DEF-002, DEF-003 |
| `NegotiatedProtocol()` / `NegotiatedCapabilities()` equal the server's record | SUPPORTED | `TestLiveHandshakeDefaults`; CONF:DEF-004 |
| Default port 7173 (`DefaultPort`); a connection string without a port uses it | SUPPORTED | `TestParseConfig`; CONF:DEF-005 |
| A server that does not negotiate protocol 2 is refused (`08004`), never used unmultiplexed | SUPPORTED | `TestFakeRefusesProtocol1` |
| The `pdb.wire_*` settings cannot be changed (`55P02`) | SUPPORTED | CONF:DEF-007 |
| Connection strings: URL (`particledb://`, `pdb://`) and key=value forms, user / password / database in either | SUPPORTED | `TestParseConfig` |
| Unix socket for a loopback host when `/tmp/.s.PDB.<port>` exists; `prefer_unix_socket=false` for TCP | SUPPORTED | `TestLiveTransportSelection` |
| Authentication (`CAP_AUTH_REQUIRED`: AUTH frame; `28P01` for bad credentials, `28000` without a user) | SUPPORTED | `TestFakeAuthentication` (scripted server) |
| Session settings at connect: `timezone` in the HELLO's session parameters, any other parameter (`search_path`, `application_name`, ...) as a `SET` after the handshake; an invalid value fails the connect with the server's SQLSTATE | SUPPORTED | `TestLiveSessionParamsAtConnect`. Server gap: the HELLO block accepts only `TimeZone` (others answer `42704`; PostgreSQL's startup packet takes any setting), hence the `SET`. |
| GOAWAY and a lost connection fail every open stream with class `08` promptly | SUPPORTED | `TestFakeGoaway`, `TestFakeLostConnectionIsClass08`; CONF:DUR-005 |
| A request refused for the stream limit (`RST_STREAM` code 4) is sent again (it never ran); more goroutines than the stream cap wait instead of failing | SUPPORTED | `TestFakeStreamLimitIsRetried`, `TestLiveStreamingAndConcurrency`; CONF:CONC-005 |

## TLS

| Surface | Status | Proof |
|---|---|---|
| Default is no TLS | SUPPORTED | `TestTLSDefaultIsPlaintext` |
| `sslmode=require` (encrypted, no verification; with `sslrootcert` it verifies the chain, as libpq) | SUPPORTED | `TestTLSModes` |
| `sslmode=verify-ca` (chain against `sslrootcert`, no host name check; refused without `sslrootcert`) | SUPPORTED | `TestTLSModes` |
| `sslmode=verify-full` (chain and host name; wrong host or wrong CA fails with `08006`) | SUPPORTED | `TestTLSModes` |
| Direct TLS with ALPN `pdb` on the database port; in-band upgrade (`sslnegotiation=upgrade`) on a dedicated native listener | SUPPORTED | `TestTLSModes` (`PDB_TLS_UPGRADE_PORT`) |
| `sslmode=prefer` / `allow` refused (could fall back to plaintext) | SUPPORTED | `TestTLSModes`, `TestParseConfig` |
| Client certificates (`sslcert`, `sslkey`) | NO TEST | Loaded into the TLS config; no test server requires them. |
| TLS never uses the Unix socket | SUPPORTED | `TestTLSModes` |

## database/sql

| Surface | Status | Proof |
|---|---|---|
| `sql.Open("particledb", dsn)`, `sql.OpenDB(NewConnector(cfg))` (`DriverContext`, `Connector`) | SUPPORTED | `TestStdlibBasics`, `TestStdlibRefusals` |
| `QueryContext`, `ExecContext` (`QueryerContext`, `ExecerContext`), `$1..$n` typed parameters | SUPPORTED | `TestStdlibBasics`, `TestDifferentialStdlib` |
| `PrepareContext` (server-side statement) | SUPPORTED | `TestStdlibBasics` |
| `BeginTx` with isolation level and read-only (`ConnBeginTx`); the level reaches the server | SUPPORTED | `TestStdlibTxAndErrors`, `TestDifferentialStdlib` |
| `Ping`, `IsValid`, `ResetSession` (a broken connection is `ErrBadConn`) | SUPPORTED | `TestStdlibBasics`, `TestFakeCancelWithoutAnswerClosesConnection` |
| `driver.ErrBadConn` only for a request that was never sent (database/sql may retry it) | SUPPORTED | `TestFakeLostConnectionIsClass08` |
| `CheckNamedValue` accepts slices (arrays), `Decimal`, `UUID`, `json.RawMessage`, time types, Valuers; named parameters refused (`0A000`) | SUPPORTED | `TestStdlibBasics`, `TestStdlibRefusals` |
| `ColumnTypeDatabaseTypeName`, `Length`, `DecimalSize`, `Nullable`, `ScanType` | SUPPORTED | `TestStdlibBasics`, `TestDifferentialStdlib` (same answers as lib/pq for the columns compared) |
| Multi-statement strings: atomic, every result reachable with `NextResultSet` | SUPPORTED | `TestStdlibMultiStatement`, `TestDifferentialStdlib`; CONF:SQL-MULTI-001, SQL-MULTI-002 |
| A multi-statement string with transaction control is refused (`0A000`, the server's rule) | SUPPORTED | `TestStdlibRefusals` |
| Context cancellation and deadlines (`57014`, also `context.DeadlineExceeded`; the connection stays usable) | SUPPORTED | `TestStdlibTxAndErrors` |
| `Conn.Raw` to the native connection (`DriverConn`) | SUPPORTED | `TestStdlibConcurrentPoolAndRaw` |
| `LastInsertId` | ABSENT | Returns an error; use `RETURNING` (`TestStdlibRefusals`). |

## Native API

| Surface | Status | Proof |
|---|---|---|
| `Connect`, `ConnectConfig`, `Conn.Exec` / `Query` / `QueryRow`, `CommandTag.RowsAffected` | SUPPORTED | `TestLiveScalarTypes`, `TestLiveParamsRoundTrip` |
| Concurrent use of one `Conn` (multiplexed statements, each its own result) | SUPPORTED | `TestLiveStreamingAndConcurrency`; CONF:CONC-001, CONC-002, CONC-004 |
| Statement cache (prepare on first use, LRU, a vanished statement prepared again once; `statement_cache_capacity`) | SUPPORTED | `TestLiveStatementCache`, `TestFakeReprepareVanishedStatement`; CONF:SQL-PREP-001, SQL-PREP-002 |
| `Conn.Prepare`, `Stmt.Query` / `Exec` / `Close`, `IsServerPrepared`, arity check (`08P01`) | SUPPORTED | `TestLiveStatementCache`; CONF:DEF-006 |
| `Pool`: shared multiplexed connections, read/write-aware routing, `MaxConns`, exclusive `Acquire` / `Release` (an open transaction is rolled back), `Begin`, `BeginFunc`, `Pool.Exec("BEGIN")` refused | SUPPORTED | `TestPoolSharedAndExclusive`; CONF:SQL-TXN-005 |
| Pool health maintenance (`MinConns`, idle time; lifetime by the same check) | SUPPORTED | `TestPoolMaintenance` |
| `CollectRows`, `CollectOneRow`, `RowTo`, `RowToMap`, `RowToStructByName` (`db` tags, case-insensitive and snake_case names) | SUPPORTED | `TestPoolSharedAndExclusive`, `TestLiveParamsRoundTrip`, `TestLiveKVSQLAndVector` |
| `RowToStructByPos`, `RowToStructByNameLax`, `RowToAddrOfStructByName`, `CollectExactlyOneRow`, `ForEachRow`, `AppendRows`; a struct field without a column is an error in the strict mapping | SUPPORTED | `TestLiveCollectHelpers` |
| Statement notices: `Config.OnNotice`, `Rows.Notices` | SUPPORTED | `TestFakeNotices` (scripted server) |

## Transactions

| Surface | Status | Proof |
|---|---|---|
| Commit / rollback visibility across connections | SUPPORTED | `TestLiveTransactions`; CONF:SQL-TXN-001 |
| Lazy BEGIN on the first statement (`FLAG_TXN_BEGIN`); an empty transaction sends nothing | SUPPORTED | `TestLiveTransactions` |
| Isolation levels (`SERIALIZABLE`, `REPEATABLE READ`, `READ COMMITTED`) and `READ ONLY` (`25006`) | SUPPORTED | `TestLiveTransactions`; CONF:SQL-TXN-003, SQL-TXN-004 |
| Savepoints (`Savepoint`, `RollbackTo`, `Release`) | SUPPORTED | `TestLiveTransactions`; CONF:SQL-TXN-002 |
| A failed transaction: `25P02`, and `Commit` returns an error matching `ErrTxCommitRollback` | SUPPORTED | `TestLiveTransactions`, `TestDifferentialSQLState` (pgx's `ErrTxCommitRollback` on PostgreSQL) |
| Serialization failure `40001` matches `ErrSerializationFailure` / `IsRetryable` | SUPPORTED | `TestSQLStateMapping`; CONF:SQL-ERR-009 |

## Types and parameters

| Surface | Status | Proof |
|---|---|---|
| Integers, floats (NaN, infinities), bool, text (Unicode up to 1 MiB), bytea (all byte values) | SUPPORTED | `TestLiveScalarTypes`, `TestDifferentialTextForms`, `TestDifferentialTypedScan`; CONF:SQL-T-001, T-002, T-004, T-005, T-006 |
| shopspring/decimal: `decimal.Decimal` parameters (by method set, no import) and results (its `sql.Scanner`), `NullDecimal` | SUPPORTED | `TestShopspringDecimal` |
| numeric as `Decimal` with each value's scale (`1.50`), binary numeric parameters, NaN / Infinity | PARTIAL | `TestLiveScalarTypes`, `TestDecimal`, `TestDifferentialParams`; CONF:SQL-T-003. Constrained and unconstrained columns retain scale (`TestNumericScalePreserved`). |
| json / jsonb (`json.RawMessage`, `json.Unmarshal` into any type) | SUPPORTED | `TestLiveScalarTypes`, `TestLiveParamsRoundTrip`; CONF:SQL-T-007 |
| date, timestamp, timestamptz (`time.Time` instant with its offset; `Date`, `Timestamp` wrappers) | SUPPORTED | `TestLiveParamsRoundTrip`, `TestDifferentialParams`, `TestDifferentialTimeIntoTimestamp`; CONF:SQL-T-008, T-009 |
| Calendar-only date / timestamp | SUPPORTED | Bind `Date` / `Timestamp` for calendar fields. `time.Time` is always an instant (timestamptz), unlike pgx's context-inferred timestamp binding. `TestDifferentialTimeIntoTimestamp`, `TestDifferentialDateFromTimestampLiteral`. |
| `time` type | PARTIAL | Returned as text, preserving microseconds. CONF:SQL-T-013. Use an explicit SQL cast when binding text. |
| uuid (`UUID`, any `[16]byte` type) | SUPPORTED | `TestLiveScalarTypes`, `TestLiveParamsRoundTrip`; CONF:SQL-T-010 |
| Arrays: slice parameters (tag 17) of every element type, NULL elements, multidimensional, empty; scanning into slices; `Array()` for database/sql | SUPPORTED | `TestLiveParamsRoundTrip`, `TestArrayText`, `TestLiteralSpelling`, `TestDifferentialStdlib` (same values as `pq.Array`); CONF:SQL-T-011 |
| vector (`Vector`, `NearestNeighbors`) | SUPPORTED | `TestLiveKVSQLAndVector`; CONF:SQL-T-012 |
| Hostile text parameters stored verbatim (no injection) | SUPPORTED | `TestLiveParamsRoundTrip`, `TestDifferentialParams`; CONF:SQL-P-001, SQL-P-002 |
| Type names of uuid / vector EXPRESSIONS | PARTIAL | Server gap: a cast expression carries no declared type, so `SELECT x::uuid` reports `TEXT` and a vector expression `BYTEA` (stored columns are right). Pinned by `TestDifferentialExpressionTypeNames`. |
| interval, numeric specials without a declared type | PARTIAL | **NO TEST**. They arrive as text and scan as strings. |

## Errors

| Surface | Status | Proof |
|---|---|---|
| `*Error` with `SQLState` for every failure; `errors.Is` sentinels; `errors.As` | SUPPORTED | `TestLiveErrorsAndSentinels`, `TestSQLStateMapping`; CONF:SQL-ERR-001 to SQL-ERR-008 |
| The same SQLSTATE as PostgreSQL for the same failures (23505, 42P01, 42601, 42703, 22012, 22P02, 42883, 23502, 23503, 25P02) | SUPPORTED | `TestDifferentialSQLState` |
| A failed batch entry carries its own SQLSTATE and `BatchIndex` | SUPPORTED | `TestLiveBatch`; CONF:SQL-BATCH-002 |

## Cancellation and timeouts

| Surface | Status | Proof |
|---|---|---|
| A cancelled context resets the statement's stream; the server answers `57014`; the connection stays usable | SUPPORTED | `TestLiveCancel`, `TestFakeCancelAnswered57014`; CONF:SQL-CANCEL-001, SQL-TIMEOUT-001 |
| Server `statement_timeout` (`57014`) | SUPPORTED | `TestLiveCancel`; CONF:SQL-TIMEOUT-001 |
| No answer within the cancel grace: the connection is closed (outcome unknown), the error is class `08` and wraps the context error | SUPPORTED | `TestFakeCancelWithoutAnswerClosesConnection` |

## Batch, bulk load and streaming

| Surface | Status | Proof |
|---|---|---|
| `Batch` / `SendBatch`: one round trip, in order, atomic outside a transaction (like pgx), results per statement | SUPPORTED | `TestLiveBatch`, `TestDifferentialBatchAtomic`; CONF:SQL-BATCH-001 |
| A batch outside a transaction refuses a statement the server will not run inside a transaction block (`TRUNCATE`: `25001`) instead of running it non-atomically | SUPPORTED | `TestLiveBatchNonTransactionalDDL` |
| Batched parameters spelled exactly as the server renders typed parameters | SUPPORTED | `TestJavaDoubleSpelling`, `TestLiteralSpelling`, `TestRenderSQLSkipsQuotedText` (golden values shared with the server's `native_literal` tests) |
| INSERT-shaped batches through bulk ingest, storing the same rows as the statement path | SUPPORTED | `TestLiveBulkBatchEqualsStatementBatch`, `TestInsertShape` |
| `CopyFrom` (`CopyFromRows`, `CopyFromSlice`, `CopyFromFunc`): native Arrow ingest, atomic, NULLs, mixed types, floats into numeric without digit loss | SUPPORTED | `TestLiveCopyFrom`, `TestLiveBulkBatchEqualsStatementBatch`; CONF:DUR-003 |
| Streaming large results with bounded memory (3M rows under a 256 MiB limit) and early close | SUPPORTED | `TestLiveStreamingAndConcurrency`; CONF:SQL-STREAM-001, CONC-003 |

## Key-value

| Surface | Status | Proof |
|---|---|---|
| `KVSet`, `KVGet`, `KVDel`, `KVIncr` (binary safe, RESP keyspace, not transactional) | SUPPORTED | `TestLiveKV`; CONF:KV-001 to KV-003, KV-006, KV-008, KV-009, KV-012 |
| `KVExpire`, `KVTTL`, `KVMSet`, `KVMGet` | SUPPORTED | `TestLiveKVSQLAndVector`; CONF:KV-004, KV-005, KV-007, KV-010 |

## Durability (conformance, server under kill -9)

CONF:DUR-001 to DUR-005 check default and strict modes: acknowledged
autocommit inserts, COMMITs, batches and key-value writes survive `kill -9`;
a lost connection reports class `08` within seconds.

## Known gaps

1. `LastInsertId` is not supported; use `RETURNING`.
2. Named parameters (`sql.Named`) are refused.
3. The HELLO session block accepts only `TimeZone`. Other settings are
   applied with SET after the handshake. Date literals, UUID / vector
   expression types and unconstrained numeric scale now have passing regressions.
4. `DISCARD ALL` does not drop the native wire's prepared statements on the
   server (PostgreSQL's does); the cache keeps using them.
