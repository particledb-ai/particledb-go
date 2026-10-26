# Go SDK comparisons (separate module)

Differential tests and benchmarks of the Go SDK against pgx and lib/pq on
PostgreSQL 17. A module of its own (`replace` to `..`), so the SDK's own
go.mod does not depend on pgx or lib/pq.

| Entry | Purpose |
|---|---|
| `differential_test.go` | The same statements through this SDK against ParticleDB and through pgx / lib/pq against PostgreSQL 17, where the semantics should match: text forms of every type, typed scans, bound parameters, SQLSTATEs, batch atomicity, `database/sql` column types and multi-statement atomicity, zoned `time.Time` into `timestamp`. Each test first proves the oracle is PostgreSQL 17 and the subject is ParticleDB on protocol 2. `TestKnownServerGap*` pin server defects the differentials found; they fail when the server changes, so the docs get updated. |
| `shopspring_test.go` | `shopspring/decimal` values as parameters and scan targets, natively and through `database/sql`. |
| `cmd/bench/main.go` | Directional benchmark: point SELECT at 1 / 32 / 256 goroutines, small UPDATE, a batch of 10,000 INSERTs, a bulk load of 1M rows and a 1M-row scan, for the ParticleDB `Pool` and `database/sql` arms and pgx (`pgxpool`, stdlib). Reports ops/s, p50 / p99 and client allocations per operation. Every arm uses TCP unless `-pdb-unix` is set. |

```bash
PDB_TEST_DSN=particledb://127.0.0.1:7173 \
PG_TEST_DSN='postgres://postgres@127.0.0.1:5433/postgres?sslmode=disable' go test .
go run ./cmd/bench -pdb particledb://127.0.0.1:7173 -pg 'postgres://...' -d 5s
```

Use a throwaway PostgreSQL 17 cluster created for the run (`initdb` on a
unique port), never a shared one; the tests drop and create `dif_*` and
`bench_*` tables.
