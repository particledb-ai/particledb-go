# Go SDK conformance bundle

Runs the ParticleDB SDK conformance suite against this Go SDK, built from
source. The JDBC driver is the suite's reference implementation. See
`SPEC.md` for what the suite checks and how results are validated.

Against a server already listening on `127.0.0.1:7173`:

```bash
./run_conformance.sh [--mode default|strict] [--results FILE]
```

Or let the runner start a fresh server, so the crash cases run too:

```bash
PDB_BIN=/path/to/particledb ./run_conformance.sh --mode strict
```

| Entry | Purpose |
|---|---|
| `run_conformance.sh` | Builds the adapter with `go build` from the SDK module, uses a running server or starts one from `PDB_BIN`, runs the mode's cases and validates the results file. `PDB_CONF_ONLY=ID,ID` runs a subset (the others are reported as skipped). |
| `adapter/` | `package main`, compiled against the SDK in `..` (same module, no go.mod of its own). `harness.go`: environment, case loader, run loop with a 180 s per-case timeout, results file, canonical value comparison (SPEC.md section 4). `script.go`: the `script` interpreter over a native `particledb.Conn` (and its `Tx`), typed parameters, canonical rendering of each cell through the typed `Scan` destination an application would use. `cases.go`: every `custom` case by id. `resp.go`: a minimal RESP2 client for the KV parity cases. |
| `cases/`, `lib/`, `SPEC.md` | Shared with every SDK's bundle. Keep them byte-identical to the JDBC bundle's copies. |

The adapter connects with the documented zero-option connection string
(`particledb://HOST`, or `particledb://HOST:PORT` when a port is configured),
because the defaults are under test. SQL-STREAM-001 sets a 256 MiB Go memory
limit (`debug.SetMemoryLimit`) and fails if the sampled peak heap in use
reaches 192 MiB. SQL-MULTI-002 runs through the `database/sql` driver,
because `NextResultSet` is where the SDK exposes every result of a
multi-statement string.
