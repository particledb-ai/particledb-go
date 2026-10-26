// Command bench measures this SDK against pgx on PostgreSQL (directional,
// one machine). Every arm runs over TCP unless -pdb-unix is set.
//
//	go run ./cmd/bench -pdb particledb://127.0.0.1:27300 -pg postgres://postgres@127.0.0.1:27350/postgres
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	pdb "github.com/particledb-ai/particledb-go"
)

var (
	pdbDSN   = flag.String("pdb", "particledb://127.0.0.1:27300", "ParticleDB DSN")
	pgDSN    = flag.String("pg", "postgres://postgres@127.0.0.1:27350/postgres?sslmode=disable", "PostgreSQL DSN")
	dur      = flag.Duration("d", 5*time.Second, "duration of each timed arm")
	only     = flag.String("only", "", "comma-separated workloads: point,update,batch,copy,scan")
	unix     = flag.Bool("pdb-unix", false, "let the ParticleDB client use the Unix socket")
	rowsN    = flag.Int("rows", 100000, "rows in the point/update table")
	copyRows = flag.Int("copy-rows", 1000000, "rows for the bulk load and scan")
	gors     = flag.String("g", "1,32,256", "goroutine counts for point SELECT")
	stdlib   = flag.Bool("stdlib", true, "also measure database/sql arms")
	maxConns = flag.Int("conns", 0, "pool size (0 = driver default)")
	pdbExtra = flag.String("pdb-params", "", "extra ParticleDB connection parameters (k=v&k=v)")
	noPG     = flag.Bool("no-pg", false, "skip the PostgreSQL arms")
)

type result struct {
	name     string
	ops      int64
	elapsed  time.Duration
	p50, p99 time.Duration
	allocs   float64
	bytes    float64
}

func (r result) String() string {
	if r.ops == 0 && r.elapsed == 0 {
		return "| " + r.name + " | skipped | | | | |"
	}
	ops := float64(r.ops) / r.elapsed.Seconds()
	return fmt.Sprintf("| %-44s | %12.0f | %9s | %9s | %8.1f | %9.0f |", r.name, ops, fmtDur(r.p50), fmtDur(r.p99), r.allocs, r.bytes)
}

func fmtDur(d time.Duration) string {
	switch {
	case d == 0:
		return "-"
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fus", float64(d)/1e3)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/1e6)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// timed runs fn from g goroutines for *dur and returns throughput, latency
// percentiles and client allocations per operation.
func timed(name string, g int, fn func(ctx context.Context, w int, rng *rand.Rand) error) result {
	if *noPG && strings.Contains(name, "PG17") {
		return result{name: name}
	}
	ctx := context.Background()
	// warmup
	warm(ctx, g, fn, *dur/5)
	runtime.GC()
	var ms0, ms1 runtime.MemStats
	runtime.ReadMemStats(&ms0)
	lat := make([][]time.Duration, g)
	var ops atomic.Int64
	var failed atomic.Value
	stop := time.Now().Add(*dur)
	var wg sync.WaitGroup
	t0 := time.Now()
	for w := 0; w < g; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 1))
			l := make([]time.Duration, 0, 1<<16)
			for time.Now().Before(stop) {
				s := time.Now()
				if err := fn(ctx, w, rng); err != nil {
					failed.Store(err)
					return
				}
				l = append(l, time.Since(s))
				ops.Add(1)
			}
			lat[w] = l
		}(w)
	}
	wg.Wait()
	el := time.Since(t0)
	runtime.ReadMemStats(&ms1)
	if err, _ := failed.Load().(error); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
	}
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	r := result{name: name, ops: ops.Load(), elapsed: el}
	if len(all) > 0 {
		r.p50 = all[len(all)/2]
		r.p99 = all[len(all)*99/100]
	}
	if r.ops > 0 {
		r.allocs = float64(ms1.Mallocs-ms0.Mallocs) / float64(r.ops)
		r.bytes = float64(ms1.TotalAlloc-ms0.TotalAlloc) / float64(r.ops)
	}
	return r
}

func warm(ctx context.Context, g int, fn func(ctx context.Context, w int, rng *rand.Rand) error, d time.Duration) {
	stop := time.Now().Add(d)
	var wg sync.WaitGroup
	for w := 0; w < g; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + 99))
			for time.Now().Before(stop) {
				if fn(ctx, w, rng) != nil {
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

// once times a single operation (best of n runs).
func once(name string, n int, units int64, fn func() error) result {
	if *noPG && strings.Contains(name, "PG17") {
		return result{name: name}
	}
	best := time.Duration(1 << 62)
	var allocs, bytes float64
	for i := 0; i < n; i++ {
		runtime.GC()
		var ms0, ms1 runtime.MemStats
		runtime.ReadMemStats(&ms0)
		t0 := time.Now()
		if err := fn(); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			return result{name: name}
		}
		el := time.Since(t0)
		runtime.ReadMemStats(&ms1)
		if el < best {
			best = el
			allocs = float64(ms1.Mallocs-ms0.Mallocs) / float64(units)
			bytes = float64(ms1.TotalAlloc-ms0.TotalAlloc) / float64(units)
		}
	}
	return result{name: name, ops: units, elapsed: best, p50: best, allocs: allocs, bytes: bytes}
}

func want(w string) bool {
	if *only == "" {
		return true
	}
	for _, x := range strings.Split(*only, ",") {
		if x == w {
			return true
		}
	}
	return false
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	pd := *pdbDSN
	if !*unix {
		pd = addParam(pd, "prefer_unix_socket=false")
	}
	if *maxConns > 0 {
		pd = addParam(pd, fmt.Sprintf("pool_max_conns=%d", *maxConns))
	}
	if *pdbExtra != "" {
		pd = addParam(pd, *pdbExtra)
	}
	pp, err := pdb.NewPool(ctx, pd)
	must(err)
	defer pp.Close()
	pgc := *pgDSN
	if *maxConns > 0 {
		pgc = addParam(pgc, fmt.Sprintf("pool_max_conns=%d", *maxConns))
	}
	gp, err := pgxpool.New(ctx, pgc)
	must(err)
	defer gp.Close()

	fmt.Printf("bench %s, GOMAXPROCS=%d, duration %v, pdb pool max %d, pgx pool max %d\n\n", time.Now().Format(time.RFC3339), runtime.GOMAXPROCS(0), *dur, pp.Config().MaxConns, gp.Config().MaxConns)
	header := "| arm | ops/s | p50 | p99 | allocs/op | bytes/op |\n|---|---:|---:|---:|---:|---:|"

	setupKV := func() {
		_, err := pp.Exec(ctx, "DROP TABLE IF EXISTS bench_kv")
		must(err)
		_, err = pp.Exec(ctx, "CREATE TABLE bench_kv (id int8 PRIMARY KEY, v text NOT NULL, n int8 NOT NULL)")
		must(err)
		_, err = gp.Exec(ctx, "DROP TABLE IF EXISTS bench_kv")
		must(err)
		_, err = gp.Exec(ctx, "CREATE TABLE bench_kv (id int8 PRIMARY KEY, v text NOT NULL, n int8 NOT NULL)")
		must(err)
		_, err = pp.CopyFrom(ctx, pdb.Identifier{"bench_kv"}, []string{"id", "v", "n"}, pdb.CopyFromSlice(*rowsN, func(i int) ([]any, error) {
			return []any{int64(i), fmt.Sprintf("value-%d", i), int64(0)}, nil
		}))
		must(err)
		_, err = gp.CopyFrom(ctx, pgx.Identifier{"bench_kv"}, []string{"id", "v", "n"}, pgx.CopyFromSlice(*rowsN, func(i int) ([]any, error) {
			return []any{int64(i), fmt.Sprintf("value-%d", i), int64(0)}, nil
		}))
		must(err)
		_, err = gp.Exec(ctx, "ANALYZE bench_kv")
		must(err)
	}
	var sdb, sdbPG *sql.DB
	if *stdlib {
		sdb, _ = sql.Open("particledb", pd)
		sdbPG, _ = sql.Open("pgx", *pgDSN)
		mc := pp.Config().MaxConns
		sdb.SetMaxOpenConns(mc)
		sdb.SetMaxIdleConns(mc)
		sdbPG.SetMaxOpenConns(int(gp.Config().MaxConns))
		sdbPG.SetMaxIdleConns(int(gp.Config().MaxConns))
		defer sdb.Close()
		defer sdbPG.Close()
	}
	if want("point") || want("update") {
		setupKV()
	}
	if want("point") {
		fmt.Print("### Point SELECT by primary key (" + fmt.Sprint(*rowsN) + " rows)\n\n")
		fmt.Println(header)
		for _, gs := range strings.Split(*gors, ",") {
			var g int
			fmt.Sscan(gs, &g)
			q := "SELECT v FROM bench_kv WHERE id = $1"
			fmt.Println(timed(fmt.Sprintf("particledb Pool, %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
				var v string
				return pp.QueryRow(ctx, q, rng.Int63n(int64(*rowsN))).Scan(&v)
			}))
			fmt.Println(timed(fmt.Sprintf("pgx pgxpool (PG17), %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
				var v string
				return gp.QueryRow(ctx, q, rng.Int63n(int64(*rowsN))).Scan(&v)
			}))
			if *stdlib {
				fmt.Println(timed(fmt.Sprintf("particledb database/sql, %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
					var v string
					return sdb.QueryRowContext(ctx, q, rng.Int63n(int64(*rowsN))).Scan(&v)
				}))
				fmt.Println(timed(fmt.Sprintf("pgx stdlib database/sql (PG17), %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
					var v string
					return sdbPG.QueryRowContext(ctx, q, rng.Int63n(int64(*rowsN))).Scan(&v)
				}))
			}
		}
		fmt.Println()
	}
	if want("update") {
		fmt.Print("### Small UPDATE by primary key (autocommit, durable commit)\n\n")
		fmt.Println(header)
		q := "UPDATE bench_kv SET n = n + 1 WHERE id = $1"
		for _, g := range []int{1, 32} {
			fmt.Println(timed(fmt.Sprintf("particledb Pool, %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
				_, err := pp.Exec(ctx, q, rng.Int63n(int64(*rowsN)))
				return err
			}))
			fmt.Println(timed(fmt.Sprintf("pgx pgxpool (PG17), %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
				_, err := gp.Exec(ctx, q, rng.Int63n(int64(*rowsN)))
				return err
			}))
			if *stdlib {
				fmt.Println(timed(fmt.Sprintf("particledb database/sql, %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
					_, err := sdb.ExecContext(ctx, q, rng.Int63n(int64(*rowsN)))
					return err
				}))
				fmt.Println(timed(fmt.Sprintf("pgx stdlib database/sql (PG17), %d goroutines", g), g, func(ctx context.Context, w int, rng *rand.Rand) error {
					_, err := sdbPG.ExecContext(ctx, q, rng.Int63n(int64(*rowsN)))
					return err
				}))
			}
		}
		fmt.Println()
	}
	if want("batch") {
		fmt.Print("### Batch of 10,000 INSERTs, one round trip, atomic (rows/s)\n\n")
		fmt.Println(header)
		for _, s := range []string{"DROP TABLE IF EXISTS bench_batch", "CREATE TABLE bench_batch (id int8 PRIMARY KEY, a int8, b text)"} {
			_, err := pp.Exec(ctx, s)
			must(err)
			_, err = gp.Exec(ctx, s)
			must(err)
		}
		base := int64(0)
		fmt.Println(once("particledb SendBatch (10k INSERT)", 3, 10000, func() error {
			b := &pdb.Batch{}
			for i := 0; i < 10000; i++ {
				b.Queue("INSERT INTO bench_batch VALUES ($1, $2, $3)", base+int64(i), int64(i), "payload-text")
			}
			base += 10000
			return pp.SendBatch(ctx, b).Close()
		}))
		base = 0
		fmt.Println(once("pgx SendBatch (PG17, 10k INSERT)", 3, 10000, func() error {
			b := &pgx.Batch{}
			for i := 0; i < 10000; i++ {
				b.Queue("INSERT INTO bench_batch VALUES ($1, $2, $3)", base+int64(i), int64(i), "payload-text")
			}
			base += 10000
			return gp.SendBatch(ctx, b).Close()
		}))
		fmt.Println()
	}
	if want("copy") || want("scan") {
		fmt.Printf("### Bulk load of %d rows (rows/s)\n\n", *copyRows)
		fmt.Println(header)
		gen := func(i int) ([]any, error) {
			return []any{int64(i), int64(i * 7), fmt.Sprintf("name-%08d", i), float64(i) * 0.5}, nil
		}
		ddl := []string{"DROP TABLE IF EXISTS bench_bulk", "CREATE TABLE bench_bulk (id int8 PRIMARY KEY, a int8, b text, c float8)"}
		fmt.Println(once("particledb CopyFrom (native Arrow ingest)", 1, int64(*copyRows), func() error {
			for _, s := range ddl {
				if _, err := pp.Exec(ctx, s); err != nil {
					return err
				}
			}
			n, err := pp.CopyFrom(ctx, pdb.Identifier{"bench_bulk"}, []string{"id", "a", "b", "c"}, pdb.CopyFromSlice(*copyRows, gen))
			if err == nil && n != int64(*copyRows) {
				err = fmt.Errorf("loaded %d", n)
			}
			return err
		}))
		fmt.Println(once("pgx CopyFrom (PG17, COPY binary)", 1, int64(*copyRows), func() error {
			for _, s := range ddl {
				if _, err := gp.Exec(ctx, s); err != nil {
					return err
				}
			}
			n, err := gp.CopyFrom(ctx, pgx.Identifier{"bench_bulk"}, []string{"id", "a", "b", "c"}, pgx.CopyFromSlice(*copyRows, gen))
			if err == nil && n != int64(*copyRows) {
				err = fmt.Errorf("loaded %d", n)
			}
			return err
		}))
		fmt.Println()
	}
	if want("scan") {
		fmt.Printf("### Scan of %d rows (rows/s)\n\n", *copyRows)
		fmt.Println(header)
		q := "SELECT id, a, b, c FROM bench_bulk"
		fmt.Println(once("particledb Query + Scan", 3, int64(*copyRows), func() error {
			rows, err := pp.Query(ctx, q)
			if err != nil {
				return err
			}
			var id, a int64
			var b string
			var c float64
			var n int
			for rows.Next() {
				if err := rows.Scan(&id, &a, &b, &c); err != nil {
					return err
				}
				n++
			}
			if n != *copyRows {
				return fmt.Errorf("scanned %d", n)
			}
			return rows.Err()
		}))
		fmt.Println(once("pgx Query + Scan (PG17)", 3, int64(*copyRows), func() error {
			rows, err := gp.Query(ctx, q)
			if err != nil {
				return err
			}
			var id, a int64
			var b string
			var c float64
			var n int
			for rows.Next() {
				if err := rows.Scan(&id, &a, &b, &c); err != nil {
					return err
				}
				n++
			}
			if n != *copyRows {
				return fmt.Errorf("scanned %d", n)
			}
			return rows.Err()
		}))
		fmt.Println()
	}
}

func addParam(dsn, p string) string {
	if strings.Contains(dsn, "?") {
		return dsn + "&" + p
	}
	return dsn + "?" + p
}
