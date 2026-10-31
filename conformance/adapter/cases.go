package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	particledb "github.com/particledb-ai/particledb-go"
)

func connect() *particledb.Conn {
	c, err := particledb.Connect(bg, dsn())
	must(err, "connect")
	return c
}

func exec(c *particledb.Conn, q string, args ...any) {
	_, err := c.Exec(bg, q, args...)
	must(err, q)
}

func scalarLong(c *particledb.Conn, q string, args ...any) int64 {
	var v int64
	must(c.QueryRow(bg, q, args...).Scan(&v), q)
	return v
}

func scalarString(c *particledb.Conn, q string) string {
	var v string
	must(c.QueryRow(bg, q).Scan(&v), q)
	return v
}

// sqlLong runs a key-value SQL function and reads its answer as a number,
// as the JDBC adapter does (numbers, booleans, 'OK').
func sqlLong(c *particledb.Conn, q string, args ...any) int64 {
	var v any
	must(c.QueryRow(bg, q, args...).Scan(&v), q)
	switch x := v.(type) {
	case int64:
		return x
	case float64:
		return int64(x)
	case bool:
		if x {
			return 1
		}
		return 0
	case string:
		if x == "OK" {
			return 1
		}
		var n int64
		_, err := fmt.Sscan(x, &n)
		must(err, "number from "+q)
		return n
	case particledb.Decimal:
		return int64(x.Float64())
	}
	fail("%s: unexpected answer %T %v", q, v, v)
	return 0
}

func capSet(csv string) []string {
	var out []string
	for _, p := range strings.Split(csv, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func customCases(defs map[string]caseDef) map[string]func() {
	m := map[string]func(){}
	var required []string
	for _, r := range defs["DEF-002"]["required_capabilities"].([]any) {
		required = append(required, r.(string))
	}

	m["DEF-001"] = func() {
		c := connect()
		defer c.Close()
		eq("pdb-native-v2", scalarString(c, "SHOW pdb.wire_protocol"), "server-recorded protocol")
	}
	m["DEF-002"] = func() {
		c := connect()
		defer c.Close()
		got := capSet(scalarString(c, "SHOW pdb.wire_capabilities"))
		var missing []string
		for _, r := range required {
			found := false
			for _, g := range got {
				found = found || g == r
			}
			if !found {
				missing = append(missing, r)
			}
		}
		check(len(missing) == 0, "server recorded %v; missing required %v", got, missing)
	}
	m["DEF-003"] = func() {
		c := connect()
		defer c.Close()
		eq("arrow_ipc", scalarString(c, "SHOW pdb.wire_result_format"), "server-recorded result format")
	}
	m["DEF-004"] = func() {
		c := connect()
		defer c.Close()
		eq(scalarString(c, "SHOW pdb.wire_protocol"), c.NegotiatedProtocol(), "client protocol vs server record")
		eq(strings.Join(capSet(scalarString(c, "SHOW pdb.wire_capabilities")), ","), strings.Join(c.NegotiatedCapabilities(), ","),
			"client capability set vs server record")
	}
	m["DEF-005"] = func() {
		eq(defaultPort(), particledb.DefaultPort, "SDK default port vs server default port")
		if port() != defaultPort() {
			skip(fmt.Sprintf("default-port constant checked; the port-less connect needs the runner's server on %d but that port was busy (server on %d)", defaultPort(), port()))
		}
		c, err := particledb.Connect(bg, "particledb://"+host())
		must(err, "port-less connect")
		defer c.Close()
		eq("pdb-native-v2", scalarString(c, "SHOW pdb.wire_protocol"), "port-less connect protocol")
	}
	m["DEF-006"] = func() {
		hostile := "o'clock; DROP TABLE x -- "
		c := connect()
		defer c.Close()
		st, err := c.Prepare(bg, "SELECT $1::text || ''")
		must(err, "prepare")
		for i := 0; i < 10; i++ {
			var got string
			must(st.QueryRow(bg, hostile).Scan(&got), "execute")
			eq(hostile, got, fmt.Sprintf("bound text param, execution %d", i))
		}
		check(st.IsServerPrepared(), "statement not server-prepared after 10 executions")
		st.Close(bg)
	}

	// ── SQL custom ──
	m["SQL-PREP-001"] = func() {
		c := connect()
		defer c.Close()
		exec(c, "DROP TABLE IF EXISTS conf_prep1")
		exec(c, "CREATE TABLE conf_prep1 (id int8 PRIMARY KEY, v text)")
		exec(c, "INSERT INTO conf_prep1 SELECT g, 'v' || g FROM generate_series(1, 50) g")
		st, err := c.Prepare(bg, "SELECT v FROM conf_prep1 WHERE id = $1")
		must(err, "prepare")
		defer st.Close(bg)
		for pass := 0; pass < 2; pass++ {
			for k := 1; k <= 50; k++ {
				id := k
				if pass == 1 {
					id = 51 - k
				}
				rows, err := st.Query(bg, id)
				must(err, "execute")
				vs, err := particledb.CollectRows(rows, particledb.RowTo[string])
				must(err, "rows")
				eq(fmt.Sprintf("[v%d]", id), fmt.Sprint(vs), fmt.Sprintf("id %d", id))
			}
		}
	}
	m["SQL-PREP-002"] = func() {
		c := connect()
		defer c.Close()
		exec(c, "DROP TABLE IF EXISTS conf_prep2")
		exec(c, "CREATE TABLE conf_prep2 (id int8 PRIMARY KEY, a text)")
		exec(c, "INSERT INTO conf_prep2 VALUES (1, 'x')")
		st, err := c.Prepare(bg, "SELECT * FROM conf_prep2 WHERE id = $1")
		must(err, "prepare")
		defer st.Close(bg)
		for i := 0; i < 6; i++ {
			rows, err := st.Query(bg, 1)
			must(err, "execute")
			eq(2, len(rows.FieldDescriptions()), "width before ALTER")
			rows.Close()
		}
		exec(c, "ALTER TABLE conf_prep2 ADD COLUMN b int8")
		exec(c, "UPDATE conf_prep2 SET b = 5")
		rows, err := st.Query(bg, 1)
		if err != nil {
			stt := particledb.SQLState(err)
			check(stt == "0A000" || stt == "26000", "after ALTER: unexpected %v [%s]", err, stt)
			return
		}
		defer rows.Close()
		check(rows.Next(), "row after ALTER (%v)", rows.Err())
		eq(3, len(rows.FieldDescriptions()), "width after ALTER (stale describe?)")
		var id, b int64
		var a string
		must(rows.Scan(&id, &a, &b), "scan after ALTER")
		eq(int64(5), b, "column b after ALTER")
	}
	m["SQL-BATCH-001"] = func() {
		c := connect()
		defer c.Close()
		exec(c, "DROP TABLE IF EXISTS conf_b1")
		exec(c, "CREATE TABLE conf_b1 (id int8 PRIMARY KEY, v text)")
		b := &particledb.Batch{}
		for i := 1; i <= 1000; i++ {
			b.Queue("INSERT INTO conf_b1 VALUES ($1, $2)", i, fmt.Sprintf("r%d", i))
		}
		br := c.SendBatch(bg, b)
		for i := 0; i < 1000; i++ {
			tag, err := br.Exec()
			must(err, fmt.Sprintf("batch entry %d", i))
			check(tag.RowsAffected() == 1, "count[%d] = %d (expected 1)", i, tag.RowsAffected())
		}
		must(br.Close(), "batch")
		eq(int64(1000), scalarLong(c, "SELECT count(*) FROM conf_b1"), "rows")
		eq(int64(500500), scalarLong(c, "SELECT sum(id) FROM conf_b1"), "sum")
	}
	m["SQL-BATCH-002"] = func() {
		c := connect()
		defer c.Close()
		exec(c, "DROP TABLE IF EXISTS conf_b2")
		exec(c, "CREATE TABLE conf_b2 (id int8 PRIMARY KEY)")
		tx, err := c.Begin(bg)
		must(err, "begin")
		b := &particledb.Batch{}
		for _, id := range []int64{1, 2, 1, 3} {
			b.Queue("INSERT INTO conf_b2 VALUES ($1)", id)
		}
		err = tx.SendBatch(bg, b).Close()
		check(err != nil, "batch with a duplicate key succeeded")
		eq("23505", particledb.SQLState(err), "batch failure SQLSTATE ("+fmt.Sprint(err)+")")
		must(tx.Rollback(bg), "rollback")
		eq(int64(0), scalarLong(c, "SELECT count(*) FROM conf_b2"), "rows after rollback")
	}
	m["SQL-STREAM-001"] = func() {
		const limit = 256 << 20
		old := debug.SetMemoryLimit(limit)
		defer debug.SetMemoryLimit(old)
		runtime.GC()
		var peak atomic.Uint64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			var ms runtime.MemStats
			for {
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > peak.Load() {
					peak.Store(ms.HeapInuse)
				}
				select {
				case <-stop:
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
		}()
		c := connect()
		defer c.Close()
		t0 := time.Now()
		rows, err := c.Query(bg, "SELECT g, repeat('x', 100) FROM generate_series(1, 3000000) g")
		must(err, "query")
		var first time.Duration = -1
		var n, sum, g int64
		for rows.Next() {
			if first < 0 {
				first = time.Since(t0)
			}
			must(rows.Scan(&g, nil), "scan")
			n++
			sum += g
		}
		must(rows.Err(), "rows")
		total := time.Since(t0)
		close(stop)
		wg.Wait()
		eq(int64(3000000), n, "rows")
		eq(int64(4500001500000), sum, "sum")
		check(peak.Load() < 192<<20, "peak heap in use %d MiB (limit 256 MiB): the result was not streamed", peak.Load()>>20)
		check(first < total/2, "first row only after %v of %v total: the result was buffered, not streamed", first, total)
	}
	m["SQL-MULTI-001"] = func() {
		c := connect()
		defer c.Close()
		exec(c, "DROP TABLE IF EXISTS conf_m1")
		exec(c, "CREATE TABLE conf_m1 (id int8 PRIMARY KEY)")
		_, err := c.Exec(bg, "INSERT INTO conf_m1 VALUES (1); INSERT INTO conf_m1 VALUES (1)")
		check(err != nil, "multi-statement with a duplicate key succeeded")
		eq("23505", particledb.SQLState(err), "multi-statement failure SQLSTATE ("+err.Error()+")")
		eq(int64(0), scalarLong(c, "SELECT count(*) FROM conf_m1"), "rows (implicit transaction must roll back statement 1)")
	}
	m["SQL-MULTI-002"] = func() {
		db, err := sql.Open("particledb", dsn())
		must(err, "sql.Open")
		defer db.Close()
		_, err = db.Exec("DROP TABLE IF EXISTS conf_m2")
		must(err, "drop")
		_, err = db.Exec("CREATE TABLE conf_m2 (id int8 PRIMARY KEY)")
		must(err, "create")
		rows, err := db.Query("INSERT INTO conf_m2 VALUES (1); INSERT INTO conf_m2 VALUES (2); SELECT count(*) FROM conf_m2")
		must(err, "multi-statement query")
		defer rows.Close()
		var seen *int64
		for guard := 0; guard < 10; guard++ {
			for rows.Next() {
				var v int64
				must(rows.Scan(&v), "scan")
				seen = &v
			}
			if !rows.NextResultSet() {
				break
			}
		}
		must(rows.Err(), "rows")
		check(seen != nil, "no result set carried the final SELECT")
		eq(int64(2), *seen, "final SELECT result of the multi-statement string")
	}
	m["SQL-TXN-005"] = func() {
		a := connect()
		defer a.Close()
		pool, err := particledb.NewPool(bg, dsn())
		must(err, "pool")
		defer pool.Close()
		exec(a, "DROP TABLE IF EXISTS conf_x5")
		exec(a, "CREATE TABLE conf_x5 (id int8 PRIMARY KEY, v int8)")
		exec(a, "INSERT INTO conf_x5 VALUES (1, 0)")
		for i := int64(1); i <= 200; i++ {
			tag, err := a.Exec(bg, "UPDATE conf_x5 SET v = $1 WHERE id = 1", i)
			must(err, "update")
			eq(int64(1), tag.RowsAffected(), fmt.Sprintf("update count %d", i))
			eq(i, scalarLong(a, "SELECT v FROM conf_x5 WHERE id = $1", 1), fmt.Sprintf("same-connection read %d", i))
			var v int64
			must(pool.QueryRow(bg, "SELECT v FROM conf_x5 WHERE id = $1", 1).Scan(&v), "pool read")
			eq(i, v, fmt.Sprintf("pool-connection read %d", i))
		}
	}
	m["SQL-CANCEL-001"] = func() {
		c := connect()
		defer c.Close()
		ctx, cancel := context.WithCancel(bg)
		var cancelledAt atomic.Int64
		go func() {
			time.Sleep(300 * time.Millisecond)
			cancelledAt.Store(time.Now().UnixNano())
			cancel()
		}()
		_, err := c.Exec(ctx, "SELECT pg_sleep(30)")
		check(err != nil, "long statement completed despite cancel")
		eq("57014", particledb.SQLState(err), "cancelled statement SQLSTATE ("+err.Error()+")")
		check(time.Since(time.Unix(0, cancelledAt.Load())) < 5*time.Second, "cancel took more than 5 s")
		eq(int64(1), scalarLong(c, "SELECT 1"), "connection usable after cancel")
	}
	m["SQL-TIMEOUT-001"] = func() {
		c := connect()
		defer c.Close()
		ctx, cancel := context.WithTimeout(bg, time.Second)
		t0 := time.Now()
		_, err := c.Exec(ctx, "SELECT pg_sleep(30)")
		cancel()
		check(err != nil, "1 s deadline statement completed")
		eq("57014", particledb.SQLState(err), "deadline SQLSTATE ("+err.Error()+")")
		check(time.Since(t0) < 6*time.Second, "1 s deadline took %v", time.Since(t0))
		check(!c.IsClosed() && c.Ping(bg) == nil, "connection unusable after deadline expiry")
		exec(c, "SET statement_timeout = '500ms'")
		t0 = time.Now()
		_, err = c.Exec(bg, "SELECT pg_sleep(30)")
		check(err != nil, "statement_timeout statement completed")
		eq("57014", particledb.SQLState(err), "statement_timeout SQLSTATE ("+err.Error()+")")
		check(time.Since(t0) < 6*time.Second, "statement_timeout took %v", time.Since(t0))
		exec(c, "RESET statement_timeout")
		eq(int64(1), scalarLong(c, "SELECT 1"), "connection usable after statement_timeout")
	}

	// ── concurrency ──
	m["CONC-001"] = func() {
		c := connect()
		defer c.Close()
		exec(c, "DROP TABLE IF EXISTS conf_c1")
		exec(c, "CREATE TABLE conf_c1 (id int8 PRIMARY KEY, v text)")
		exec(c, "INSERT INTO conf_c1 SELECT g, 'row-' || g FROM generate_series(1, 1000) g")
		var ok atomic.Int64
		var mu sync.Mutex
		var errs []string
		var wg sync.WaitGroup
		for t := 0; t < 32; t++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				r := rand.New(rand.NewSource(seed))
				for i := 0; i < 200; i++ {
					id := int64(1 + r.Intn(1000))
					var rows *particledb.Rows
					var err error
					if i&1 == 0 {
						rows, err = c.Query(bg, "SELECT id, v FROM conf_c1 WHERE id = $1", id)
					} else {
						rows, err = c.Query(bg, fmt.Sprintf("SELECT id, v FROM conf_c1 WHERE id = %d", id))
					}
					e := ""
					if err != nil {
						e = err.Error()
					} else {
						var got int64
						var v string
						if !rows.Next() {
							e = fmt.Sprintf("no row for %d (%v)", id, rows.Err())
						} else if err := rows.Scan(&got, &v); err != nil {
							e = err.Error()
						} else if got != id || v != fmt.Sprintf("row-%d", id) {
							e = fmt.Sprintf("asked %d, got [%d, %s]", id, got, v)
						} else if rows.Next() {
							e = fmt.Sprintf("extra row for %d", id)
						}
						rows.Close()
					}
					if e != "" {
						mu.Lock()
						errs = append(errs, e)
						mu.Unlock()
					} else {
						ok.Add(1)
					}
				}
			}(int64(t))
		}
		wg.Wait()
		check(len(errs) == 0, "%d errors, first: %v", len(errs), errs)
		eq(int64(6400), ok.Load(), "successful statements")
		eq("pdb-native-v2", scalarString(c, "SHOW pdb.wire_protocol"), "protocol of the shared connection")
	}
	m["CONC-002"] = func() {
		c := connect()
		defer c.Close()
		t0 := time.Now()
		slowDone := make(chan time.Time, 1)
		go func() {
			_, err := c.Exec(bg, "SELECT pg_sleep(2)")
			if err != nil {
				slowDone <- time.Time{}
				return
			}
			slowDone <- time.Now()
		}()
		time.Sleep(100 * time.Millisecond)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var lastFast time.Time
		var slowOnes []string
		var errs []error
		for t := 0; t < 8; t++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 20; i++ {
					s0 := time.Now()
					var x int64
					err := c.QueryRow(bg, "SELECT 1").Scan(&x)
					d := time.Since(s0)
					mu.Lock()
					if err != nil {
						errs = append(errs, err)
					}
					if d > time.Second {
						slowOnes = append(slowOnes, d.String())
					}
					if time.Now().After(lastFast) {
						lastFast = time.Now()
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		sd := <-slowDone
		check(len(errs) == 0, "fast statement errors: %v", errs)
		check(!sd.IsZero(), "the slow statement failed")
		check(len(slowOnes) == 0, "fast statements blocked behind the slow one: %v", slowOnes)
		check(lastFast.Before(sd), "fast statements finished %v AFTER the slow one (head-of-line blocking); slow took %v", lastFast.Sub(sd), sd.Sub(t0))
	}
	m["CONC-003"] = func() {
		c := connect()
		defer c.Close()
		type res struct{ n, sum, disorder int64 }
		run := func(q string, sign int64) res {
			rows, err := c.Query(bg, q)
			must(err, q)
			defer rows.Close()
			var r res
			var prev, v int64
			for rows.Next() {
				must(rows.Scan(&v), "scan")
				if r.n > 0 && sign*v != sign*prev+1 {
					r.disorder++
				}
				prev = v
				r.n++
				r.sum += v
			}
			must(rows.Err(), "rows")
			return r
		}
		var ra, rb res
		var wg sync.WaitGroup
		var panicked atomic.Value
		wg.Add(2)
		go func() {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					panicked.Store(fmt.Sprint(p))
				}
			}()
			ra = run("SELECT g FROM generate_series(1, 200000) g", 1)
		}()
		go func() {
			defer wg.Done()
			defer func() {
				if p := recover(); p != nil {
					panicked.Store(fmt.Sprint(p))
				}
			}()
			rb = run("SELECT -g FROM generate_series(1, 200000) g", -1)
		}()
		wg.Wait()
		if p := panicked.Load(); p != nil {
			fail("%v", p)
		}
		eq(int64(200000), ra.n, "stream A rows")
		eq(int64(20000100000), ra.sum, "stream A sum")
		eq(int64(0), ra.disorder, "stream A out-of-order values")
		eq(int64(200000), rb.n, "stream B rows")
		eq(int64(-20000100000), rb.sum, "stream B sum")
		eq(int64(0), rb.disorder, "stream B out-of-order values")
	}
	m["CONC-004"] = func() {
		a, b, z := connect(), connect(), connect()
		defer a.Close()
		defer b.Close()
		defer z.Close()
		exec(z, "DROP TABLE IF EXISTS conf_c4")
		exec(z, "CREATE TABLE conf_c4 (id int8 PRIMARY KEY, owner text)")
		ta, err := a.Begin(bg)
		must(err, "begin a")
		tb, err := b.Begin(bg)
		must(err, "begin b")
		_, err = ta.Exec(bg, "INSERT INTO conf_c4 VALUES (1, 'a')")
		must(err, "insert a")
		_, err = tb.Exec(bg, "INSERT INTO conf_c4 VALUES (2, 'b')")
		must(err, "insert b")
		got := make([]string, 16)
		errs := make([]error, 16)
		var wg sync.WaitGroup
		for t := 0; t < 16; t++ {
			tx := ta
			if t%2 == 1 {
				tx = tb
			}
			wg.Add(1)
			go func(t int, tx *particledb.Tx) {
				defer wg.Done()
				rows, err := tx.Query(bg, "SELECT owner FROM conf_c4 ORDER BY id")
				if err != nil {
					errs[t] = err
					return
				}
				vs, err := particledb.CollectRows(rows, particledb.RowTo[string])
				errs[t] = err
				got[t] = strings.Join(vs, "")
			}(t, tx)
		}
		wg.Wait()
		for t := 0; t < 16; t++ {
			must(errs[t], fmt.Sprintf("thread %d", t))
			want := "a"
			if t%2 == 1 {
				want = "b"
			}
			eq(want, got[t], fmt.Sprintf("thread %d view inside its connection's transaction", t))
		}
		must(ta.Commit(bg), "commit a")
		must(tb.Commit(bg), "commit b")
		eq("ab", scalarString(z, "SELECT string_agg(owner, '' ORDER BY id) FROM conf_c4"), "after both commits")
	}
	m["CONC-005"] = func() {
		c := connect()
		defer c.Close()
		limit := c.MaxStreams()
		check(limit > 0 && limit < 4096, "implausible stream cap %d", limit)
		n := limit * 3
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				var x, y any
				err := c.QueryRow(bg, "SELECT pg_sleep(0.05), 1").Scan(&x, &y)
				if err == nil && fmt.Sprint(y) != "1" {
					err = fmt.Errorf("got %v", y)
				}
				if err != nil {
					errs <- err
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			fail("result past the stream cap (%d): %v", limit, err)
		}
	}

	// ── key-value parity ──
	kv := func(fn func(c *particledb.Conn, r *resp)) func() {
		return func() {
			c := connect()
			defer c.Close()
			r := dialResp()
			defer r.close()
			fn(c, r)
		}
	}
	k := func(s string) []byte { return []byte(s) }
	m["KV-001"] = kv(func(c *particledb.Conn, r *resp) {
		must(c.KVSet(bg, k("conf:kv1"), k("hello wörld")), "SDK SET")
		eq("hello wörld", r.str("GET", "conf:kv1"), "RESP GET of SDK SET")
		r.cmd("SET", "conf:kv1b", "from resp")
		v, err := c.KVGet(bg, k("conf:kv1b"))
		must(err, "SDK GET")
		eq("from resp", string(v), "SDK GET of RESP SET")
	})
	m["KV-002"] = kv(func(c *particledb.Conn, r *resp) {
		v, err := c.KVGet(bg, k("conf:kv2-absent"))
		must(err, "SDK GET")
		check(v == nil, "GET of an absent key: %q", v)
		must(c.KVSet(bg, k("conf:kv2"), k("x")), "SDK SET")
		ok, err := c.KVDel(bg, k("conf:kv2"))
		must(err, "SDK DEL")
		check(ok, "DEL of a present key reported not-deleted")
		eq(int64(0), r.num("EXISTS", "conf:kv2"), "RESP EXISTS after SDK DEL")
	})
	m["KV-003"] = kv(func(c *particledb.Conn, r *resp) {
		r.cmd("DEL", "conf:kv3")
		v, err := c.KVIncr(bg, k("conf:kv3"))
		must(err, "SDK INCR")
		eq(int64(1), v, "SDK INCR of absent")
		eq(int64(11), r.num("INCRBY", "conf:kv3", "10"), "RESP INCRBY")
		v, err = c.KVIncr(bg, k("conf:kv3"))
		must(err, "SDK INCR")
		eq(int64(12), v, "SDK INCR")
		eq("12", r.str("GET", "conf:kv3"), "RESP GET")
	})
	m["KV-004"] = kv(func(c *particledb.Conn, r *resp) {
		must(c.KVSet(bg, k("conf:kv4"), k("v")), "SDK SET")
		eq(int64(1), sqlLong(c, "SELECT KV_EXPIRE($1, $2)", "conf:kv4", int64(100)), "KV_EXPIRE")
		ttl := r.num("TTL", "conf:kv4")
		check(ttl >= 99 && ttl <= 100, "RESP TTL after SDK expire 100: %d", ttl)
		r.cmd("SET", "conf:kv4b", "w", "EX", "200")
		t2 := sqlLong(c, "SELECT KV_TTL($1)", "conf:kv4b")
		check(t2 >= 199 && t2 <= 200, "SDK TTL after RESP EX 200: %d", t2)
	})
	m["KV-005"] = kv(func(c *particledb.Conn, r *resp) {
		must(c.KVSet(bg, k("conf:kv5"), k("v")), "SDK SET")
		eq(int64(-1), sqlLong(c, "SELECT KV_TTL($1)", "conf:kv5"), "SDK TTL of a key with none")
		eq(int64(-1), r.num("TTL", "conf:kv5"), "RESP TTL of a key with none")
		eq(int64(-2), sqlLong(c, "SELECT KV_TTL($1)", "conf:kv5-absent"), "SDK TTL of a missing key")
		eq(int64(-2), r.num("TTL", "conf:kv5-absent"), "RESP TTL of a missing key")
	})
	m["KV-006"] = kv(func(c *particledb.Conn, r *resp) {
		r.cmd("SET", "conf:kv6", "a", "EX", "300")
		must(c.KVSet(bg, k("conf:kv6"), k("b")), "SDK SET")
		eq(int64(-1), r.num("TTL", "conf:kv6"), "RESP TTL after SDK plain SET")
		eq("b", r.str("GET", "conf:kv6"), "RESP GET")
	})
	m["KV-007"] = kv(func(c *particledb.Conn, r *resp) {
		must(c.KVSet(bg, k("conf:kv7"), k("v")), "SDK SET")
		sqlLong(c, "SELECT KV_EXPIRE($1, $2)", "conf:kv7", int64(1))
		time.Sleep(2200 * time.Millisecond)
		v, err := c.KVGet(bg, k("conf:kv7"))
		must(err, "SDK GET")
		check(v == nil, "SDK GET of an expired key: %q", v)
		eq(int64(0), r.num("EXISTS", "conf:kv7"), "RESP EXISTS of an expired key")
	})
	m["KV-008"] = kv(func(c *particledb.Conn, r *resp) {
		all := make([]byte, 256)
		for i := range all {
			all[i] = byte(i)
		}
		must(c.KVSet(bg, k("conf:kv8"), all), "SDK SET")
		got, _ := r.cmd("GET", "conf:kv8").([]byte)
		check(bytes.Equal(all, got), "RESP GET returned different bytes")
		r.cmd("SET", k("conf:kv8b"), all)
		v, err := c.KVGet(bg, k("conf:kv8b"))
		must(err, "SDK GET")
		check(bytes.Equal(all, v), "SDK GET of RESP-written binary differs")
	})
	m["KV-009"] = kv(func(c *particledb.Conn, r *resp) {
		big := strings.Repeat("x", 1<<20)
		must(c.KVSet(bg, k("conf:kv9"), k(big)), "SDK SET")
		eq(int64(1048576), r.num("STRLEN", "conf:kv9"), "RESP STRLEN")
		check(r.str("GET", "conf:kv9") == big, "RESP GET of 1 MiB differs")
	})
	m["KV-010"] = kv(func(c *particledb.Conn, r *resp) {
		r.cmd("DEL", "conf:kv10-absent")
		rows, err := c.Query(bg, "SELECT KV_MSET($1, $2, $3, $4)", "conf:kv10a", "1", "conf:kv10b", "2")
		must(err, "KV_MSET")
		rows.Close()
		l, _ := r.cmd("MGET", "conf:kv10a", "conf:kv10b", "conf:kv10-absent").([]any)
		check(len(l) == 3, "RESP MGET returned %v", l)
		eq("1", string(l[0].([]byte)), "RESP MGET[0]")
		eq("2", string(l[1].([]byte)), "RESP MGET[1]")
		check(l[2].([]byte) == nil, "RESP MGET[2] not nil")
		var vs []*string
		must(c.QueryRow(bg, "SELECT KV_MGET($1, $2, $3)", "conf:kv10a", "conf:kv10-absent", "conf:kv10b").Scan(&vs), "KV_MGET")
		got := make([]any, len(vs))
		for i, v := range vs {
			if v != nil {
				got[i] = *v
			}
		}
		eq("[1 <nil> 2]", fmt.Sprint(got), "SDK KV_MGET")
	})
	m["KV-011"] = kv(func(c *particledb.Conn, r *resp) {
		r.cmd("DEL", "conf:h", "conf:l", "conf:s", "conf:z")
		sqlLong(c, "SELECT HASH_SET($1, $2, $3)", "conf:h", "f", "v")
		eq("v", r.str("HGET", "conf:h", "f"), "RESP HGET of SDK HASH_SET")
		sqlLong(c, "SELECT LIST_PUSH($1, $2)", "conf:l", "a")
		sqlLong(c, "SELECT LIST_PUSH($1, $2)", "conf:l", "b")
		lr, _ := r.cmd("LRANGE", "conf:l", "0", "-1").([]any)
		eq(2, len(lr), "RESP LRANGE length")
		sqlLong(c, "SELECT SET_ADD($1, $2)", "conf:s", "m")
		eq(int64(1), r.num("SISMEMBER", "conf:s", "m"), "RESP SISMEMBER of SDK SET_ADD")
		rows, err := c.Query(bg, "SELECT ZSET_ADD($1, $2, $3)", "conf:z", 1.5, "m")
		must(err, "ZSET_ADD")
		rows.Close()
		eq("1.5", r.str("ZSCORE", "conf:z", "m"), "RESP ZSCORE of SDK ZSET_ADD")
	})
	m["KV-012"] = kv(func(c *particledb.Conn, r *resp) {
		r.cmd("DEL", "conf:kv12")
		tx, err := c.Begin(bg)
		must(err, "begin")
		_, err = tx.Exec(bg, "SELECT 1")
		must(err, "open the transaction")
		check(c.InTransaction(), "transaction not open on the server")
		must(c.KVSet(bg, k("conf:kv12"), k("kept")), "SDK SET inside the transaction")
		must(tx.Rollback(bg), "rollback")
		eq("kept", r.str("GET", "conf:kv12"), "KV write after rollback")
	})

	// ── durability ──
	crashRestart := func() {
		control("kill9")
		control("restart")
	}
	m["DUR-001"] = func() {
		c := connect()
		exec(c, "DROP TABLE IF EXISTS conf_dur1")
		exec(c, "CREATE TABLE conf_dur1 (id int8 PRIMARY KEY, v text)")
		for i := 1; i <= 500; i++ {
			exec(c, "INSERT INTO conf_dur1 VALUES ($1, 'v')", i)
		}
		c.Close()
		crashRestart()
		c = connect()
		defer c.Close()
		eq(int64(500), scalarLong(c, "SELECT count(*) FROM conf_dur1"), "acked rows after kill -9")
		eq(int64(125250), scalarLong(c, "SELECT sum(id) FROM conf_dur1"), "acked ids after kill -9")
	}
	m["DUR-002"] = func() {
		c := connect()
		exec(c, "DROP TABLE IF EXISTS conf_dur2")
		exec(c, "CREATE TABLE conf_dur2 (id int8 PRIMARY KEY, v int8)")
		tx, err := c.Begin(bg)
		must(err, "begin")
		_, err = tx.Exec(bg, "INSERT INTO conf_dur2 SELECT g, 1 FROM generate_series(1, 100) g")
		must(err, "insert")
		_, err = tx.Exec(bg, "UPDATE conf_dur2 SET v = 101 WHERE id = 7")
		must(err, "update")
		must(tx.Commit(bg), "commit")
		c.Close()
		crashRestart()
		c = connect()
		defer c.Close()
		eq(int64(100), scalarLong(c, "SELECT count(*) FROM conf_dur2"), "rows of an acked COMMIT after kill -9")
		eq(int64(200), scalarLong(c, "SELECT sum(v) FROM conf_dur2"), "the COMMIT's UPDATE after kill -9")
	}
	m["DUR-003"] = func() {
		c := connect()
		exec(c, "DROP TABLE IF EXISTS conf_dur3")
		exec(c, "CREATE TABLE conf_dur3 (id int8 PRIMARY KEY)")
		b := &particledb.Batch{}
		for i := 1; i <= 1000; i++ {
			b.Queue("INSERT INTO conf_dur3 VALUES ($1)", i)
		}
		must(c.SendBatch(bg, b).Close(), "batch")
		c.Close()
		crashRestart()
		c = connect()
		defer c.Close()
		eq(int64(1000), scalarLong(c, "SELECT count(*) FROM conf_dur3"), "acked batch rows after kill -9")
	}
	m["DUR-004"] = func() {
		c := connect()
		must(c.KVSet(bg, k("conf:dur4"), k("durable")), "SDK SET")
		_, err := c.KVDel(bg, k("conf:dur4n"))
		must(err, "SDK DEL")
		for i := 0; i < 3; i++ {
			_, err := c.KVIncr(bg, k("conf:dur4n"))
			must(err, "SDK INCR")
		}
		c.Close()
		crashRestart()
		c = connect()
		defer c.Close()
		v, err := c.KVGet(bg, k("conf:dur4"))
		must(err, "SDK GET")
		eq("durable", string(v), "SDK GET after kill -9")
		r := dialResp()
		defer r.close()
		eq("3", r.str("GET", "conf:dur4n"), "RESP GET of the counter after kill -9")
	}
	m["DUR-005"] = func() {
		a := connect()
		pool, err := particledb.NewPool(bg, dsn())
		must(err, "pool")
		defer pool.Close()
		must(pool.Ping(bg), "pool ping")
		func() {
			defer func() {
				a.Close()
				if p := recover(); p != nil {
					control("restart")
					panic(p)
				}
			}()
			eq(int64(1), scalarLong(a, "SELECT 1"), "before the kill")
			control("kill9")
			t0 := time.Now()
			var x int64
			err := a.QueryRow(bg, "SELECT 1").Scan(&x)
			check(err != nil, "statement on a connection to a killed server succeeded")
			st := particledb.SQLState(err)
			check(strings.HasPrefix(st, "08"), "connection-loss SQLSTATE %s is not class 08 (%v)", st, err)
			check(errors.Is(err, particledb.ErrConnection), "error does not match ErrConnection")
			check(time.Since(t0) < 10*time.Second, "connection loss took > 10 s to surface")
		}()
		control("restart")
		b := connect()
		defer b.Close()
		eq(int64(1), scalarLong(b, "SELECT 1"), "new connection after restart")
		var x int64
		must(pool.QueryRow(bg, "SELECT 1").Scan(&x), "fresh pool borrow after restart")
		eq(int64(1), x, "pool after restart")
	}
	return m
}
