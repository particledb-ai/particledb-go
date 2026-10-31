package legacy

// Live-server test of the DEFAULT transport (PDB native wire) through the
// high-level Client: query, bound parameters (PREPARE/EXECUTE), a 10,000-row
// EXECUTE_BATCH insert checked by count + sums + a spot value, typed errors,
// transactions, and the KV helpers.
//
// Runs only when PDB_NATIVE_PORT is set (host 127.0.0.1, --no-auth server);
// otherwise it is skipped. Prints "NCL_PASS go" when every case passed.
//
//	PDB_NATIVE_PORT=5440 go test -count=1 -v -run TestNativeClientLive .

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNativeClientLive(t *testing.T) {
	portStr := os.Getenv("PDB_NATIVE_PORT")
	if portStr == "" {
		t.Skip("PDB_NATIVE_PORT not set")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("PDB_NATIVE_PORT=%q: %v", portStr, err)
	}
	cfg := DefaultConfig() // no Protocol, no lib/pq: the native wire
	cfg.Host = "127.0.0.1"
	cfg.Port = port
	cfg.PoolSize = 4
	db, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.Protocol() != ProtocolNative {
		t.Fatalf("default protocol = %q", db.Protocol())
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	const tbl = "ncl_go"
	mustExec := func(t *testing.T, sql string, args ...interface{}) int64 {
		t.Helper()
		n, err := db.Execute(sql, args...)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return n
	}
	_, _ = db.Execute("DROP TABLE IF EXISTS " + tbl)
	mustExec(t, "CREATE TABLE "+tbl+" (id BIGINT PRIMARY KEY, name TEXT, score DOUBLE PRECISION, ok BOOLEAN, blob BYTEA, ts TIMESTAMP)")
	defer db.Execute("DROP TABLE IF EXISTS " + tbl)

	passed, total := 0, 0
	run := func(name string, fn func(t *testing.T)) {
		total++
		if t.Run(name, fn) {
			passed++
		}
	}

	run("query_types", func(t *testing.T) {
		res, err := db.Query("SELECT 1 AS a, 'x' AS b, 2.5 AS c, true AS d, NULL AS e")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(res.Columns, ",") != "a,b,c,d,e" {
			t.Errorf("columns = %v", res.Columns)
		}
		r := res.Rows[0]
		if r["a"] != int64(1) || r["b"] != "x" || r["d"] != true || r["e"] != nil {
			t.Errorf("row = %#v", r)
		}
		if f, ok := r["c"].(float64); !ok || f != 2.5 {
			if s, ok := r["c"].(string); !ok || s != "2.5" {
				t.Errorf("c = %#v, want 2.5", r["c"])
			}
		}
	})

	run("params_roundtrip", func(t *testing.T) {
		hostile := "o'brien\"; DROP TABLE " + tbl + "; --"
		ts := time.Date(2026, 10, 12, 1, 2, 3, 456789000, time.UTC)
		n := mustExec(t, "INSERT INTO "+tbl+" (id, name, score, ok, blob, ts) VALUES ($1, $2, $3, $4, $5, $6)",
			-1, hostile, 0.125, false, []byte{0, 1, 255}, ts)
		if n != 1 {
			t.Errorf("affected = %d, want 1", n)
		}
		mustExec(t, "INSERT INTO "+tbl+" (id, name, score, ok, blob, ts) VALUES ($1, $2, $3, $4, $5, $6)",
			-2, nil, nil, nil, nil, nil)
		row, err := db.QueryOne("SELECT name, score, ok, blob, ts FROM "+tbl+" WHERE id = $1", -1)
		if err != nil {
			t.Fatal(err)
		}
		if row["name"] != hostile || row["score"] != 0.125 || row["ok"] != false {
			t.Errorf("row = %#v", row)
		}
		if b, _ := row["blob"].([]byte); !bytes.Equal(b, []byte{0, 1, 255}) {
			t.Errorf("blob = %#v", row["blob"])
		}
		if tv, ok := row["ts"].(time.Time); !ok || !tv.Equal(ts) {
			t.Errorf("ts = %#v, want %v", row["ts"], ts)
		}
		row, err = db.QueryOne("SELECT name, score, ok, blob, ts FROM "+tbl+" WHERE id = $1", -2)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range row {
			if v != nil {
				t.Errorf("NULL param column %s read back as %#v", k, v)
			}
		}
		mustExec(t, "DELETE FROM "+tbl+" WHERE id < 0")
	})

	const N = 10000
	run("execute_batch_10k", func(t *testing.T) {
		rows := make([][]interface{}, N)
		for i := 0; i < N; i++ {
			rows[i] = []interface{}{i + 1, fmt.Sprintf("name-%d", i+1), float64(i+1) / 2, (i+1)%2 == 0}
		}
		res, err := db.ExecuteBatch("INSERT INTO "+tbl+" (id, name, score, ok) VALUES ($1, $2, $3, $4)", rows)
		if err != nil {
			t.Fatalf("ExecuteBatch: %v", err)
		}
		if res.Attempted != N || res.TotalAffected() != N || len(res.Errors) != 0 {
			t.Fatalf("result: attempted=%d affected=%d errors=%d", res.Attempted, res.TotalAffected(), len(res.Errors))
		}
		row, err := db.QueryOne("SELECT count(*) AS n, sum(id) AS s, sum(score) AS sc FROM " + tbl)
		if err != nil {
			t.Fatal(err)
		}
		// sum(bigint) is NUMERIC (decoded as a decimal string), as in PostgreSQL.
		if row["n"] != int64(N) || fmt.Sprint(row["s"]) != strconv.Itoa(N*(N+1)/2) {
			t.Fatalf("count/sum = %#v/%#v, want %d/%d", row["n"], row["s"], N, N*(N+1)/2)
		}
		if sc, ok := row["sc"].(float64); !ok || sc != float64(N*(N+1)/2)/2 {
			t.Fatalf("sum(score) = %#v, want %v", row["sc"], float64(N*(N+1)/2)/2)
		}
		row, err = db.QueryOne("SELECT name, ok FROM "+tbl+" WHERE id = $1", 4321)
		if err != nil || row["name"] != "name-4321" || row["ok"] != false {
			t.Fatalf("spot check id 4321: %#v %v", row, err)
		}
	})

	run("bad_sql_typed_error", func(t *testing.T) {
		_, err := db.Query("SELEC 1")
		var qe *PdbQueryError
		if err == nil || !errors.As(err, &qe) {
			t.Fatalf("bad SQL: %T %v, want *PdbQueryError", err, err)
		}
		_, err = db.Query("SELECT * FROM no_such_table_ncl WHERE id = $1", 1)
		if err == nil || !errors.As(err, &qe) {
			t.Fatalf("bad prepared SQL: %T %v, want *PdbQueryError", err, err)
		}
		if _, err := db.Native().Prepare(context.Background(), "SELEC $1"); err == nil {
			t.Fatal("Prepare of bad SQL succeeded")
		}
	})

	run("batch_constraint_violation_autocommit", func(t *testing.T) {
		rows := [][]interface{}{{N + 1, "a"}, {1, "dup"}, {N + 2, "b"}, {2, "dup2"}, {N + 3, "c"}}
		res, err := db.ExecuteBatch("INSERT INTO "+tbl+" (id, name) VALUES ($1, $2)", rows)
		var be *PdbBatchError
		if !errors.As(err, &be) {
			t.Fatalf("err = %T %v, want *PdbBatchError", err, err)
		}
		var qe *PdbQueryError
		if !errors.As(err, &qe) || !strings.Contains(qe.Message, "duplicate key") {
			t.Fatalf("batch error does not carry the duplicate-key message: %v", err)
		}
		if res.Attempted != 5 || len(res.Errors) != 2 || res.Errors[0].Row != 1 || res.Errors[1].Row != 3 {
			t.Fatalf("result = %+v", res)
		}
		row, _ := db.QueryOne("SELECT count(*) AS n FROM " + tbl)
		if row["n"] != int64(N+3) {
			t.Fatalf("count = %v, want %d (independent rows commit)", row["n"], N+3)
		}
	})

	run("batch_in_transaction_rolls_back", func(t *testing.T) {
		before, _ := db.QueryOne("SELECT count(*) AS n FROM " + tbl)
		err := db.Transaction(func(tx *Tx) error {
			_, err := tx.ExecuteBatch("INSERT INTO "+tbl+" (id, name) VALUES ($1, $2)",
				[][]interface{}{{N + 10, "x"}, {1, "dup"}, {N + 11, "y"}})
			return err
		})
		var be *PdbBatchError
		if !errors.As(err, &be) {
			t.Fatalf("err = %T %v, want *PdbBatchError", err, err)
		}
		if be.Result.Attempted != 2 {
			t.Errorf("attempted = %d, want 2 (server stops at the failed row in a txn)", be.Result.Attempted)
		}
		after, _ := db.QueryOne("SELECT count(*) AS n FROM " + tbl)
		if after["n"] != before["n"] {
			t.Fatalf("count %v -> %v: the failed transaction leaked rows", before["n"], after["n"])
		}
	})

	run("transaction_commit", func(t *testing.T) {
		err := db.Transaction(func(tx *Tx) error {
			if _, err := tx.Execute("INSERT INTO "+tbl+" (id, name) VALUES ($1, $2)", N+20, "t"); err != nil {
				return err
			}
			row, err := tx.QueryOne("SELECT name FROM "+tbl+" WHERE id = $1", N+20)
			if err != nil || row == nil || row["name"] != "t" {
				return fmt.Errorf("own write not visible inside the txn: %v %v", row, err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		row, err := db.QueryOne("SELECT name FROM "+tbl+" WHERE id = $1", N+20)
		if err != nil || row == nil || row["name"] != "t" {
			t.Fatalf("committed row: %v %v", row, err)
		}
	})

	run("prepared_stmt_handle", func(t *testing.T) {
		ctx := context.Background()
		st, err := db.Native().Prepare(ctx, "SELECT name FROM "+tbl+" WHERE id = $1")
		if err != nil {
			t.Fatal(err)
		}
		if st.NumParams() != 1 {
			t.Errorf("NumParams = %d", st.NumParams())
		}
		rows, err := st.Query(ctx, 77)
		if err != nil || len(rows) != 1 || rows[0]["name"] != "name-77" {
			t.Fatalf("stmt query: %v %v", rows, err)
		}
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Query(ctx, 77); err == nil {
			t.Fatal("query on a closed statement succeeded")
		}
	})

	run("kv_helpers", func(t *testing.T) {
		kv := db.KV()
		if err := kv.Set("ncl:k", "v1"); err != nil {
			t.Fatal(err)
		}
		v, err := kv.Get("ncl:k")
		if err != nil || v != "v1" {
			t.Fatalf("Get = %q %v", v, err)
		}
		n, err := kv.Incr("ncl:ctr", 5)
		if err != nil || n != 5 {
			t.Fatalf("Incr = %d %v", n, err)
		}
		// MGet: one value per key, a missing key is nil, and a value
		// containing a comma survives (KV_MGET's joined string would not).
		if err := kv.Set("ncl:c", "p,q"); err != nil {
			t.Fatal(err)
		}
		m, err := kv.MGet([]string{"ncl:k", "ncl:missing", "ncl:c"})
		if err != nil {
			t.Fatalf("MGet: %v", err)
		}
		if m["ncl:k"] == nil || *m["ncl:k"] != "v1" || m["ncl:missing"] != nil ||
			m["ncl:c"] == nil || *m["ncl:c"] != "p,q" {
			t.Fatalf("MGet = %v", m)
		}
		_ = kv.Delete("ncl:k")
		_ = kv.Delete("ncl:ctr")
		_ = kv.Delete("ncl:c")
	})

	if passed == total {
		fmt.Println("NCL_PASS go")
	}
}
