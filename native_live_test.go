package particledb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLiveHandshakeDefaults(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	if got := c.NegotiatedProtocol(); got != "pdb-native-v2" {
		t.Fatalf("protocol %q", got)
	}
	var proto, caps string
	if err := c.QueryRow(ctx, "SHOW pdb.wire_protocol").Scan(&proto); err != nil {
		t.Fatal(err)
	}
	if err := c.QueryRow(ctx, "SHOW pdb.wire_capabilities").Scan(&caps); err != nil {
		t.Fatal(err)
	}
	if proto != c.NegotiatedProtocol() || caps != strings.Join(c.NegotiatedCapabilities(), ",") {
		t.Fatalf("server %q %q, client %q %v", proto, caps, c.NegotiatedProtocol(), c.NegotiatedCapabilities())
	}
	for _, want := range []string{"v2_mux", "prepared", "typed_params", "array_params", "batch_sqlstate", "statement_notice", "numeric_text", "bulk_ingest"} {
		if !strings.Contains(","+caps+",", ","+want+",") {
			t.Errorf("capability %s missing from %q", want, caps)
		}
	}
}

func TestLiveScalarTypes(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	var (
		i2, i4, i8 int64
		f4         float32
		f8         float64
		b          bool
		s          string
		ns         *string
		num        Decimal
		num2       string
		d, ts, tz  time.Time
		u          UUID
		by         []byte
		js         json.RawMessage
		jb         map[string]any
	)
	err := c.QueryRow(ctx, `SELECT (-32768)::int2, 2147483647::int4, (-9223372036854775808)::int8, 1.5::float4, 0.1::float8,
		true, 'héllo ✓ 𝄞', NULL::text, 1.50::numeric(10,2), '0.52'::numeric, '2024-02-29'::date,
		'1999-12-31 23:59:59.999999'::timestamp, '2024-03-10 10:00:00+00'::timestamptz,
		'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, '\xdeadbeef'::bytea, '{"b":1,  "a":[1,2]}'::json, '{"b":1,"a":[1,2]}'::jsonb`).
		Scan(&i2, &i4, &i8, &f4, &f8, &b, &s, &ns, &num, &num2, &d, &ts, &tz, &u, &by, &js, &jb)
	if err != nil {
		t.Fatal(err)
	}
	if i2 != -32768 || i4 != 2147483647 || i8 != math.MinInt64 || f4 != 1.5 || f8 != 0.1 || !b || s != "héllo ✓ 𝄞" || ns != nil {
		t.Fatalf("scalars %v %v %v %v %v %v %q %v", i2, i4, i8, f4, f8, b, s, ns)
	}
	if num.String() != "1.50" || num2 != "0.52" {
		t.Fatalf("numeric %s %s", num, num2)
	}
	if d.Format("2006-01-02") != "2024-02-29" || ts.Format("2006-01-02 15:04:05.999999") != "1999-12-31 23:59:59.999999" {
		t.Fatalf("date/ts %v %v", d, ts)
	}
	if !tz.Equal(time.Date(2024, 3, 10, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("timestamptz %v", tz)
	}
	if u.String() != "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11" || fmt.Sprintf("%x", by) != "deadbeef" {
		t.Fatalf("uuid/bytea %v %x", u, by)
	}
	if string(js) != `{"b":1,  "a":[1,2]}` || jb["b"].(float64) != 1 {
		t.Fatalf("json %s %v", js, jb)
	}
}

func TestLiveParamsRoundTrip(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY, a int2, b int4, c float4, d float8, e numeric, f bool, g text, h bytea, i date, j timestamp, k timestamptz, l uuid, m jsonb, n int8[], o text[])")
	u, _ := ParseUUID("00000000-0000-0000-0000-000000000001")
	k := time.Date(2001, 9, 9, 1, 46, 40, 0, time.UTC)
	hostile := `it's "quoted" \ back'); DROP TABLE x; --`
	_, err := c.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
		1, int16(-7), int32(123456), float32(2.5), math.Inf(-1), MustParseDecimal("3.14159265358979323846"), true, hostile,
		[]byte{0, 0xff, 0x10}, Date{time.Date(2001, 9, 9, 0, 0, 0, 0, time.UTC)}, Timestamp{time.Date(2001, 9, 9, 1, 46, 40, 0, time.UTC)},
		k, u, json.RawMessage(`{"k": "v", "n": null}`), []int64{7, -9}, []*string{ptr("a"), nil, ptr("c,d")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
		2, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := c.Query(ctx, "SELECT * FROM "+tbl+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	got, err := CollectRows(rows, RowToMap)
	if err != nil {
		t.Fatal(err)
	}
	r := got[0]
	if r["a"].(int64) != -7 || r["b"].(int64) != 123456 || r["c"].(float32) != 2.5 || !math.IsInf(r["d"].(float64), -1) {
		t.Fatalf("row %v", r)
	}
	if r["e"].(Decimal).String() != "3.14159265358979323846" || r["g"].(string) != hostile || !reflect.DeepEqual(r["h"], []byte{0, 0xff, 0x10}) {
		t.Fatalf("row %v", r)
	}
	if !r["k"].(time.Time).Equal(k) || r["l"].(UUID) != u || string(r["m"].(json.RawMessage)) != `{"k": "v", "n": null}` {
		t.Fatalf("row %v", r)
	}
	if !reflect.DeepEqual(r["n"], []any{int64(7), int64(-9)}) || !reflect.DeepEqual(r["o"], []any{"a", nil, "c,d"}) {
		t.Fatalf("arrays %#v %#v", r["n"], r["o"])
	}
	for k, v := range got[1] {
		if k != "id" && v != nil {
			t.Fatalf("row 2 %s = %#v, want NULL", k, v)
		}
	}
	var ns []*string
	if err := c.QueryRow(ctx, "SELECT o FROM "+tbl+" WHERE id = 1").Scan(&ns); err != nil || len(ns) != 3 || ns[1] != nil || *ns[2] != "c,d" {
		t.Fatalf("scan array %v %v", ns, err)
	}
}

func ptr[T any](v T) *T { return &v }

func TestLiveErrorsAndSentinels(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO "+tbl+" VALUES (1)")
	_, err := c.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1)", 1)
	var pe *Error
	if !errors.As(err, &pe) || pe.SQLState != "23505" || !errors.Is(err, ErrUniqueViolation) || !errors.Is(err, ErrIntegrityViolation) {
		t.Fatalf("got %v", err)
	}
	for sql, state := range map[string]string{
		"SELECT * FROM no_such_table_x": "42P01", "SELEC 1": "42601", "SELECT 1/0": "22012",
		"SELECT 'abc'::int4": "22P02", "SELECT no_such_fn_x(1)": "42883",
	} {
		_, err := c.Exec(ctx, sql)
		if SQLState(err) != state {
			t.Errorf("%s: %v, want %s", sql, err, state)
		}
	}
}

func TestLiveTransactions(t *testing.T) {
	c := liveConn(t)
	c2 := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY)")
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO "+tbl+" VALUES (1)")
	var n int
	c2.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&n)
	if n != 0 {
		t.Fatalf("uncommitted row visible: %d", n)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	c2.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&n)
	if n != 1 {
		t.Fatalf("committed row not visible: %d", n)
	}
	// rollback, savepoint
	tx, _ = c.Begin(ctx)
	mustExec(t, tx, "INSERT INTO "+tbl+" VALUES (2)")
	if err := tx.Savepoint(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO "+tbl+" VALUES (1)"); !errors.Is(err, ErrUniqueViolation) {
		t.Fatal(err)
	}
	if err := tx.RollbackTo(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO "+tbl+" VALUES (3)")
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	c2.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&n)
	if n != 1 {
		t.Fatalf("after rollback %d", n)
	}
	// failed transaction: commit reports the rollback
	tx, _ = c.Begin(ctx)
	_, err = tx.Exec(ctx, "SELECT 1/0")
	if SQLState(err) != "22012" {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "SELECT 1")
	if !errors.Is(err, ErrInFailedTransaction) {
		t.Fatalf("got %v", err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, ErrTxCommitRollback) {
		t.Fatalf("commit of a failed tx: %v", err)
	}
	// isolation level reaches the server
	for _, lvl := range []TxIsoLevel{Serializable, RepeatableRead, ReadCommitted} {
		tx, err := c.BeginTx(ctx, TxOptions{IsoLevel: lvl})
		if err != nil {
			t.Fatal(err)
		}
		var got string
		tx.QueryRow(ctx, "SHOW transaction_isolation").Scan(&got)
		tx.Commit(ctx)
		if !strings.EqualFold(got, string(lvl)) {
			t.Fatalf("level %s reported %s", lvl, got)
		}
	}
	// read only
	tx, _ = c.BeginTx(ctx, TxOptions{AccessMode: ReadOnly})
	_, err = tx.Exec(ctx, "INSERT INTO "+tbl+" VALUES (9)")
	tx.Rollback(ctx)
	if SQLState(err) != "25006" {
		t.Fatalf("read-only write: %v", err)
	}
}

func TestLiveBatch(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY, v text)")
	b := &Batch{}
	for i := 1; i <= 1000; i++ {
		b.Queue("INSERT INTO "+tbl+" VALUES ($1, $2)", i, fmt.Sprintf("v'%d", i))
	}
	b.Queue("SELECT count(*), sum(id) FROM " + tbl)
	br := c.SendBatch(ctx, b)
	for i := 0; i < 1000; i++ {
		tag, err := br.Exec()
		if err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("entry %d: %v %v", i, tag, err)
		}
	}
	var cnt, sum int64
	if err := br.QueryRow().Scan(&cnt, &sum); err != nil || cnt != 1000 || sum != 500500 {
		t.Fatalf("%d %d %v", cnt, sum, err)
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
	// atomic: a failing entry leaves nothing
	b = &Batch{}
	b.Queue("INSERT INTO "+tbl+" VALUES ($1, 'x')", 5000)
	b.Queue("INSERT INTO "+tbl+" VALUES ($1, 'x')", 1)
	b.Queue("INSERT INTO "+tbl+" VALUES ($1, 'x')", 5001)
	br = c.SendBatch(ctx, b)
	err := br.Close()
	var pe *Error
	if !errors.As(err, &pe) || pe.SQLState != "23505" || pe.BatchIndex != 1 {
		t.Fatalf("batch error %v", err)
	}
	var n int
	c.QueryRow(ctx, "SELECT count(*) FROM "+tbl+" WHERE id >= 5000").Scan(&n)
	if n != 0 {
		t.Fatalf("non-atomic batch: %d rows", n)
	}
}

func TestLiveCopyFrom(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY, v text, f float8, b bool, n numeric(10,2), ts timestamptz, by bytea)")
	ts := time.Date(2024, 1, 2, 3, 4, 5, 600000000, time.UTC)
	n, err := c.CopyFrom(ctx, Identifier{tbl}, []string{"id", "v", "f", "b", "n", "ts", "by"}, CopyFromSlice(200000, func(i int) ([]any, error) {
		if i%10 == 0 {
			return []any{i, nil, nil, nil, nil, nil, nil}, nil
		}
		return []any{i, fmt.Sprintf("row-%d", i), float64(i) / 2, i%2 == 0, MustParseDecimal("1.50"), ts, []byte{byte(i)}}, nil
	}))
	if err != nil || n != 200000 {
		t.Fatalf("copy %d %v", n, err)
	}
	var cnt, nulls int64
	var sumF float64
	var v string
	var num Decimal
	var gotTS time.Time
	var by []byte
	if err := c.QueryRow(ctx, "SELECT count(*), count(*) FILTER (WHERE v IS NULL), sum(f) FROM "+tbl).Scan(&cnt, &nulls, &sumF); err != nil {
		t.Fatal(err)
	}
	if cnt != 200000 || nulls != 20000 {
		t.Fatalf("%d %d", cnt, nulls)
	}
	if err := c.QueryRow(ctx, "SELECT v, n, ts, by FROM "+tbl+" WHERE id = 7").Scan(&v, &num, &gotTS, &by); err != nil {
		t.Fatal(err)
	}
	if v != "row-7" || num.String() != "1.50" || !gotTS.Equal(ts) || len(by) != 1 || by[0] != 7 {
		t.Fatalf("%q %s %v %v", v, num, gotTS, by)
	}
	// atomic: a duplicate key loads nothing
	_, err = c.CopyFrom(ctx, Identifier{tbl}, []string{"id"}, CopyFromRows([][]any{{900001}, {1}}))
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("duplicate copy: %v", err)
	}
	c.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&cnt)
	if cnt != 200000 {
		t.Fatalf("partial load: %d", cnt)
	}
}

func TestLiveCancel(t *testing.T) {
	c := liveConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	t0 := time.Now()
	_, err := c.Exec(ctx, "SELECT pg_sleep(30)")
	if !errors.Is(err, ErrQueryCanceled) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if time.Since(t0) > 5*time.Second {
		t.Fatalf("cancel took %v", time.Since(t0))
	}
	var one int
	if err := c.QueryRow(context.Background(), "SELECT 1").Scan(&one); err != nil || one != 1 {
		t.Fatalf("connection unusable after cancel: %v", err)
	}
	// server-side statement_timeout
	mustExec(t, c, "SET statement_timeout = '500ms'")
	_, err = c.Exec(context.Background(), "SELECT pg_sleep(30)")
	if !errors.Is(err, ErrQueryCanceled) {
		t.Fatalf("statement_timeout: %v", err)
	}
	mustExec(t, c, "RESET statement_timeout")
}

func TestLiveStreamingAndConcurrency(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, sign := range []int64{1, -1} {
		wg.Add(1)
		go func(sign int64) {
			defer wg.Done()
			rows, err := c.Query(ctx, fmt.Sprintf("SELECT %d * g FROM generate_series(1, 200000) g", sign))
			if err != nil {
				errs <- err
				return
			}
			var n, sum, v int64
			for rows.Next() {
				if err := rows.Scan(&v); err != nil {
					errs <- err
					return
				}
				n++
				sum += v
				if v != sign*n {
					errs <- fmt.Errorf("out of order: %d at %d", v, n)
					return
				}
			}
			if rows.Err() != nil || n != 200000 || sum != sign*20000100000 {
				errs <- fmt.Errorf("n=%d sum=%d err=%v", n, sum, rows.Err())
			}
		}(sign)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// abandon a stream early, then keep using the connection
	rows, err := c.Query(ctx, "SELECT g, repeat('x', 100) FROM generate_series(1, 300000) g")
	if err != nil {
		t.Fatal(err)
	}
	rows.Next()
	rows.Close()
	var one int
	if err := c.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatal(err)
	}
	// more goroutines than the stream cap
	var wg2 sync.WaitGroup
	fail := make(chan error, 200)
	for i := 0; i < c.MaxStreams()*3; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			var x int
			if err := c.QueryRow(ctx, "SELECT 1 FROM (SELECT pg_sleep(0.05)) s").Scan(&x); err != nil {
				fail <- err
			}
		}()
	}
	wg2.Wait()
	close(fail)
	for err := range fail {
		t.Fatal(err)
	}
}

func TestLiveKV(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	if err := c.KVSet(ctx, []byte("gokv:bin"), all); err != nil {
		t.Fatal(err)
	}
	got, err := c.KVGet(ctx, []byte("gokv:bin"))
	if err != nil || !reflect.DeepEqual(got, all) {
		t.Fatalf("%v %v", got, err)
	}
	if v, err := c.KVGet(ctx, []byte("gokv:absent")); v != nil || err != nil {
		t.Fatalf("%v %v", v, err)
	}
	c.KVDel(ctx, []byte("gokv:n"))
	for i := int64(1); i <= 3; i++ {
		if v, err := c.KVIncr(ctx, []byte("gokv:n")); v != i || err != nil {
			t.Fatalf("%d %v", v, err)
		}
	}
	if ok, err := c.KVDel(ctx, []byte("gokv:n")); !ok || err != nil {
		t.Fatalf("%v %v", ok, err)
	}
}

func TestLiveKVSQLAndVector(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	if err := c.KVSet(ctx, []byte("gokv:ttl"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if ttl, err := KVTTL(ctx, c, "gokv:ttl"); err != nil || ttl != -1 {
		t.Fatalf("ttl %d %v", ttl, err)
	}
	if ok, err := KVExpire(ctx, c, "gokv:ttl", 100); err != nil || !ok {
		t.Fatalf("expire %v %v", ok, err)
	}
	if ttl, _ := KVTTL(ctx, c, "gokv:ttl"); ttl < 99 || ttl > 100 {
		t.Fatalf("ttl after expire %d", ttl)
	}
	if ttl, _ := KVTTL(ctx, c, "gokv:absent-x"); ttl != -2 {
		t.Fatalf("ttl absent %d", ttl)
	}
	if err := KVMSet(ctx, c, map[string]string{"gokv:a": "1", "gokv:b": "2"}); err != nil {
		t.Fatal(err)
	}
	vs, err := KVMGet(ctx, c, "gokv:a", "gokv:absent-x", "gokv:b")
	if err != nil || len(vs) != 3 || *vs[0] != "1" || vs[1] != nil || *vs[2] != "2" {
		t.Fatalf("mget %v %v", vs, err)
	}
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY, tenant int8, v vector(3))")
	mustExec(t, c, "INSERT INTO "+tbl+" VALUES ($1, 1, $2), ($3, 1, $4), ($5, 2, $6)", 1, Vector{1, 2, 3}, 2, Vector{0.5, -1, 0}, 3, Vector{1, 2, 3.5})
	rows, err := NearestNeighbors(ctx, c, Identifier{tbl}, "v", Vector{1, 2, 3}, 2, &NearestOptions{Columns: []string{"id", "v"}, Where: "tenant = $2", WhereArgs: []any{1}})
	if err != nil {
		t.Fatal(err)
	}
	type hit struct {
		ID       int64
		V        Vector
		Distance float64
	}
	hits, err := CollectRows(rows, RowToStructByName[hit])
	if err != nil || len(hits) != 2 || hits[0].ID != 1 || hits[0].Distance != 0 || !reflect.DeepEqual(hits[1].V, Vector{0.5, -1, 0}) {
		t.Fatalf("%+v %v", hits, err)
	}
}

// TestLiveBulkBatchEqualsStatementBatch: an INSERT-shaped batch takes the
// bulk ingest path; the rows it stores equal the statement path's.
func TestLiveBulkBatchEqualsStatementBatch(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	ddl := " (id int8 PRIMARY KEY, n numeric, f float8, ts timestamp, tz timestamptz, t text, b bool, i4 int4, j jsonb, u uuid, by bytea)"
	for _, s := range []string{"a", "b"} {
		mustExec(t, c, "DROP TABLE IF EXISTS "+tbl+s)
		mustExec(t, c, "CREATE TABLE "+tbl+s+ddl)
	}
	u, _ := ParseUUID("123e4567-e89b-12d3-a456-426614174000")
	at := time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.FixedZone("x", 2*3600))
	row := func(i int) []any {
		if i%7 == 0 {
			return []any{i, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}
		}
		return []any{i, MustParseDecimal(fmt.Sprintf("%d.25", i)), 0.1 * float64(i), Timestamp{at}, at, fmt.Sprintf("it's %d", i), i%2 == 0, int32(i), json.RawMessage(`{"k": 1}`), u, []byte{byte(i), 0}}
	}
	bulk := &Batch{}
	for i := 1; i <= 200; i++ {
		bulk.Queue("INSERT INTO "+tbl+"a VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", row(i)...)
	}
	if !bulk.bulkShaped() {
		t.Fatal("batch is not bulk shaped")
	}
	br := c.SendBatch(ctx, bulk)
	for i := 0; i < 200; i++ {
		if tag, err := br.Exec(); err != nil || tag.RowsAffected() != 1 {
			t.Fatalf("bulk entry %d: %v %v", i, tag, err)
		}
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}
	// the same rows statement by statement (a leading no-op defeats the bulk shape)
	stmt := &Batch{}
	stmt.Queue("SELECT 1")
	for i := 1; i <= 200; i++ {
		stmt.Queue("INSERT INTO "+tbl+"b VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", row(i)...)
	}
	if err := c.SendBatch(ctx, stmt).Close(); err != nil {
		t.Fatal(err)
	}
	read := func(s string) []map[string]any {
		rows, err := c.Query(ctx, "SELECT id, n::text AS n, f::text AS f, ts::text AS ts, tz, t, b, i4, j::text AS j, u::text AS u, by FROM "+tbl+s+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		out, err := CollectRows(rows, RowToMap)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	a, b := read("a"), read("b")
	if len(a) != 200 || !reflect.DeepEqual(a, b) {
		for i := range a {
			if !reflect.DeepEqual(a[i], b[i]) {
				t.Fatalf("row %d differs:\nbulk %v\nstmt %v", i, a[i], b[i])
			}
		}
		t.Fatalf("len %d %d", len(a), len(b))
	}
	// atomic failure
	bad := &Batch{}
	for i := 1000; i < 1100; i++ {
		bad.Queue("INSERT INTO "+tbl+"a (id) VALUES ($1)", i)
	}
	bad.Queue("INSERT INTO "+tbl+"a (id) VALUES ($1)", 5)
	if err := c.SendBatch(ctx, bad).Close(); !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("bulk duplicate: %v", err)
	}
	var n int
	c.QueryRow(ctx, "SELECT count(*) FROM "+tbl+"a WHERE id >= 1000").Scan(&n)
	if n != 0 {
		t.Fatalf("failed bulk batch left %d rows", n)
	}
}

func TestLiveStatementCache(t *testing.T) {
	ctx := context.Background()
	dsn := liveDSN(t)
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	c, err := Connect(ctx, dsn+sep+"statement_cache_capacity=2")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	// more distinct statements than the cache holds, interleaved
	for round := 0; round < 3; round++ {
		for k := 0; k < 5; k++ {
			var got int64
			if err := c.QueryRow(ctx, fmt.Sprintf("SELECT $1::int8 + %d", k), int64(round)).Scan(&got); err != nil || got != int64(round+k) {
				t.Fatalf("round %d k %d: %d %v", round, k, got, err)
			}
		}
	}
	c.stmtMu.Lock()
	n := c.lru.Len()
	c.stmtMu.Unlock()
	if n > 2 {
		t.Fatalf("cache holds %d statements", n)
	}
	var x int64
	// a cache-less connection prepares and closes per statement
	c2, err := Connect(ctx, dsn+sep+"statement_cache_capacity=-1")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	for i := 0; i < 5; i++ {
		if err := c2.QueryRow(ctx, "SELECT $1::int8", int64(i)).Scan(&x); err != nil || x != int64(i) {
			t.Fatalf("%d %v", x, err)
		}
	}
	// an explicit statement
	st, err := c.Prepare(ctx, "SELECT $1::text || $2::text")
	if err != nil {
		t.Fatal(err)
	}
	var s string
	if err := st.QueryRow(ctx, "a", "b").Scan(&s); err != nil || s != "ab" || !st.IsServerPrepared() || st.NumParams() != 2 {
		t.Fatalf("%q %v", s, err)
	}
	if _, err := st.Exec(ctx, "only one"); SQLState(err) != "08P01" {
		t.Fatalf("wrong arity: %v", err)
	}
	st.Close(ctx)
	if _, err := st.Query(ctx, "a", "b"); SQLState(err) != "26000" {
		t.Fatalf("closed statement: %v", err)
	}
}

func TestLiveTransportSelection(t *testing.T) {
	ctx := context.Background()
	dsn := liveDSN(t)
	cfg, err := ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if p, _ := cfg.unixSocket(); p == "" {
		t.Skip("not a loopback DSN")
	}
	for _, want := range []string{"unix", "tcp"} {
		cfg2 := cfg.Copy()
		cfg2.PreferUnixSocket = want == "unix"
		c, err := ConnectConfig(ctx, cfg2)
		if err != nil {
			t.Fatal(err)
		}
		var tr string
		c.QueryRow(ctx, "SHOW pdb.wire_transport").Scan(&tr)
		c.Close()
		if c.Transport() != want || tr != want {
			t.Fatalf("want %s: client %s server %s", want, c.Transport(), tr)
		}
	}
}
