package compare

// Differential tests: the same statements through this SDK against
// ParticleDB and through pgx / lib/pq against a throwaway PostgreSQL 17,
// where the semantics should match.
//
//	PDB_TEST_DSN=particledb://127.0.0.1:27300 \
//	PG_TEST_DSN=postgres://postgres@127.0.0.1:27350/postgres?sslmode=disable go test -run Differential

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	pdb "github.com/particledb-ai/particledb-go"
)

func envs(t *testing.T) (string, string) {
	p, g := os.Getenv("PDB_TEST_DSN"), os.Getenv("PG_TEST_DSN")
	if p == "" || g == "" {
		t.Skip("PDB_TEST_DSN and PG_TEST_DSN are required")
	}
	return p, g
}

type pair struct {
	p   *pdb.Conn
	g   *pgx.Conn
	ctx context.Context
}

func connectBoth(t *testing.T) pair {
	pd, pg := envs(t)
	ctx := context.Background()
	p, err := pdb.Connect(ctx, pd)
	if err != nil {
		t.Fatal(err)
	}
	g, err := pgx.Connect(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	// Prove the oracle is PostgreSQL and the subject is ParticleDB.
	var v string
	if err := g.QueryRow(ctx, "SELECT version()").Scan(&v); err != nil || !strings.HasPrefix(v, "PostgreSQL 17") {
		t.Fatalf("oracle is not PostgreSQL 17: %q %v", v, err)
	}
	var proto string
	if err := p.QueryRow(ctx, "SHOW pdb.wire_protocol").Scan(&proto); err != nil || proto != "pdb-native-v2" {
		t.Fatalf("subject is not ParticleDB native v2: %q %v", proto, err)
	}
	if _, err := p.Exec(ctx, "SET TIME ZONE 'UTC'"); err != nil {
		p.Close()
		g.Close(ctx)
		t.Fatal(err)
	}
	if _, err := g.Exec(ctx, "SET TIME ZONE 'UTC'"); err != nil {
		p.Close()
		g.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close(); g.Close(ctx) })
	return pair{p, g, ctx}
}

func (x pair) both(t *testing.T, sql string) {
	t.Helper()
	if _, err := x.p.Exec(x.ctx, sql); err != nil {
		t.Fatalf("pdb %s: %v", sql, err)
	}
	if _, err := x.g.Exec(x.ctx, sql); err != nil {
		t.Fatalf("pg %s: %v", sql, err)
	}
}

// canon renders a scanned value in the conformance suite's canonical form.
func canon(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case *string:
		if x == nil {
			return "NULL"
		}
		return *x
	case []byte:
		return fmt.Sprintf(`\x%x`, x)
	case time.Time:
		return x.UTC().Format("2006-01-02 15:04:05.999999")
	case float64:
		return fmt.Sprintf("%v", x)
	case float32:
		return fmt.Sprintf("%v", x)
	}
	return fmt.Sprint(v)
}

// TestDifferentialTextForms compares the text form of every type
// (::text on both servers, scanned as string on both clients).
func TestDifferentialTextForms(t *testing.T) {
	x := connectBoth(t)
	exprs := []string{
		"(-32768)::int2", "2147483647::int4", "(-9223372036854775808)::int8",
		"'NaN'::float8", "'Infinity'::float8", "0.1::float8", "1.5::float4",
		"0.52::numeric", "1.5::numeric(10,2)", "(-12345678901234567890.0001)::numeric",
		"true", "false", "''", "'héllo wörld ✓ 𝄞'", "'abc'::varchar(5)",
		`'\xdeadbeef'::bytea`, `'{"b":1,  "a":[1,2]}'::json`, `'{"b":1,  "a":[1,2]}'::jsonb`,
		"'2024-02-29'::date", "'1999-12-31 23:59:59.999999'::timestamp", "'0001-01-01 00:00:00'::timestamp",
		"'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid",
		"ARRAY[1, NULL, 3]::int4[]", "ARRAY['a', NULL, 'c,d']::text[]", "'{}'::int4[]",
	}
	for _, e := range exprs {
		var a, b *string
		if err := x.p.QueryRow(x.ctx, "SELECT ("+e+")::text").Scan(&a); err != nil {
			t.Errorf("pdb %s: %v", e, err)
			continue
		}
		if err := x.g.QueryRow(x.ctx, "SELECT ("+e+")::text").Scan(&b); err != nil {
			t.Errorf("pg %s: %v", e, err)
			continue
		}
		if canon(a) != canon(b) {
			t.Errorf("%s: pdb %q pg %q", e, canon(a), canon(b))
		}
	}
}

// TestDifferentialTypedScan scans native values (not text) on both clients.
func TestDifferentialTypedScan(t *testing.T) {
	x := connectBoth(t)
	type c struct {
		expr string
		mk   func() any
	}
	cases := []c{
		{"(-32768)::int2", func() any { return new(int16) }},
		{"2147483647::int4", func() any { return new(int32) }},
		{"(-9223372036854775808)::int8", func() any { return new(int64) }},
		{"0.1::float8", func() any { return new(float64) }},
		{"1.5::float4", func() any { return new(float32) }},
		{"'NaN'::float8", func() any { return new(float64) }},
		{"true", func() any { return new(bool) }},
		{"'héllo ✓ 𝄞'", func() any { return new(string) }},
		{`'\x00ff10'::bytea`, func() any { return new([]byte) }},
		{"'2024-02-29'::date", func() any { return new(time.Time) }},
		{"'1999-12-31 23:59:59.999999'::timestamp", func() any { return new(time.Time) }},
		{"'2024-03-10 10:00:00+00'::timestamptz", func() any { return new(time.Time) }},
		{"NULL::int8", func() any { return new(*int64) }},
		{"NULL::text", func() any { return new(*string) }},
		{"ARRAY[1,2,3]::int8[]", func() any { return new([]int64) }},
		{"ARRAY['a','b']::text[]", func() any { return new([]string) }},
	}
	for _, k := range cases {
		a, b := k.mk(), k.mk()
		if err := x.p.QueryRow(x.ctx, "SELECT "+k.expr).Scan(a); err != nil {
			t.Errorf("pdb %s: %v", k.expr, err)
			continue
		}
		if err := x.g.QueryRow(x.ctx, "SELECT "+k.expr).Scan(b); err != nil {
			t.Errorf("pg %s: %v", k.expr, err)
			continue
		}
		av, bv := reflect.ValueOf(a).Elem().Interface(), reflect.ValueOf(b).Elem().Interface()
		if ta, ok := av.(time.Time); ok {
			if !ta.Equal(bv.(time.Time)) {
				t.Errorf("%s: pdb %v pg %v", k.expr, ta, bv)
			}
			continue
		}
		if f, ok := av.(float64); ok && math.IsNaN(f) && math.IsNaN(bv.(float64)) {
			continue
		}
		if !reflect.DeepEqual(av, bv) {
			t.Errorf("%s: pdb %#v pg %#v", k.expr, av, bv)
		}
	}
}

// TestDifferentialParams binds the same Go values on both clients and
// compares the server's text of the bound value.
func TestDifferentialParams(t *testing.T) {
	x := connectBoth(t)
	ts := time.Date(2020, 6, 15, 12, 34, 56, 789012000, time.UTC)
	tz := time.Date(2024, 7, 1, 0, 0, 0, 500000000, time.FixedZone("x", 2*3600))
	cases := []struct {
		cast string
		v    any
	}{
		{"int8", int64(math.MaxInt64)}, {"int8", int64(-5)}, {"int4", int32(-7)}, {"int2", int16(12)},
		{"float8", 2.5}, {"float8", -0.0001}, {"float8", 1e300},
		{"text", "x'); DROP TABLE t; --"}, {"text", `back\slash`}, {"text", "unicode ✓ 𝄞"},
		{"bool", true}, {"bool", false},
		{"bytea", []byte{0, 1, 2, 0xff}},
		{"timestamptz", tz}, {"timestamp", ts}, {"date", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"jsonb", json.RawMessage(`{"k": "v", "n": null}`)},
		{"int8[]", []int64{7, -9}}, {"text[]", []string{"a", "b,c", `q"x`}},
		{"uuid", "123e4567-e89b-12d3-a456-426614174000"},
	}
	for _, k := range cases {
		var a, b string
		q := "SELECT ($1::" + k.cast + ")::text"
		pv := k.v
		if k.cast == "timestamp" {
			pv = pdb.Timestamp{Time: k.v.(time.Time)}
		}
		if err := x.p.QueryRow(x.ctx, q, pv).Scan(&a); err != nil {
			t.Errorf("pdb %s %v: %v", k.cast, k.v, err)
			continue
		}
		gv := k.v
		if err := x.g.QueryRow(x.ctx, q, gv).Scan(&b); err != nil {
			t.Errorf("pg %s %v: %v", k.cast, k.v, err)
			continue
		}
		if k.cast == "timestamptz" {
			// session zones differ: compare instants
			ta, _ := time.Parse("2006-01-02 15:04:05.999999-07", a)
			tb, _ := time.Parse("2006-01-02 15:04:05.999999-07", b)
			if !ta.Equal(tb) {
				t.Errorf("timestamptz: pdb %q pg %q", a, b)
			}
			continue
		}
		if a != b {
			t.Errorf("%s %#v: pdb %q pg %q", k.cast, k.v, a, b)
		}
	}
	// a numeric keeps its scale
	var a, b string
	x.p.QueryRow(x.ctx, "SELECT ($1::numeric)::text", pdb.MustParseDecimal("1.50")).Scan(&a)
	x.g.QueryRow(x.ctx, "SELECT ($1::numeric)::text", "1.50").Scan(&b)
	if a != b {
		t.Errorf("numeric 1.50: pdb %q pg %q", a, b)
	}
}

func pgState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// TestDifferentialSQLState compares the SQLSTATE of the same failures.
func TestDifferentialSQLState(t *testing.T) {
	x := connectBoth(t)
	x.both(t, "DROP TABLE IF EXISTS dif_c")
	x.both(t, "DROP TABLE IF EXISTS dif_p")
	x.both(t, "CREATE TABLE dif_p (id int8 PRIMARY KEY)")
	x.both(t, "CREATE TABLE dif_c (id int8 PRIMARY KEY, p int8 NOT NULL REFERENCES dif_p(id))")
	x.both(t, "INSERT INTO dif_p VALUES (1)")
	for _, q := range []string{
		"INSERT INTO dif_p VALUES (1)", "SELECT * FROM dif_no_such", "SELEC 1", "SELECT nope FROM dif_p",
		"SELECT 1/0", "SELECT 'abc'::int4", "SELECT dif_no_fn(1)", "INSERT INTO dif_c VALUES (1, NULL)",
		"INSERT INTO dif_c VALUES (1, 99)",
	} {
		_, ep := x.p.Exec(x.ctx, q)
		_, eg := x.g.Exec(x.ctx, q)
		if pdb.SQLState(ep) != pgState(eg) || pgState(eg) == "" {
			t.Errorf("%s: pdb %s (%v) pg %s", q, pdb.SQLState(ep), ep, pgState(eg))
		}
	}
	// failed transaction block: 25P02, and COMMIT reports a rollback
	tp, _ := x.p.Begin(x.ctx)
	tg, _ := x.g.Begin(x.ctx)
	tp.Exec(x.ctx, "SELECT 1/0")
	tg.Exec(x.ctx, "SELECT 1/0")
	_, ep := tp.Exec(x.ctx, "SELECT 1")
	_, eg := tg.Exec(x.ctx, "SELECT 1")
	if pdb.SQLState(ep) != "25P02" || pgState(eg) != "25P02" {
		t.Errorf("failed block: pdb %v pg %v", ep, eg)
	}
	ep, eg = tp.Commit(x.ctx), tg.Commit(x.ctx)
	if !errors.Is(ep, pdb.ErrTxCommitRollback) || !errors.Is(eg, pgx.ErrTxCommitRollback) {
		t.Errorf("commit of failed tx: pdb %v pg %v", ep, eg)
	}
}

// TestDifferentialBatchAtomic: a pgx batch outside a transaction is atomic;
// so is a particledb Batch.
func TestDifferentialBatchAtomic(t *testing.T) {
	x := connectBoth(t)
	x.both(t, "DROP TABLE IF EXISTS dif_b")
	x.both(t, "CREATE TABLE dif_b (id int8 PRIMARY KEY)")
	pb, gb := &pdb.Batch{}, &pgx.Batch{}
	for _, id := range []int{1, 2, 1, 3} {
		pb.Queue("INSERT INTO dif_b VALUES ($1)", id)
		gb.Queue("INSERT INTO dif_b VALUES ($1)", id)
	}
	ep := x.p.SendBatch(x.ctx, pb).Close()
	eg := x.g.SendBatch(x.ctx, gb).Close()
	if pdb.SQLState(ep) != "23505" || pgState(eg) != "23505" {
		t.Fatalf("pdb %v pg %v", ep, eg)
	}
	var np, ng int
	x.p.QueryRow(x.ctx, "SELECT count(*) FROM dif_b").Scan(&np)
	x.g.QueryRow(x.ctx, "SELECT count(*) FROM dif_b").Scan(&ng)
	if np != ng || np != 0 {
		t.Fatalf("after failed batch: pdb %d pg %d", np, ng)
	}
}

// TestDifferentialStdlib compares database/sql behaviour with lib/pq.
func TestDifferentialStdlib(t *testing.T) {
	pd, pg := envs(t)
	p, _ := sql.Open("particledb", pd)
	g, _ := sql.Open("postgres", pg)
	defer p.Close()
	defer g.Close()
	for _, db := range []*sql.DB{p, g} {
		db.Exec("DROP TABLE IF EXISTS dif_s")
		if _, err := db.Exec("CREATE TABLE dif_s (id int8 PRIMARY KEY, n numeric(10,2), v varchar(7) NOT NULL, a int4[], j jsonb, t timestamptz, b bytea, u uuid, f float8, bo bool)"); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec("INSERT INTO dif_s VALUES (1, 1.5, 'x', '{1,2}', '{\"a\":1}', '2024-01-01 00:00:00+00', '\\x0102', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 0.5, true), (2, NULL, 'y', NULL, NULL, NULL, NULL, NULL, NULL, NULL)"); err != nil {
			t.Fatal(err)
		}
	}
	q := "SELECT id, n, v, a, j, t, b, u, f, bo FROM dif_s ORDER BY id"
	rp, err := p.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	rg, err := g.Query(q)
	if err != nil {
		t.Fatal(err)
	}
	cp, _ := rp.ColumnTypes()
	cg, _ := rg.ColumnTypes()
	for i := range cp {
		if cp[i].DatabaseTypeName() != cg[i].DatabaseTypeName() {
			t.Errorf("col %d type pdb %s pq %s", i, cp[i].DatabaseTypeName(), cg[i].DatabaseTypeName())
		}
		lp, okp := cp[i].Length()
		lg, okg := cg[i].Length()
		if lp != lg || okp != okg {
			t.Errorf("col %d length pdb %d,%v pq %d,%v", i, lp, okp, lg, okg)
		}
		pp, sp, okp := cp[i].DecimalSize()
		pg2, sg, okg := cg[i].DecimalSize()
		if pp != pg2 || sp != sg || okp != okg {
			t.Errorf("col %d decimal pdb %d,%d,%v pq %d,%d,%v", i, pp, sp, okp, pg2, sg, okg)
		}
	}
	scan := func(r *sql.Rows) [][]string {
		var out [][]string
		for r.Next() {
			var (
				id int64
				n  sql.NullString
				v  string
				a  sql.NullString
				j  []byte
				ts sql.NullTime
				b  []byte
				u  sql.NullString
				f  sql.NullFloat64
				bo sql.NullBool
			)
			if err := r.Scan(&id, &n, &v, &a, &j, &ts, &b, &u, &f, &bo); err != nil {
				t.Fatal(err)
			}
			tsS := "NULL"
			if ts.Valid {
				tsS = ts.Time.UTC().String()
			}
			out = append(out, []string{fmt.Sprint(id), fmt.Sprint(n), v, fmt.Sprint(a), string(j), tsS, fmt.Sprintf("%x", b), fmt.Sprint(u), fmt.Sprint(f), fmt.Sprint(bo)})
		}
		return out
	}
	sp, sg := scan(rp), scan(rg)
	if !reflect.DeepEqual(sp, sg) {
		t.Errorf("rows differ:\npdb %v\npq  %v", sp, sg)
	}
	// Array scanning: particledb.Array and pq.Array read the same values.
	var ap, ag []int64
	if err := p.QueryRow("SELECT a FROM dif_s WHERE id = 1").Scan(pdb.Array(&ap)); err != nil {
		t.Fatal(err)
	}
	if err := g.QueryRow("SELECT a FROM dif_s WHERE id = 1").Scan(pq.Array(&ag)); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ap, ag) {
		t.Errorf("arrays pdb %v pq %v", ap, ag)
	}
	// multi-statement atomicity
	for _, db := range []*sql.DB{p, g} {
		_, err := db.Exec("INSERT INTO dif_s (id, v) VALUES (10, 'a'); INSERT INTO dif_s (id, v) VALUES (1, 'dup')")
		if err == nil {
			t.Fatal("duplicate accepted")
		}
	}
	var np, ng int
	p.QueryRow("SELECT count(*) FROM dif_s WHERE id = 10").Scan(&np)
	g.QueryRow("SELECT count(*) FROM dif_s WHERE id = 10").Scan(&ng)
	if np != ng {
		t.Errorf("multi-statement atomicity pdb %d pq %d", np, ng)
	}
	// isolation and read-only reach the server
	for _, db := range []*sql.DB{p, g} {
		tx, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		var lvl, ro string
		tx.QueryRow("SHOW transaction_isolation").Scan(&lvl)
		tx.QueryRow("SHOW transaction_read_only").Scan(&ro)
		tx.Rollback()
		if lvl != "repeatable read" || ro != "on" {
			t.Errorf("%T: isolation %q read_only %q", db.Driver(), lvl, ro)
		}
	}
}

// Compare the same timestamptz parameter type on both clients, then compare
// an explicit wall-clock Timestamp with pgx's inferred timestamp binding.
func TestDifferentialTimeIntoTimestamp(t *testing.T) {
	x := connectBoth(t)
	x.both(t, "DROP TABLE IF EXISTS dif_t")
	x.both(t, "CREATE TABLE dif_t (id int8, ts timestamp, tz timestamptz)")
	v := time.Date(2024, 1, 1, 23, 30, 0, 0, time.FixedZone("x", 2*3600))
	if _, err := x.p.Exec(x.ctx, "INSERT INTO dif_t VALUES (1, $1::timestamptz, $2)", v, v); err != nil {
		t.Fatal(err)
	}
	if _, err := x.g.Exec(x.ctx, "INSERT INTO dif_t VALUES (1, $1::timestamptz, $2)", v, v); err != nil {
		t.Fatal(err)
	}
	var ap, ag string
	var tp, tg time.Time
	if err := x.p.QueryRow(x.ctx, "SELECT ts::text, tz FROM dif_t").Scan(&ap, &tp); err != nil {
		t.Fatal(err)
	}
	if err := x.g.QueryRow(x.ctx, "SELECT ts::text, tz FROM dif_t").Scan(&ag, &tg); err != nil {
		t.Fatal(err)
	}
	if !tp.Equal(tg) {
		t.Errorf("timestamptz: pdb %v pg %v", tp, tg)
	}
	if ap != ag {
		t.Errorf("timestamp from a timestamptz parameter: pdb %q pgx %q", ap, ag)
	}
	if _, err := x.p.Exec(x.ctx, "INSERT INTO dif_t VALUES (2, $1, $2)", pdb.Timestamp{Time: v}, v); err != nil {
		t.Fatal(err)
	}
	if _, err := x.g.Exec(x.ctx, "INSERT INTO dif_t VALUES (2, $1, $2)", v, v); err != nil {
		t.Fatal(err)
	}
	if err := x.p.QueryRow(x.ctx, "SELECT ts::text FROM dif_t WHERE id = 2").Scan(&ap); err != nil {
		t.Fatal(err)
	}
	if err := x.g.QueryRow(x.ctx, "SELECT ts::text FROM dif_t WHERE id = 2").Scan(&ag); err != nil {
		t.Fatal(err)
	}
	if ap != ag || ap != "2024-01-01 23:30:00" {
		t.Errorf("wall-clock timestamp: pdb %q pgx %q", ap, ag)
	}
}

// Timestamp-shaped date literals and Date parameters match PostgreSQL.
func TestDifferentialDateFromTimestampLiteral(t *testing.T) {
	x := connectBoth(t)
	x.both(t, "DROP TABLE IF EXISTS dif_d")
	x.both(t, "CREATE TABLE dif_d (d date)")
	_, ep := x.p.Exec(x.ctx, "INSERT INTO dif_d VALUES ('2024-01-01T23:30+02:00')")
	_, eg := x.g.Exec(x.ctx, "INSERT INTO dif_d VALUES ('2024-01-01T23:30+02:00')")
	if eg != nil {
		t.Fatalf("PostgreSQL refused: %v", eg)
	}
	if ep != nil {
		t.Fatalf("ParticleDB refused a valid date literal: %v", ep)
	}
	var dp, dg string
	if err := x.p.QueryRow(x.ctx, "SELECT d::text FROM dif_d").Scan(&dp); err != nil {
		t.Fatal(err)
	}
	if err := x.g.QueryRow(x.ctx, "SELECT d::text FROM dif_d").Scan(&dg); err != nil {
		t.Fatal(err)
	}
	if dp != dg {
		t.Fatalf("date literal: pdb %q pg %q", dp, dg)
	}
	// An explicit Date binds calendar fields without a timezone conversion.
	if _, err := x.p.Exec(x.ctx, "INSERT INTO dif_d VALUES ($1)", pdb.Date{Time: time.Date(2024, 1, 1, 23, 30, 0, 0, time.FixedZone("x", 7200))}); err != nil {
		t.Fatal(err)
	}
	var d string
	x.p.QueryRow(x.ctx, "SELECT d::text FROM dif_d").Scan(&d)
	if d != "2024-01-01" {
		t.Fatalf("Date workaround stored %q", d)
	}
}

// Declared expression types must be retained alongside stored-column types.
func TestDifferentialExpressionTypeNames(t *testing.T) {
	pd, pg := envs(t)
	p, _ := sql.Open("particledb", pd)
	g, _ := sql.Open("postgres", pg)
	defer p.Close()
	defer g.Close()
	typeOf := func(db *sql.DB, q string) string {
		r, err := db.Query(q)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		ct, _ := r.ColumnTypes()
		return ct[0].DatabaseTypeName()
	}
	if got := typeOf(g, "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid"); got != "UUID" {
		t.Fatalf("PostgreSQL says %s", got)
	}
	if got := typeOf(p, "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid"); got != "UUID" {
		t.Fatalf("uuid expression is %s (update SUPPORT.md)", got)
	}
	if got := typeOf(p, "SELECT '[1,2,3]'::vector(3)"); got != "VECTOR" {
		t.Fatalf("vector expression is %s (update SUPPORT.md)", got)
	}
	p.Exec("DROP TABLE IF EXISTS dif_u")
	p.Exec("CREATE TABLE dif_u (u uuid, v vector(3))")
	if got := typeOf(p, "SELECT u FROM dif_u"); got != "UUID" {
		t.Fatalf("stored uuid column is %s", got)
	}
	if got := typeOf(p, "SELECT v FROM dif_u"); got != "VECTOR" {
		t.Fatalf("stored vector column is %s", got)
	}
}
