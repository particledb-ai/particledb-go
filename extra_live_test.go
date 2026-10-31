package particledb

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func withParam(dsn, p string) string {
	if strings.Contains(dsn, "?") {
		return dsn + "&" + p
	}
	return dsn + "?" + p
}

func TestLiveSessionParamsAtConnect(t *testing.T) {
	ctx := context.Background()
	c, err := Connect(ctx, withParam(liveDSN(t), "timezone=America/Los_Angeles&search_path=public&application_name=gosdk-test"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var tz string
	if err := c.QueryRow(ctx, "SHOW TimeZone").Scan(&tz); err != nil || tz != "America/Los_Angeles" {
		t.Fatalf("TimeZone %q %v", tz, err)
	}
	var app string
	if err := c.QueryRow(ctx, "SHOW application_name").Scan(&app); err != nil || app != "gosdk-test" {
		t.Fatalf("application_name %q %v", app, err)
	}
	var sp string
	if err := c.QueryRow(ctx, "SHOW search_path").Scan(&sp); err != nil || sp != "public" {
		t.Fatalf("search_path %q %v", sp, err)
	}
	// an invalid setting fails the connect with the server's SQLSTATE
	if _, err := Connect(ctx, withParam(liveDSN(t), "timezone=Not/AZone")); SQLState(err) != "22023" {
		t.Fatalf("invalid TimeZone: %v", err)
	}
}

func TestStdlibRefusals(t *testing.T) {
	db := sql.OpenDB(NewConnector(mustConfig(t)))
	defer db.Close()
	if _, err := db.Exec("SELECT $1", sql.Named("x", 1)); SQLState(err) != "0A000" {
		t.Fatalf("named parameter: %v", err)
	}
	if _, err := db.Exec("SELECT 1; BEGIN; SELECT 2"); SQLState(err) != "0A000" {
		t.Fatalf("multi-statement with transaction control: %v", err)
	}
	res, err := db.Exec("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := res.LastInsertId(); err == nil {
		t.Fatal("LastInsertId answered")
	}
}

func mustConfig(t *testing.T) *Config {
	cfg, err := ParseConfig(liveDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestLiveCollectHelpers(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	q := "SELECT g AS id, 'n' || g AS full_name FROM generate_series(1, 3) g"
	type rec struct {
		ID       int64
		FullName string
	}
	type lax struct {
		ID    int64
		Extra string
	}
	rows, _ := c.Query(ctx, q)
	byPos, err := CollectRows(rows, RowToStructByPos[rec])
	if err != nil || len(byPos) != 3 || byPos[2].FullName != "n3" {
		t.Fatalf("%v %v", byPos, err)
	}
	rows, _ = c.Query(ctx, q)
	if _, err := CollectRows(rows, RowToStructByName[lax]); err == nil {
		t.Fatal("strict mapping accepted a missing column")
	}
	rows, _ = c.Query(ctx, "SELECT g AS id FROM generate_series(1, 2) g")
	l, err := CollectRows(rows, RowToStructByNameLax[lax])
	if err != nil || len(l) != 2 || l[1].ID != 2 {
		t.Fatalf("%v %v", l, err)
	}
	rows, _ = c.Query(ctx, q)
	if _, err := CollectExactlyOneRow(rows, RowToStructByName[rec]); err != ErrTooManyRows {
		t.Fatalf("exactly one: %v", err)
	}
	rows, _ = c.Query(ctx, q)
	ids, err := AppendRows([]int64{0}, rows, func(r CollectableRow) (int64, error) {
		var id int64
		var s string
		return id, r.Scan(&id, &s)
	})
	if err != nil || len(ids) != 4 || ids[3] != 3 {
		t.Fatalf("%v %v", ids, err)
	}
	rows, _ = c.Query(ctx, q)
	var id int64
	var name string
	sum := int64(0)
	tag, err := ForEachRow(rows, []any{&id, &name}, func() error { sum += id; return nil })
	if err != nil || sum != 6 || tag.RowsAffected() != 3 {
		t.Fatalf("%d %v %v", sum, tag, err)
	}
	p, _ := RowToAddrOfStructByName[rec](mustRow(t, c, q))
	if p == nil || p.ID != 1 {
		t.Fatalf("%v", p)
	}
}

func mustRow(t *testing.T, c *Conn, q string) *Rows {
	r, err := c.Query(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Next() {
		t.Fatal("no row")
	}
	return r
}

func TestPoolMaintenance(t *testing.T) {
	p := livePool(t, "pool_health_check_period=50ms&pool_max_conn_idle_time=100ms&pool_min_conns=1")
	ctx := context.Background()
	c1, _ := p.Acquire(ctx)
	c2, _ := p.Acquire(ctx)
	c3, _ := p.Acquire(ctx)
	c1.Release()
	c2.Release()
	c3.Release()
	if s := p.Stat(); s.Total < 3 {
		t.Fatalf("%+v", s)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && p.Stat().Total > 1 {
		time.Sleep(50 * time.Millisecond)
	}
	if s := p.Stat(); s.Total != 1 {
		t.Fatalf("idle connections not closed down to MinConns: %+v", s)
	}
	if err := p.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

// Numeric values retain their declared scale in constrained and unconstrained columns.
func TestNumericScalePreserved(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (a numeric, b numeric(10,2))")
	mustExec(t, c, "INSERT INTO "+tbl+" VALUES ($1, $1)", MustParseDecimal("1.50"))
	var a, b Decimal
	if err := c.QueryRow(ctx, "SELECT a, b FROM "+tbl).Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if b.String() != "1.50" {
		t.Fatalf("numeric(10,2) read %s", b)
	}
	if a.String() != "1.50" {
		t.Fatalf("unconstrained numeric lost scale: %s", a)
	}
}

// A batch outside a transaction is atomic, so a statement the server will not
// run inside a transaction block (TRUNCATE) is refused there, loudly, rather
// than run non-atomically. On its own it runs.
func TestLiveBatchNonTransactionalDDL(t *testing.T) {
	c := liveConn(t)
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, c, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, c, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY)")
	mustExec(t, c, "INSERT INTO "+tbl+" VALUES (1)")
	b := &Batch{}
	b.Queue("TRUNCATE " + tbl)
	b.Queue("INSERT INTO " + tbl + " VALUES (2)")
	err := c.SendBatch(ctx, b).Close()
	if SQLState(err) != "25001" {
		t.Fatalf("got %v", err)
	}
	var n int
	c.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&n)
	if n != 1 {
		t.Fatalf("count %d", n)
	}
	mustExec(t, c, "TRUNCATE "+tbl)
}
