package particledb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func liveDB(t testing.TB) *sql.DB {
	t.Helper()
	db, err := sql.Open("particledb", liveDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestStdlibBasics(t *testing.T) {
	db := liveDB(t)
	ctx := context.Background()
	if err := db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	tbl := tableName(t)
	if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE " + tbl + " (id int8 PRIMARY KEY, name varchar(20) NOT NULL, price numeric(10,2), tags text[], meta jsonb, at timestamptz, u uuid)"); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2024, 5, 6, 7, 8, 9, 123456000, time.UTC)
	u, _ := ParseUUID("123e4567-e89b-12d3-a456-426614174000")
	res, err := db.Exec("INSERT INTO "+tbl+" VALUES ($1, $2, $3, $4, $5, $6, $7)", 1, "widget", MustParseDecimal("9.90"), Array([]string{"a", "b"}), json.RawMessage(`{"x":1}`), at, u)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("affected %d", n)
	}
	if _, err := db.Exec("INSERT INTO "+tbl+" (id, name) VALUES ($1, $2)", 2, "gadget"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query("SELECT id, name, price, tags, meta, at, u FROM " + tbl + " ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	cts, _ := rows.ColumnTypes()
	wantNames := []string{"INT8", "VARCHAR", "NUMERIC", "_TEXT", "JSONB", "TIMESTAMPTZ", "UUID"}
	for i, ct := range cts {
		if ct.DatabaseTypeName() != wantNames[i] {
			t.Errorf("column %d type %s, want %s", i, ct.DatabaseTypeName(), wantNames[i])
		}
	}
	if n, ok := cts[1].Nullable(); !ok || n {
		t.Errorf("name nullable=%v ok=%v", n, ok)
	}
	if l, ok := cts[1].Length(); !ok || l != 20 {
		t.Errorf("varchar length %d %v", l, ok)
	}
	if p, s, ok := cts[2].DecimalSize(); !ok || p != 10 || s != 2 {
		t.Errorf("numeric size %d %d %v", p, s, ok)
	}
	if cts[0].ScanType() != reflect.TypeOf(int64(0)) || cts[5].ScanType() != reflect.TypeOf(time.Time{}) {
		t.Errorf("scan types %v %v", cts[0].ScanType(), cts[5].ScanType())
	}
	type rec struct {
		id    int64
		name  string
		price sql.Null[Decimal]
		tags  []string
		meta  []byte
		at    sql.NullTime
		u     sql.NullString
	}
	var got []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.name, &r.price, Array(&r.tags), &r.meta, &r.at, &r.u); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].price.V.String() != "9.90" || !reflect.DeepEqual(got[0].tags, []string{"a", "b"}) ||
		string(got[0].meta) != `{"x": 1}` || !got[0].at.Time.Equal(at) || got[0].u.String != u.String() {
		t.Fatalf("%+v", got)
	}
	if got[1].price.Valid || got[1].tags != nil || got[1].meta != nil || got[1].at.Valid || got[1].u.Valid {
		t.Fatalf("nulls %+v", got[1])
	}
	// prepared statement
	st, err := db.Prepare("SELECT name FROM " + tbl + " WHERE id = $1")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		var name string
		if err := st.QueryRow(1).Scan(&name); err != nil || name != "widget" {
			t.Fatal(name, err)
		}
	}
	st.Close()
	var name string
	if err := db.QueryRow("SELECT name FROM "+tbl+" WHERE id = $1", 42).Scan(&name); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("no rows: %v", err)
	}
}

func TestStdlibTxAndErrors(t *testing.T) {
	db := liveDB(t)
	ctx := context.Background()
	tbl := tableName(t)
	db.Exec("DROP TABLE IF EXISTS " + tbl)
	db.Exec("CREATE TABLE " + tbl + " (id int8 PRIMARY KEY)")
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		t.Fatal(err)
	}
	var lvl string
	tx.QueryRow("SHOW transaction_isolation").Scan(&lvl)
	if lvl != "serializable" {
		t.Fatalf("level %s", lvl)
	}
	tx.Exec("INSERT INTO "+tbl+" VALUES ($1)", 1)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, _ = db.BeginTx(ctx, nil)
	tx.Exec("INSERT INTO "+tbl+" VALUES ($1)", 2)
	tx.Rollback()
	var n int
	db.QueryRow("SELECT count(*) FROM " + tbl).Scan(&n)
	if n != 1 {
		t.Fatalf("count %d", n)
	}
	_, err = db.Exec("INSERT INTO "+tbl+" VALUES ($1)", 1)
	var pe *Error
	if !errors.As(err, &pe) || !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("got %T %v", err, err)
	}
	// read-only transaction
	tx, _ = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	_, err = tx.Exec("INSERT INTO " + tbl + " VALUES (5)")
	tx.Rollback()
	if SQLState(err) != "25006" {
		t.Fatalf("read only: %v", err)
	}
	// context cancel
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = db.ExecContext(cctx, "SELECT pg_sleep(10)")
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrQueryCanceled) {
		t.Fatalf("cancel: %v", err)
	}
	if err := db.QueryRow("SELECT 1").Scan(&n); err != nil {
		t.Fatal(err)
	}
}

func TestStdlibMultiStatement(t *testing.T) {
	db := liveDB(t)
	tbl := tableName(t)
	db.Exec("DROP TABLE IF EXISTS " + tbl)
	db.Exec("CREATE TABLE " + tbl + " (id int8 PRIMARY KEY)")
	// atomic: the duplicate fails the whole string
	_, err := db.Exec("INSERT INTO " + tbl + " VALUES (1); INSERT INTO " + tbl + " VALUES (1)")
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("got %v", err)
	}
	var n int
	db.QueryRow("SELECT count(*) FROM " + tbl).Scan(&n)
	if n != 0 {
		t.Fatalf("count %d", n)
	}
	rows, err := db.Query("INSERT INTO " + tbl + " VALUES (1); INSERT INTO " + tbl + " VALUES (2); SELECT count(*) FROM " + tbl + "; SELECT 'x' AS s")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	sets := 0
	var cnt int
	var s string
	for {
		for rows.Next() {
			switch sets {
			case 2:
				rows.Scan(&cnt)
			case 3:
				rows.Scan(&s)
			}
		}
		sets++
		if !rows.NextResultSet() {
			break
		}
	}
	if sets != 4 || cnt != 2 || s != "x" {
		t.Fatalf("sets=%d cnt=%d s=%q err=%v", sets, cnt, s, rows.Err())
	}
}

func TestStdlibConcurrentPoolAndRaw(t *testing.T) {
	db := liveDB(t)
	db.SetMaxOpenConns(8)
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				var v int
				if err := db.QueryRow("SELECT $1::int8 + 1", g*1000+i).Scan(&v); err != nil || v != g*1000+i+1 {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = conn.Raw(func(dc any) error {
		nc := dc.(DriverConn).NativeConn()
		if nc.NegotiatedProtocol() != "pdb-native-v2" {
			t.Errorf("raw protocol %s", nc.NegotiatedProtocol())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
