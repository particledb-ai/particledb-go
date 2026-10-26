package particledb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func livePool(t testing.TB, extra string) *Pool {
	t.Helper()
	dsn := liveDSN(t)
	if extra != "" {
		sep := "?"
		for _, c := range dsn {
			if c == '?' {
				sep = "&"
			}
		}
		dsn += sep + extra
	}
	p, err := NewPool(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestPoolSharedAndExclusive(t *testing.T) {
	p := livePool(t, "pool_max_conns=4")
	ctx := context.Background()
	tbl := tableName(t)
	mustExec(t, p, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, p, "CREATE TABLE "+tbl+" (id int8 PRIMARY KEY, v text)")
	var wg sync.WaitGroup
	errs := make(chan error, 256)
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				id := g*100 + i
				if _, err := p.Exec(ctx, "INSERT INTO "+tbl+" VALUES ($1, $2)", id, fmt.Sprint("v", id)); err != nil {
					errs <- err
					return
				}
				var v string
				if err := p.QueryRow(ctx, "SELECT v FROM "+tbl+" WHERE id = $1", id).Scan(&v); err != nil || v != fmt.Sprint("v", id) {
					errs <- fmt.Errorf("read-your-write %d: %q %v", id, v, err)
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
	if s := p.Stat(); s.Total > 4 {
		t.Fatalf("pool grew past MaxConns: %+v", s)
	}
	// transaction control on shared connections is refused
	if _, err := p.Exec(ctx, "BEGIN"); err == nil {
		t.Fatal("Pool.Exec(BEGIN) accepted")
	}
	// exclusive transaction
	tx, err := p.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, tx, "INSERT INTO "+tbl+" VALUES (99999, 'tx')")
	var n int
	p.QueryRow(ctx, "SELECT count(*) FROM "+tbl+" WHERE id = 99999").Scan(&n)
	if n != 0 {
		t.Fatal("uncommitted row visible through the pool")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	p.QueryRow(ctx, "SELECT count(*) FROM "+tbl+" WHERE id = 99999").Scan(&n)
	if n != 1 {
		t.Fatal("committed row missing")
	}
	// Acquire: session state stays with the caller; release rolls back
	c, err := p.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, c, "BEGIN")
	mustExec(t, c, "INSERT INTO "+tbl+" VALUES (88888, 'left open')")
	c.Release()
	p.QueryRow(ctx, "SELECT count(*) FROM "+tbl+" WHERE id = 88888").Scan(&n)
	if n != 0 {
		t.Fatal("a released connection's open transaction was not rolled back")
	}
	// CollectRows through the pool
	rows, err := p.Query(ctx, "SELECT id, v FROM "+tbl+" WHERE id < 3 ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	type rec struct {
		ID int64
		V  string
	}
	recs, err := CollectRows(rows, RowToStructByName[rec])
	if err != nil || len(recs) != 3 || recs[2].V != "v2" {
		t.Fatalf("%v %v", recs, err)
	}
	one, err := CollectOneRow(mustQuery(t, p, "SELECT count(*) FROM "+tbl), RowTo[int64])
	if err != nil || one != 64*20+1 {
		t.Fatalf("%d %v", one, err)
	}
	if _, err := CollectOneRow(mustQuery(t, p, "SELECT 1 WHERE false"), RowTo[int64]); !errors.Is(err, ErrNoRows) {
		t.Fatalf("no rows: %v", err)
	}
}

func mustQuery(t testing.TB, p *Pool, sql string, args ...any) *Rows {
	t.Helper()
	r, err := p.Query(context.Background(), sql, args...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
