package legacy

// Live regressions for the native Client's transaction and batch edges.
// Runs only when PDB_NATIVE_PORT is set (127.0.0.1, --no-auth server).
//
//	PDB_NATIVE_PORT=5440 go test -count=1 -v -run TestNativeRegress .

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

func regressClient(t *testing.T, pool int) *Client {
	t.Helper()
	portStr := os.Getenv("PDB_NATIVE_PORT")
	if portStr == "" {
		t.Skip("PDB_NATIVE_PORT not set")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Host = "127.0.0.1"
	cfg.Port = port
	cfg.PoolSize = pool
	db, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func regressCount(t *testing.T, db *Client, tbl string) int64 {
	t.Helper()
	r, err := db.QueryOne("SELECT count(*) AS n FROM " + tbl)
	if err != nil {
		t.Fatal(err)
	}
	return r["n"].(int64)
}

// A panic inside fn must not hand the pinned connection, still inside the
// open transaction, back to the pool: with one pooled connection the next
// caller would otherwise run inside it and its COMMIT would persist the row
// the panicking transaction wrote.
func TestNativeRegressPanicInTransactionDoesNotPoolOpenTxn(t *testing.T) {
	db := regressClient(t, 1)
	const tbl = "ncl_go_panic_tx"
	_, _ = db.Execute("DROP TABLE IF EXISTS " + tbl)
	if _, err := db.Execute("CREATE TABLE " + tbl + " (id BIGINT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer db.Execute("DROP TABLE IF EXISTS " + tbl)

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected the panic to propagate")
			}
		}()
		_ = db.Transaction(func(tx *Tx) error {
			if _, err := tx.Execute("INSERT INTO "+tbl+" VALUES ($1)", 1); err != nil {
				t.Fatal(err)
			}
			panic("boom")
		})
	}()
	if idle := db.native.pool.idleCount(); idle != 0 {
		t.Fatalf("the panicked transaction's connection was pooled (idle=%d)", idle)
	}
	// Next borrower: a fresh connection, so its statements autocommit alone.
	if _, err := db.Execute("COMMIT"); err != nil && !strings.Contains(err.Error(), "no transaction") {
		t.Logf("COMMIT outside a transaction: %v", err)
	}
	if n := regressCount(t, db, tbl); n != 0 {
		t.Fatalf("row of the panicked transaction is visible/committed: count=%d", n)
	}
}

// A statement error caught inside fn: Transaction must not report success.
func TestNativeRegressSwallowedErrorFailsTransaction(t *testing.T) {
	db := regressClient(t, 2)
	const tbl = "ncl_go_swallow_tx"
	_, _ = db.Execute("DROP TABLE IF EXISTS " + tbl)
	if _, err := db.Execute("CREATE TABLE " + tbl + " (id BIGINT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer db.Execute("DROP TABLE IF EXISTS " + tbl)
	err := db.Transaction(func(tx *Tx) error {
		if _, err := tx.Execute("INSERT INTO "+tbl+" VALUES ($1)", 1); err != nil {
			return err
		}
		_, _ = tx.Execute("INSERT INTO "+tbl+" VALUES ($1)", 1) // duplicate key, swallowed
		return nil
	})
	if err == nil {
		t.Fatal("Transaction returned nil although the server rolled the block back")
	}
	if n := regressCount(t, db, tbl); n != 0 {
		t.Fatalf("count=%d after a rolled-back transaction", n)
	}
}

// A batch whose LAST row cannot fit a frame is refused before any frame is
// sent: outside a transaction the earlier frames would otherwise commit and
// the error would not say so.
func TestNativeRegressOversizedRowRefusedBeforeAnyFrame(t *testing.T) {
	db := regressClient(t, 2)
	const tbl = "ncl_go_oversize"
	_, _ = db.Execute("DROP TABLE IF EXISTS " + tbl)
	if _, err := db.Execute("CREATE TABLE " + tbl + " (id BIGINT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	defer db.Execute("DROP TABLE IF EXISTS " + tbl)
	rows := [][]interface{}{{int64(1), "ok"}, {int64(2), strings.Repeat("x", 17<<20)}}
	_, err := db.ExecuteBatch("INSERT INTO "+tbl+" VALUES ($1, $2)", rows)
	var pe *PdbParamError
	if !errors.As(err, &pe) {
		t.Fatalf("want *PdbParamError, got %T %v", err, err)
	}
	if n := regressCount(t, db, tbl); n != 0 {
		t.Fatalf("count=%d: a frame was sent before the oversized row was refused", n)
	}
}
