package legacy

// Live-server test: a failed statement inside a native-wire Batch or
// Transaction must surface as an error, never as success.
//
// Runs only when PDB_NATIVE_PORT is set (host 127.0.0.1, --no-auth server);
// otherwise it is skipped. Prints "NBE_PASS go" when every case passed.
//
//	PDB_NATIVE_PORT=5440 go test -count=1 -v -run TestNativeBatchErrorsLive .

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestNativeBatchErrorsLive(t *testing.T) {
	portStr := os.Getenv("PDB_NATIVE_PORT")
	if portStr == "" {
		t.Skip("PDB_NATIVE_PORT not set")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("PDB_NATIVE_PORT=%q: %v", portStr, err)
	}
	const tbl = "nbe_go"
	ctx := context.Background()

	opts := defaultConnectionOptions()
	opts.Host = "127.0.0.1"
	opts.Port = port
	opts.PoolSize = 1 // every call reuses the same connection
	opts.MaxRetries = 0
	db := NewPdbWireClientWithOptions(opts)
	defer db.Close()

	mustExec := func(t *testing.T, sql string) int64 {
		t.Helper()
		n, err := db.Execute(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		return n
	}
	expectDup := func(t *testing.T, what string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s: expected a duplicate-key error, got success (nil)", what)
		}
		var qe *PdbQueryError
		if !errors.As(err, &qe) {
			t.Fatalf("%s: expected *PdbQueryError, got %T: %v", what, err, err)
		}
		if !strings.Contains(err.Error(), "duplicate key") {
			t.Fatalf("%s: error does not mention duplicate key: %v", what, err)
		}
	}
	ins := func(k int) string { return fmt.Sprintf("INSERT INTO %s VALUES (%d)", tbl, k) }

	mustExec(t, "DROP TABLE IF EXISTS "+tbl)
	mustExec(t, "CREATE TABLE "+tbl+" (id INT PRIMARY KEY)")

	t.Run("a_batch_dup_raises", func(t *testing.T) {
		expectDup(t, "batch", db.Batch(ctx, []string{ins(1), ins(1), ins(2)}))
	})

	t.Run("b_transaction_dup_raises", func(t *testing.T) {
		expectDup(t, "transaction", db.Transaction(ctx, func(tx *WireTx) error {
			tx.Execute(ins(3))
			tx.Execute(ins(3))
			return nil
		}))
	})

	t.Run("c_connection_usable_after_failed_transaction", func(t *testing.T) {
		if n := mustExec(t, ins(4)); n != 1 {
			t.Fatalf("insert after failed transaction: affected=%d, want 1", n)
		}
		// k3 was rolled back, so it is insertable again.
		if n := mustExec(t, ins(3)); n != 1 {
			t.Fatalf("re-insert of rolled-back key: affected=%d, want 1", n)
		}
	})

	t.Run("d_success_still_succeeds", func(t *testing.T) {
		if err := db.Batch(ctx, []string{ins(10), ins(11)}); err != nil {
			t.Fatalf("successful batch: %v", err)
		}
		if err := db.Transaction(ctx, func(tx *WireTx) error {
			tx.Execute(ins(12))
			return nil
		}); err != nil {
			t.Fatalf("successful transaction: %v", err)
		}
		for _, k := range []int{10, 11, 12} {
			if n := mustExec(t, fmt.Sprintf("DELETE FROM %s WHERE id = %d", tbl, k)); n != 1 {
				t.Fatalf("row %d not visible: DELETE affected=%d", k, n)
			}
		}
	})

	t.Run("e_failed_statement_after_raw_begin_does_not_poison_the_pool", func(t *testing.T) {
		// BEGIN through the single-statement API, then a failing statement:
		// that pooled connection is inside a FAILED transaction and must not
		// be handed out again.
		mustExec(t, "BEGIN")
		_, err := db.Execute(ctx, ins(4))
		expectDup(t, "statement after BEGIN", err)
		if n := mustExec(t, ins(40)); n != 1 {
			t.Fatalf("next statement: affected=%d, want 1", n)
		}
		if n := mustExec(t, fmt.Sprintf("DELETE FROM %s WHERE id = 40", tbl)); n != 1 {
			t.Fatalf("the next statement did not commit: DELETE affected=%d", n)
		}
	})

	if !t.Failed() {
		fmt.Println("NBE_PASS go")
	}
}
