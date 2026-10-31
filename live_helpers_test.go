package particledb

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// Live tests run against a server named by PDB_TEST_DSN (for example
// particledb://127.0.0.1:27300). They are skipped without it.
func liveDSN(t testing.TB) string {
	t.Helper()
	dsn := os.Getenv("PDB_TEST_DSN")
	if dsn == "" {
		t.Skip("PDB_TEST_DSN is not set")
	}
	return dsn
}

func liveConn(t testing.TB) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := Connect(ctx, liveDSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func mustExec(t testing.TB, c interface {
	Exec(context.Context, string, ...any) (CommandTag, error)
}, sql string, args ...any) CommandTag {
	t.Helper()
	tag, err := c.Exec(context.Background(), sql, args...)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return tag
}

// tableName returns a per-test table name.
func tableName(t testing.TB) string {
	n := strings.ToLower(strings.NewReplacer("/", "_", " ", "_", "-", "_", "#", "_").Replace(t.Name()))
	return "gt_" + n
}
