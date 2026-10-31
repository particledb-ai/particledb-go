package compare

import (
	"context"
	"database/sql"
	"testing"

	pdb "github.com/particledb-ai/particledb-go"
	"github.com/shopspring/decimal"
)

// shopspring/decimal is accepted without the SDK importing it: a parameter
// is recognised by its Coefficient/Exponent methods and sent as a binary
// numeric; a result scans into it through its sql.Scanner.
func TestShopspringDecimal(t *testing.T) {
	pd, _ := envs(t)
	ctx := context.Background()
	c, err := pdb.Connect(ctx, pd)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	in := decimal.RequireFromString("12345678901234567890.0125")
	var out decimal.Decimal
	if err := c.QueryRow(ctx, "SELECT $1::numeric", in).Scan(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Equal(in) || out.String() != "12345678901234567890.0125" {
		t.Fatalf("native: %s", out)
	}
	db, _ := sql.Open("particledb", pd)
	defer db.Close()
	var out2 decimal.Decimal
	if err := db.QueryRow("SELECT ($1::numeric) * 2", decimal.RequireFromString("-0.50")).Scan(&out2); err != nil {
		t.Fatal(err)
	}
	if !out2.Equal(decimal.RequireFromString("-1")) {
		t.Fatalf("database/sql: %s", out2)
	}
	var nd decimal.NullDecimal
	if err := db.QueryRow("SELECT NULL::numeric").Scan(&nd); err != nil || nd.Valid {
		t.Fatalf("null: %v %v", nd, err)
	}
}
