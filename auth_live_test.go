package particledb

import (
	"context"
	"os"
	"testing"
)

func TestLiveAuthentication(t *testing.T) {
	port := os.Getenv("PDB_AUTH_PORT")
	if port == "" {
		t.Skip("PDB_AUTH_PORT required")
	}
	cfg, err := ParseConfig("particledb://127.0.0.1:" + port + "?prefer_unix_socket=false")
	if err != nil {
		t.Fatal(err)
	}
	cfg.User, cfg.Password = os.Getenv("PDB_AUTH_USER"), os.Getenv("PDB_AUTH_PASS")
	c, err := ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var value int
	err = c.QueryRow(context.Background(), "SELECT 42").Scan(&value)
	c.Close()
	if err != nil || value != 42 {
		t.Fatalf("authenticated query: %d %v", value, err)
	}
	cfg.Password = "wrong-test-password"
	c, err = ConnectConfig(context.Background(), cfg)
	if c != nil {
		c.Close()
	}
	if SQLState(err) != "28P01" {
		t.Fatalf("wrong password: %v", err)
	}
}
