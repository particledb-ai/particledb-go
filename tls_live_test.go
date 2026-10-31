package particledb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TLS tests need a server started with --tls-cert/--tls-key:
//
//	PDB_TLS_PORT       the database port (direct TLS, ALPN "pdb")
//	PDB_TLS_CA         the PEM CA that signed the server certificate (SAN DNS:localhost)
//	PDB_TLS_OTHER_CA   an unrelated CA
//	PDB_TLS_UPGRADE_PORT  optional: a dedicated native-wire listener (in-band upgrade)
func tlsEnv(t *testing.T) (port, ca, other string) {
	port, ca, other = os.Getenv("PDB_TLS_PORT"), os.Getenv("PDB_TLS_CA"), os.Getenv("PDB_TLS_OTHER_CA")
	if port == "" || ca == "" {
		t.Skip("PDB_TLS_PORT / PDB_TLS_CA not set")
	}
	return
}

func tryConnect(dsn string) (*Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return Connect(ctx, dsn)
}

func TestTLSModes(t *testing.T) {
	port, ca, other := tlsEnv(t)
	ok := func(name, dsn, transport string) {
		t.Helper()
		c, err := tryConnect(dsn)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		defer c.Close()
		if c.Transport() != transport {
			t.Fatalf("%s: transport %s", name, c.Transport())
		}
		var tr string
		if err := c.QueryRow(context.Background(), "SHOW pdb.wire_transport").Scan(&tr); err != nil {
			t.Fatal(err)
		}
		if tr != transport {
			t.Fatalf("%s: server says transport %s", name, tr)
		}
	}
	bad := func(name, dsn, state string) {
		t.Helper()
		c, err := tryConnect(dsn)
		if err == nil {
			c.Close()
			t.Fatalf("%s: connected, want failure", name)
		}
		var pe *Error
		if !errors.As(err, &pe) || (state != "" && pe.SQLState != state) {
			t.Fatalf("%s: %v (want SQLSTATE %s)", name, err, state)
		}
	}
	base := "particledb://localhost:" + port
	ok("require", base+"?sslmode=require", "tls")
	ok("verify-ca", base+"?sslmode=verify-ca&sslrootcert="+ca, "tls")
	ok("verify-full", base+"?sslmode=verify-full&sslrootcert="+ca, "tls")
	ok("require+root=verify-ca", base+"?sslmode=require&sslrootcert="+ca, "tls")
	// verify-full against a name not in the certificate (127.0.0.1 has no IP SAN)
	bad("verify-full wrong host", "particledb://127.0.0.1:"+port+"?sslmode=verify-full&sslrootcert="+ca, "08006")
	// verify-ca works by IP: no host name check
	ok("verify-ca by IP", "particledb://127.0.0.1:"+port+"?sslmode=verify-ca&sslrootcert="+ca, "tls")
	if other != "" {
		bad("verify-ca wrong CA", base+"?sslmode=verify-ca&sslrootcert="+other, "08006")
		bad("verify-full wrong CA", base+"?sslmode=verify-full&sslrootcert="+other, "08006")
	}
	// verify-ca without a root is a configuration error
	if _, err := ParseConfig(base + "?sslmode=verify-ca"); err == nil {
		t.Fatal("verify-ca without sslrootcert accepted")
	}
	// prefer/allow would silently fall back: refused
	if _, err := ParseConfig(base + "?sslmode=prefer"); err == nil {
		t.Fatal("sslmode=prefer accepted")
	}
	// TLS never uses the Unix socket
	cfg, _ := ParseConfig(base + "?sslmode=require")
	if p, _ := cfg.unixSocket(); p != "" {
		t.Fatalf("TLS config would use the Unix socket %s", p)
	}
	if up := os.Getenv("PDB_TLS_UPGRADE_PORT"); up != "" {
		ok("upgrade", "particledb://localhost:"+up+"?sslmode=verify-full&sslnegotiation=upgrade&sslrootcert="+ca, "tls")
		bad("direct TLS to a dedicated listener", "particledb://localhost:"+up+"?sslmode=require&prefer_unix_socket=false", "")
	}
	_ = filepath.Join
	_ = fmt.Sprint
}

func TestTLSDefaultIsPlaintext(t *testing.T) {
	cfg, err := ParseConfig("particledb://localhost")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLS != nil || cfg.Port != DefaultPort {
		t.Fatalf("defaults: TLS=%v port=%d", cfg.TLS, cfg.Port)
	}
}
