package legacy

// Unix-domain-socket selection for the native wire client.
//
// Unit part (no server): which hosts try the socket, the path convention
// (<UnixSocketDir>/.s.PDB.<port>, the server's --unix-socket-dir), the
// DisableUnixSocket opt-out, the TCP fallback for an absent or stale socket,
// and the unix_socket / unix_socket_dir URL and DSN parameters.
//
// Live part (TestUnixSocketLive; runs only when PDB_NATIVE_PORT is set, a
// --no-auth server with its default socket in /tmp): Health().Protocol
// reports the socket kind the pooled connection really used.
//
//	go test -count=1 -run 'UnixSocket' -v .
//	PDB_NATIVE_PORT=5440 go test -count=1 -run 'UnixSocket' -v .

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
)

func TestUnixSocketLocalHostLiterals(t *testing.T) {
	for _, h := range []string{"localhost", "LOCALHOST", "127.0.0.1", "127.9.9.9", "::1", "[::1]"} {
		if !IsLocalHost(h) {
			t.Errorf("IsLocalHost(%q) = false", h)
		}
	}
	for _, h := range []string{"10.0.0.7", "db.example.com", "192.0.2.1", "::2"} {
		if IsLocalHost(h) {
			t.Errorf("IsLocalHost(%q) = true", h)
		}
	}
	if got := UnixSocketPath("", 5440); got != filepath.Join(DefaultUnixSocketDir, ".s.PDB.5440") {
		t.Errorf("UnixSocketPath default = %q", got)
	}
	if got := UnixSocketPath("/run/pdb", 7001); got != filepath.Join("/run/pdb", ".s.PDB.7001") {
		t.Errorf("UnixSocketPath = %q", got)
	}
}

// A TCP listener on port P plus (optionally) a Unix listener at
// <dir>/.s.PDB.P; each answers one HELLO so the route is observable.
func socketFixture(t *testing.T, withUnix bool) (dir string, port int) {
	t.Helper()
	// Keep Unix paths short; Windows has no system /tmp directory.
	tempRoot := "/tmp"
	if runtime.GOOS == "windows" {
		tempRoot = os.TempDir()
	}
	dir, err := os.MkdirTemp(tempRoot, "pdbgo")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	tl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tl.Close() })
	port = tl.Addr().(*net.TCPAddr).Port
	serve := func(ln net.Listener) {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				readClientFrame(c)
				_ = writeHelloOKFrame(c, 1)
			}()
		}
	}
	go serve(tl)
	if withUnix {
		ul, err := net.Listen("unix", UnixSocketPath(dir, port))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ul.Close() })
		go serve(ul)
	}
	return dir, port
}

func route(t *testing.T, host string, port int, disable bool, dir string) string {
	t.Helper()
	wc, err := dialWire(host, port, 2000, disable, dir)
	if err != nil {
		t.Fatalf("dialWire(%s, %d): %v", host, port, err)
	}
	defer wc.doClose()
	if _, err := wc.doHello(); err != nil {
		t.Fatalf("hello: %v", err)
	}
	return wc.protocol
}

func TestUnixSocketSelection(t *testing.T) {
	dir, port := socketFixture(t, true)
	want := "unix"
	if runtime.GOOS == "windows" {
		want = "tcp" // The legacy client deliberately uses TCP on Windows.
	}
	for _, h := range []string{"localhost", "127.0.0.1"} {
		if got := route(t, h, port, false, dir); got != want {
			t.Errorf("%s: route %q, want %s", h, got, want)
		}
	}
	if got := route(t, "127.0.0.1", port, true, dir); got != "tcp" {
		t.Errorf("DisableUnixSocket: route %q, want tcp", got)
	}
	if got := route(t, "127.0.0.1", port, false, "/nonexistent"); got != "tcp" {
		t.Errorf("another dir: route %q, want tcp", got)
	}
}

func TestUnixSocketAbsentOrStaleFallsBackToTCP(t *testing.T) {
	dir, port := socketFixture(t, false)
	if got := route(t, "127.0.0.1", port, false, dir); got != "tcp" {
		t.Errorf("absent: route %q, want tcp", got)
	}
	// Stale: a socket file nobody accepts on.
	ul, err := net.Listen("unix", UnixSocketPath(dir, port))
	if err != nil {
		t.Fatal(err)
	}
	ul.(*net.UnixListener).SetUnlinkOnClose(false)
	ul.Close()
	if _, err := os.Stat(UnixSocketPath(dir, port)); err != nil {
		t.Fatalf("stale file missing: %v", err)
	}
	if got := route(t, "127.0.0.1", port, false, dir); got != "tcp" {
		t.Errorf("stale: route %q, want tcp", got)
	}
}

func TestUnixSocketConnectParams(t *testing.T) {
	c, err := Connect("pdb://127.0.0.1:1/?unix_socket=off&unix_socket_dir=/run/pdb")
	if err != nil {
		t.Fatal(err)
	}
	if !c.config.DisableUnixSocket || c.config.UnixSocketDir != "/run/pdb" {
		t.Errorf("URL params: %+v", c.config)
	}
	if !c.native.opts.DisableUnixSocket || c.native.opts.UnixSocketDir != "/run/pdb" {
		t.Errorf("URL params not passed to the wire client: %+v", c.native.opts)
	}
	c.Close()
	c, err = Connect("host=127.0.0.1 port=1 unix_socket=off unix_socket_dir=/x")
	if err != nil {
		t.Fatal(err)
	}
	if !c.config.DisableUnixSocket || c.config.UnixSocketDir != "/x" {
		t.Errorf("DSN params: %+v", c.config)
	}
	c.Close()
	if _, err := Connect("pdb://127.0.0.1:1/?unix_socket=maybe"); err == nil {
		t.Error("unix_socket=maybe accepted")
	}
}

func TestUnixSocketLive(t *testing.T) {
	portStr := os.Getenv("PDB_NATIVE_PORT")
	if portStr == "" {
		t.Skip("PDB_NATIVE_PORT not set")
	}
	port, _ := strconv.Atoi(portStr)
	if _, err := os.Stat(UnixSocketPath("", port)); err != nil {
		t.Skipf("server has no socket at %s", UnixSocketPath("", port))
	}
	ctx := context.Background()
	for _, tc := range []struct {
		disable bool
		dir     string
		want    string
	}{{false, "", "unix"}, {true, "", "tcp"}, {false, "/nonexistent", "tcp"}} {
		opts := defaultConnectionOptions()
		opts.Host, opts.Port, opts.PoolSize = "127.0.0.1", port, 1
		opts.DisableUnixSocket, opts.UnixSocketDir = tc.disable, tc.dir
		db := NewPdbWireClientWithOptions(opts)
		h, err := db.Health(ctx)
		if err != nil || !h.Connected {
			t.Fatalf("%+v: health %v %+v", tc, err, h)
		}
		if h.Protocol != tc.want {
			t.Errorf("%+v: protocol %q, want %q", tc, h.Protocol, tc.want)
		}
		if _, err := db.Query(ctx, "SELECT 1"); err != nil {
			t.Errorf("%+v: SELECT 1: %v", tc, err)
		}
		db.Close()
	}
}
