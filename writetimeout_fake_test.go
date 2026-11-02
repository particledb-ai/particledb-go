package particledb

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// stoppedReader completes the handshake and then never reads again: a server
// that was SIGSTOPped (or partitioned) while the client still has bytes to
// send. Once the kernel buffers fill, a client write blocks.
func stoppedReader(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	t.Cleanup(func() {
		ln.Close()
		for _, c := range conns {
			c.Close()
		}
	})
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			conns = append(conns, nc)
			if tc, ok := nc.(*net.TCPConn); ok {
				_ = tc.SetReadBuffer(4096)
			}
			go func(nc net.Conn) {
				var h [v1HeaderSize]byte
				if _, err := io.ReadFull(nc, h[:]); err != nil {
					return
				}
				p := make([]byte, binary.LittleEndian.Uint32(h[:]))
				io.ReadFull(nc, p)
				hello := helloOKBytes(fullCaps, 0, 8)
				hdr := binary.LittleEndian.AppendUint32(nil, uint32(len(hello)))
				hdr = append(hdr, 0, 0, fHelloOK, 0)
				nc.Write(append(hdr, hello...))
				// Stopped: never read again, never answer.
			}(nc)
		}
	}()
	return "particledb://" + ln.Addr().String() + "?prefer_unix_socket=false&cancel_grace=300ms"
}

// A request far larger than any socket buffer.
var hugeSQL = "SELECT '" + strings.Repeat("x", 64<<20) + "'"

func execBounded(t *testing.T, c *Conn, ctx context.Context, limit time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := c.Exec(ctx, hugeSQL)
		done <- err
	}()
	select {
	case err := <-done:
		if el := time.Since(start); el > limit {
			t.Fatalf("returned after %v, want < %v", el, limit)
		}
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("Exec blocked writing to a server that stopped reading")
		return nil
	}
}

func assertBroken(t *testing.T, c *Conn, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("a write to a stopped server succeeded")
	}
	if !errors.Is(err, ErrConnection) {
		t.Fatalf("want a connection error, got %v", err)
	}
	if SQLState(err)[:2] != "08" {
		t.Fatalf("SQLSTATE %q (%v)", SQLState(err), err)
	}
	if !c.IsClosed() {
		t.Fatal("a connection with a half-written frame stayed open")
	}
}

func TestFakeWriteHonoursContextDeadline(t *testing.T) {
	c, err := Connect(context.Background(), stoppedReader(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	assertBroken(t, c, execBounded(t, c, ctx, 5*time.Second))
}

func TestFakeWriteHonoursContextCancel(t *testing.T) {
	c, err := Connect(context.Background(), stoppedReader(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	assertBroken(t, c, execBounded(t, c, ctx, 5*time.Second))
}

func TestFakeWriteTimeoutBoundsABackgroundContext(t *testing.T) {
	c, err := Connect(context.Background(), stoppedReader(t)+"&write_timeout=300ms")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	assertBroken(t, c, execBounded(t, c, context.Background(), 5*time.Second))
}

func TestWriteTimeoutConfig(t *testing.T) {
	cfg, err := ParseConfig("particledb://127.0.0.1:1/db")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.writeTimeout(); got != 30*time.Second {
		t.Fatalf("default write timeout %v, want 30s", got)
	}
	cfg, err = ParseConfig("particledb://127.0.0.1:1/db?write_timeout=-1s")
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.writeTimeout(); got != 0 {
		t.Fatalf("negative write_timeout should disable the bound, got %v", got)
	}
}
