package legacy

// Ambiguous transport failures are surfaced, never retried, and the
// connection is discarded.
//
// Once any byte of a request may have reached the server, a socket error, EOF
// or deadline expiry is AMBIGUOUS: the server may have executed and committed
// it. Resending applies the writes twice (a SELECT is not necessarily
// read-only either: `SELECT KV_SET(...)`), and pooling the connection hands
// the next caller a socket that may still deliver the late reply.
//
// A real TCP fake server (no ParticleDB) counts the requests it read and the
// connections it accepted. Only a failure before any byte of the request was
// written (connect refused, HELLO dropped on a fresh connection) is retried.

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type ambAct int

const (
	ambOK ambAct = iota
	ambClose
	ambHang
)

type ambServer struct {
	ln        net.Listener
	accepts   atomic.Int32
	requests  atomic.Int32
	dropHello atomic.Int32
	mu        sync.Mutex
	plan      []ambAct
	conns     []net.Conn
}

func startAmbServer(t *testing.T, ln net.Listener, dropHello int32, plan ...ambAct) *ambServer {
	t.Helper()
	if ln == nil {
		var err error
		ln, err = net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
	}
	s := &ambServer{ln: ln, plan: plan}
	s.dropHello.Store(dropHello)
	t.Cleanup(s.stop)
	go s.acceptLoop()
	return s
}

func (s *ambServer) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *ambServer) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.accepts.Add(1)
		s.mu.Lock()
		s.conns = append(s.conns, c)
		s.mu.Unlock()
		go s.serve(c)
	}
}

func (s *ambServer) serve(c net.Conn) {
	defer c.Close()
	for {
		mt, data, err := readClientFrame(c)
		if err != nil {
			return
		}
		switch mt {
		case msgWireHello:
			if s.dropHello.Add(-1) >= 0 {
				return // close during the handshake
			}
			_ = writeHelloOKFrame(c, 1)
			continue
		case msgWireQuery, msgWireBatch:
		default:
			if mt == msgWireClose {
				return
			}
			continue
		}
		s.requests.Add(1)
		s.mu.Lock()
		act := ambOK
		if len(s.plan) > 0 {
			act = s.plan[0]
			s.plan = s.plan[1:]
		}
		s.mu.Unlock()
		switch act {
		case ambClose:
			return
		case ambHang:
			time.Sleep(time.Hour)
			return
		}
		if mt == msgWireQuery {
			_ = writeOKFrame(c, 1)
		} else {
			n := binary.LittleEndian.Uint32(data[0:4])
			body := binary.LittleEndian.AppendUint32(nil, n)
			for i := uint32(0); i < n; i++ {
				body = append(body, batchEntryOK)
				body = binary.LittleEndian.AppendUint64(body, 1)
			}
			_ = writeServerFrame(c, msgWireBatchResult, 0, body)
		}
	}
}

func (s *ambServer) stop() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
}

func ambClient(t *testing.T, port int) *PdbWireClient {
	t.Helper()
	opts := defaultConnectionOptions()
	opts.Host = "127.0.0.1"
	opts.Port = port
	opts.PoolSize = 2
	opts.MaxRetries = 3
	opts.RetryDelayMs = 20
	opts.TimeoutMs = 5000
	c := NewPdbWireClientWithOptions(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func wantConnErr(t *testing.T, err error) {
	t.Helper()
	var ce *PdbConnectionError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *PdbConnectionError, got %T: %v", err, err)
	}
}

func TestAmbiguous_BatchDroppedAfterSend_SentOnceAndDiscarded(t *testing.T) {
	srv := startAmbServer(t, nil, 0, ambClose)
	c := ambClient(t, srv.port())
	err := c.Batch(context.Background(), []string{"INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"})
	if got := srv.requests.Load(); got != 1 {
		t.Fatalf("the batch was resent: server received it %d times", got)
	}
	wantConnErr(t, err)
	if err := c.Batch(context.Background(), []string{"INSERT INTO t VALUES (3)"}); err != nil {
		t.Fatalf("batch on a fresh connection: %v", err)
	}
	if got := srv.accepts.Load(); got != 2 {
		t.Fatalf("the dead connection was reused: accepts=%d", got)
	}
}

func TestAmbiguous_TransactionCommitDropped_SentOnce(t *testing.T) {
	srv := startAmbServer(t, nil, 0, ambClose)
	c := ambClient(t, srv.port())
	err := c.Transaction(context.Background(), func(tx *WireTx) error {
		tx.Execute("INSERT INTO t VALUES (1)")
		return nil
	})
	if got := srv.requests.Load(); got != 1 {
		t.Fatalf("the transaction was resent: server received it %d times", got)
	}
	wantConnErr(t, err)
}

func TestAmbiguous_StatementDropped_SentOnce(t *testing.T) {
	srv := startAmbServer(t, nil, 0, ambClose)
	c := ambClient(t, srv.port())
	_, err := c.Execute(context.Background(), "SELECT KV_SET('k', 'v')")
	if got := srv.requests.Load(); got != 1 {
		t.Fatalf("the statement was resent: server received it %d times", got)
	}
	wantConnErr(t, err)
	if n, err := c.Execute(context.Background(), "SELECT 1"); err != nil || n != 1 {
		t.Fatalf("execute on a fresh connection: n=%d err=%v", n, err)
	}
	if got := srv.accepts.Load(); got != 2 {
		t.Fatalf("the dead connection was reused: accepts=%d", got)
	}
}

func TestAmbiguous_DeadlineExpiry_SentOnceAndDiscarded(t *testing.T) {
	srv := startAmbServer(t, nil, 0, ambHang)
	c := ambClient(t, srv.port())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := c.Execute(ctx, "UPDATE t SET x = x + 1")
	if got := srv.requests.Load(); got != 1 {
		t.Fatalf("the timed-out statement was resent: server received it %d times", got)
	}
	wantConnErr(t, err)
	if n, err := c.Execute(context.Background(), "SELECT 1"); err != nil || n != 1 {
		t.Fatalf("execute on a fresh connection: n=%d err=%v", n, err)
	}
	if got := srv.accepts.Load(); got != 2 {
		t.Fatalf("the timed-out connection was reused: accepts=%d", got)
	}
}

func TestAmbiguous_DroppedHelloOnFreshConnection_IsRetried(t *testing.T) {
	srv := startAmbServer(t, nil, 1)
	c := ambClient(t, srv.port())
	if err := c.Batch(context.Background(), []string{"INSERT INTO t VALUES (1)"}); err != nil {
		t.Fatalf("a dropped HELLO was not retried: %v", err)
	}
	if a, r := srv.accepts.Load(), srv.requests.Load(); a != 2 || r != 1 {
		t.Fatalf("accepts=%d requests=%d, want 2 and 1", a, r)
	}
}

func TestAmbiguous_RefusedConnect_IsRetried(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close() // nothing listens now: the first connect is refused
	c := ambClient(t, port)
	c.opts.RetryDelayMs = 100
	srvCh := make(chan *ambServer, 1)
	go func() {
		time.Sleep(150 * time.Millisecond) // inside the ~100/200/400 ms backoff
		ln, err := net.Listen("tcp", probe.Addr().String())
		if err != nil {
			srvCh <- nil
			return
		}
		srvCh <- startAmbServer(t, ln, 0)
	}()
	n, err := c.Execute(context.Background(), "SELECT 1")
	srv := <-srvCh
	if srv == nil {
		t.Skip("could not re-listen on the probed port")
	}
	if err != nil || n != 1 {
		t.Fatalf("a refused connect was not retried: n=%d err=%v", n, err)
	}
	if got := srv.requests.Load(); got != 1 {
		t.Fatalf("requests=%d, want 1", got)
	}
}
