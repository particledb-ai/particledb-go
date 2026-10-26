package particledb

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer speaks just enough of the native wire for client-side tests
// that need a misbehaving or scripted server.
type fakeServer struct {
	ln      net.Listener
	helloOK []byte
	handle  func(fs *fakeConn, f frame) // called for every request frame
	mu      sync.Mutex
	conns   []*fakeConn
	auth    chan [2]string
}

type fakeConn struct {
	nc net.Conn
	mu sync.Mutex
}

func (fc *fakeConn) send(typ, flags byte, sid uint32, payload []byte) {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	b := appendV2Header(nil, typ, flags, sid, len(payload))
	fc.nc.Write(append(b, payload...))
}

func okPayload(n uint64) []byte { return binary.LittleEndian.AppendUint64(nil, n) }

func errPayload(code uint16, msg string) []byte {
	return append(binary.LittleEndian.AppendUint16(nil, code), msg...)
}

func helloOKBytes(caps, ext byte, maxStreams uint32) []byte {
	p := []byte{2, caps, fmtArrowIPC}
	p = binary.LittleEndian.AppendUint32(p, maxStreams)
	p = binary.LittleEndian.AppendUint32(p, 65536)
	p = append(p, 1, ext)
	return p
}

func newFakeServer(t *testing.T, helloOK []byte, handle func(*fakeConn, frame)) *fakeServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeServer{ln: ln, helloOK: helloOK, handle: handle, auth: make(chan [2]string, 4)}
	go fs.serve()
	t.Cleanup(func() { fs.close() })
	return fs
}

func (fs *fakeServer) dsn() string {
	return "particledb://" + fs.ln.Addr().String() + "?prefer_unix_socket=false&cancel_grace=300ms&user=u&password=p"
}

func (fs *fakeServer) close() {
	fs.ln.Close()
	fs.mu.Lock()
	for _, c := range fs.conns {
		c.nc.Close()
	}
	fs.mu.Unlock()
}

func (fs *fakeServer) dropAll() {
	fs.mu.Lock()
	for _, c := range fs.conns {
		c.nc.Close()
	}
	fs.mu.Unlock()
}

func (fs *fakeServer) serve() {
	for {
		nc, err := fs.ln.Accept()
		if err != nil {
			return
		}
		fc := &fakeConn{nc: nc}
		fs.mu.Lock()
		fs.conns = append(fs.conns, fc)
		fs.mu.Unlock()
		go fs.run(fc)
	}
}

func (fs *fakeServer) run(fc *fakeConn) {
	var h [v1HeaderSize]byte
	if _, err := io.ReadFull(fc.nc, h[:]); err != nil {
		return
	}
	p := make([]byte, binary.LittleEndian.Uint32(h[:]))
	io.ReadFull(fc.nc, p)
	hdr := binary.LittleEndian.AppendUint32(nil, uint32(len(fs.helloOK)))
	hdr = append(hdr, 0, 0, fHelloOK, 0)
	fc.nc.Write(append(hdr, fs.helloOK...))
	for {
		var vh [v2HeaderSize]byte
		if _, err := io.ReadFull(fc.nc, vh[:]); err != nil {
			return
		}
		n := binary.LittleEndian.Uint32(vh[7:])
		pl := make([]byte, n)
		if _, err := io.ReadFull(fc.nc, pl); err != nil {
			return
		}
		f := frame{typ: vh[1], flags: vh[2], sid: binary.LittleEndian.Uint32(vh[3:]), payload: pl}
		if f.typ == fAuth {
			ul := binary.LittleEndian.Uint32(pl)
			user := string(pl[4 : 4+ul])
			pass := string(pl[8+ul:])
			fs.auth <- [2]string{user, pass}
			if pass == "p" {
				fc.send(fOK, flagEndStream, f.sid, okPayload(0))
			} else {
				fc.send(fError, flagEndStream, f.sid, errPayload(errAuthFailedCode, "password authentication failed"))
				fc.nc.Close()
				return
			}
			continue
		}
		if f.typ == fPing {
			fc.send(fPong, flagEndStream, 0, nil)
			continue
		}
		if f.typ == fGoaway {
			return
		}
		go fs.handle(fc, f)
	}
}

const errAuthFailedCode = 0x14

const fullCaps = capV2Mux | capPrepared | capNumericText | capStatementNotice

func TestFakeRefusesProtocol1(t *testing.T) {
	fs := newFakeServer(t, []byte{1, capV1Legacy, fmtArrowIPC, 1, 0, 0, 0, 0, 0, 0, 0}, func(*fakeConn, frame) {})
	_, err := Connect(context.Background(), fs.dsn())
	if SQLState(err) != "08004" || !strings.Contains(err.Error(), "protocol 2") {
		t.Fatalf("got %v", err)
	}
}

func TestFakeAuthentication(t *testing.T) {
	fs := newFakeServer(t, helloOKBytes(fullCaps|capAuthRequired, 0, 8), func(fc *fakeConn, f frame) {
		fc.send(fOK, flagEndStream, f.sid, okPayload(1))
	})
	c, err := Connect(context.Background(), fs.dsn())
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	if got := <-fs.auth; got != [2]string{"u", "p"} {
		t.Fatalf("credentials %v", got)
	}
	_, err = Connect(context.Background(), strings.Replace(fs.dsn(), "password=p", "password=wrong", 1))
	if SQLState(err) != "28P01" {
		t.Fatalf("bad password: %v", err)
	}
	_, err = Connect(context.Background(), strings.Replace(strings.Replace(fs.dsn(), "user=u", "", 1), "password=p", "", 1))
	if SQLState(err) != "28000" {
		t.Fatalf("no user: %v", err)
	}
}

func TestFakeCancelWithoutAnswerClosesConnection(t *testing.T) {
	fs := newFakeServer(t, helloOKBytes(fullCaps, 0, 8), func(fc *fakeConn, f frame) {
		// never answers anything, not even the reset
	})
	c, err := Connect(context.Background(), fs.dsn())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = c.Exec(ctx, "SELECT pg_sleep(10)")
	if !errors.Is(err, ErrConnection) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if !c.IsClosed() {
		t.Fatal("connection with an unknown outcome stayed open")
	}
	dc := &driverConn{c: c}
	if dc.IsValid() || dc.ResetSession(context.Background()) != driver.ErrBadConn {
		t.Fatal("driver kept a dead connection")
	}
}

func TestFakeCancelAnswered57014(t *testing.T) {
	var mu sync.Mutex
	pending := map[uint32]bool{}
	fs := newFakeServer(t, helloOKBytes(fullCaps, 0, 8), nil)
	fs.handle = func(fc *fakeConn, f frame) {
		mu.Lock()
		defer mu.Unlock()
		if f.typ == fRstStreamC {
			if pending[f.sid] {
				fc.send(fError, flagEndStream, f.sid, errPayload(57014, "canceling statement due to user request"))
			}
			return
		}
		pending[f.sid] = true
	}
	c, err := Connect(context.Background(), fs.dsn())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = c.Exec(ctx, "SELECT pg_sleep(10)")
	if !errors.Is(err, ErrQueryCanceled) || !errors.Is(err, context.DeadlineExceeded) || c.IsClosed() {
		t.Fatalf("got %v closed=%v", err, c.IsClosed())
	}
}

func TestFakeStreamLimitIsRetried(t *testing.T) {
	var mu sync.Mutex
	refused := 0
	fs := newFakeServer(t, helloOKBytes(fullCaps, 0, 8), nil)
	fs.handle = func(fc *fakeConn, f frame) {
		mu.Lock()
		r := refused
		refused++
		mu.Unlock()
		if r < 3 {
			fc.send(fRstStreamS, flagEndStream, f.sid, binary.LittleEndian.AppendUint16(nil, errStreamLimit))
			return
		}
		fc.send(fOK, flagEndStream, f.sid, okPayload(7))
	}
	c, err := Connect(context.Background(), fs.dsn())
	if err != nil {
		t.Fatal(err)
	}
	tag, err := c.Exec(context.Background(), "UPDATE t SET a = 1")
	if err != nil || tag.RowsAffected() != 7 {
		t.Fatalf("%v %v", tag, err)
	}
}

func TestFakeLostConnectionIsClass08(t *testing.T) {
	fs := newFakeServer(t, helloOKBytes(fullCaps, 0, 8), func(fc *fakeConn, f frame) {})
	c, err := Connect(context.Background(), fs.dsn())
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.Exec(context.Background(), "SELECT 1"); done <- err }()
	time.Sleep(50 * time.Millisecond)
	fs.dropAll()
	select {
	case err := <-done:
		if !errors.Is(err, ErrConnection) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lost connection not reported")
	}
	// a request on the dead connection was never sent: database/sql may retry it
	_, err = (&driverConn{c: c}).ExecContext(context.Background(), "SELECT 1", nil)
	if err != driver.ErrBadConn {
		t.Fatalf("dead connection: %v", err)
	}
}

func TestFakeGoaway(t *testing.T) {
	fs := newFakeServer(t, helloOKBytes(fullCaps, 0, 8), func(fc *fakeConn, f frame) {
		p := binary.LittleEndian.AppendUint32(nil, f.sid)
		fc.send(fGoaway, 0, 0, binary.LittleEndian.AppendUint16(p, 0))
	})
	c, err := Connect(context.Background(), fs.dsn())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Exec(context.Background(), "SELECT 1")
	if !errors.Is(err, ErrConnection) || !c.IsClosed() {
		t.Fatalf("got %v", err)
	}
}

func TestFakeNotices(t *testing.T) {
	notice := []byte{1, 0, 7}
	notice = append(notice, "WARNING"...)
	notice = append(notice, 5)
	notice = append(notice, "01000"...)
	notice = binary.LittleEndian.AppendUint32(notice, 5)
	notice = append(notice, "hello"...)
	fs := newFakeServer(t, helloOKBytes(fullCaps, 0, 8), func(fc *fakeConn, f frame) {
		fc.send(fNotice, 0, f.sid, notice)
		fc.send(fOK, flagEndStream, f.sid, okPayload(0))
	})
	cfg, _ := ParseConfig(fs.dsn())
	var got []Notice
	cfg.OnNotice = func(_ *Conn, n Notice) { got = append(got, n) }
	c, err := ConnectConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(context.Background(), "CREATE INDEX i ON t USING gin (j)"); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != (Notice{"WARNING", "01000", "hello"}) {
		t.Fatalf("%v", got)
	}
}

// A cached statement the server no longer has is prepared again once.
func TestFakeReprepareVanishedStatement(t *testing.T) {
	var mu sync.Mutex
	prepares, executes := 0, 0
	fs := newFakeServer(t, helloOKBytes(fullCaps, capExtTypedParams, 8), nil)
	fs.handle = func(fc *fakeConn, f frame) {
		mu.Lock()
		defer mu.Unlock()
		switch f.typ {
		case fPrepare:
			prepares++
			fc.send(fPrepareOK, flagEndStream, f.sid, []byte{1, 0})
		case fExecute:
			executes++
			if executes == 2 {
				fc.send(fError, flagEndStream, f.sid, errPayload(1064, `prepared statement "s1" does not exist`))
				return
			}
			fc.send(fOK, flagEndStream, f.sid, okPayload(1))
		}
	}
	c, err := Connect(context.Background(), fs.dsn())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := c.Exec(context.Background(), "UPDATE t SET a = $1", i); err != nil {
			t.Fatalf("exec %d: %v", i, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if prepares != 2 || executes != 4 {
		t.Fatalf("prepares %d executes %d", prepares, executes)
	}
}
