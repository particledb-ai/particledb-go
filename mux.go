package particledb

// The multiplexed connection: one socket, many concurrent streams.
//
// One reader goroutine owns the socket's read side and hands each frame to
// its stream. Writers append whole frames to a shared buffer under a mutex;
// the first writer to find no flush in progress writes the buffer out and
// keeps writing until it is empty, so concurrent requests are coalesced into
// few writes without a writer goroutine on the latency path.
//
// Requests on different streams of one connection execute concurrently on the
// server and have no defined order. A caller that needs order waits for each
// answer before it sends the next request (the database/sql driver and a
// transaction always do), or puts the statements in one BATCH frame.

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// maxStreamBuffer is how many bytes of unread frames one stream may queue
// before the reader stops reading the socket, when no other stream of the
// connection is open. With other streams open the reader keeps reading, so a
// caller that leaves one result unread while it runs another statement on the
// same connection cannot deadlock it.
const maxStreamBuffer = 8 << 20

type wireConn struct {
	cfg       *Config
	nc        net.Conn
	br        *bufio.Reader
	hello     helloOK
	transport string

	wmu       sync.Mutex
	wbuf      []byte
	wspare    []byte
	wflushing bool
	werr      error

	smu     sync.Mutex
	streams map[uint32]*stream
	nextID  uint32
	nOpen   atomic.Int32
	slots   chan struct{}
	blocked atomic.Pointer[stream]

	done      chan struct{}
	closeOnce sync.Once
	closeErr  error // set before done is closed

	pingMu sync.Mutex
	pong   chan struct{}

	slab []byte // reader-owned allocation arena for small payloads
	hdr  [v2HeaderSize]byte

	lastFlags atomic.Uint32 // flags of the latest terminal frame (transaction status)
}

type stream struct {
	c       *wireConn
	id      uint32
	mu      sync.Mutex
	space   sync.Cond
	q       []frame
	qbuf    [2]frame
	head    int
	qbytes  int
	notify  chan struct{}
	ended   bool // terminal frame received
	discard bool // the consumer abandoned the stream
	reset   bool // RST_STREAM sent
	err     error
	last    atomic.Int64 // unix nanos of the latest frame (abandon's progress check)
}

// dialWire opens and negotiates one connection.
func dialWire(ctx context.Context, cfg *Config) (*wireConn, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.connectTimeout())
	defer cancel()
	nc, transport, err := dialTransport(ctx, cfg)
	if err != nil {
		return nil, err
	}
	c := &wireConn{
		cfg:       cfg,
		nc:        nc,
		transport: transport,
		streams:   make(map[uint32]*stream),
		nextID:    1,
		done:      make(chan struct{}),
		pong:      make(chan struct{}, 1),
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = nc.SetDeadline(dl)
	}
	// Watch ctx during the handshake: cancelling unblocks the reads.
	stop := context.AfterFunc(ctx, func() { _ = nc.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	if err := c.handshake(); err != nil {
		nc.Close()
		if ctx.Err() != nil {
			return nil, connErr("connect: "+ctx.Err().Error(), ctx.Err())
		}
		return nil, err
	}
	if !stop() {
		nc.Close()
		return nil, connErr("connect: "+ctx.Err().Error(), ctx.Err())
	}
	_ = c.nc.SetDeadline(time.Time{})
	go c.readLoop()
	if err := c.applyRuntimeParams(ctx); err != nil {
		c.close(err)
		return nil, err
	}
	return c, nil
}

func dialTransport(ctx context.Context, cfg *Config) (net.Conn, string, error) {
	dial := cfg.DialFunc
	if dial == nil {
		d := &net.Dialer{KeepAlive: 30 * time.Second}
		dial = d.DialContext
	}
	if path, explicit := cfg.unixSocket(); path != "" {
		if _, statErr := os.Stat(path); statErr == nil || explicit {
			nc, err := dial(ctx, "unix", path)
			if err == nil {
				return nc, "unix", nil
			}
			if explicit {
				return nil, "", connErr("connect to Unix socket "+path, err)
			}
			// A socket file with no listener (a server that was killed):
			// fall back to TCP.
		}
	}
	addr := net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port))
	nc, err := dial(ctx, "tcp", addr)
	if err != nil {
		e := connErr("connect to "+addr, err)
		e.SQLState = "08001"
		return nil, "", e
	}
	if cfg.TLS == nil || cfg.TLSUpgrade {
		return nc, "tcp", nil
	}
	tc, err := tlsHandshake(ctx, nc, cfg, true)
	if err != nil {
		nc.Close()
		return nil, "", err
	}
	return tc, "tls", nil
}

func tlsHandshake(ctx context.Context, nc net.Conn, cfg *Config, alpn bool) (*tls.Conn, error) {
	tc := cfg.TLS.Clone()
	if tc.ServerName == "" && !tc.InsecureSkipVerify {
		tc.ServerName = cfg.Host
	}
	if alpn {
		tc.NextProtos = []string{"pdb"}
	}
	conn := tls.Client(nc, tc)
	if err := conn.HandshakeContext(ctx); err != nil {
		var certErr *tls.CertificateVerificationError
		msg := "TLS handshake with " + cfg.Host + " failed"
		if errors.As(err, &certErr) {
			msg = "TLS: the server certificate was rejected"
		} else if errors.Is(err, io.EOF) {
			msg += ": the server closed the connection (no TLS on this port, or a dedicated native listener that needs sslnegotiation=upgrade)"
		}
		return nil, &Error{SQLState: "08006", Severity: "FATAL", Message: msg, cause: err, BatchIndex: -1}
	}
	if alpn && conn.ConnectionState().NegotiatedProtocol != "pdb" {
		conn.Close()
		return nil, &Error{SQLState: "08004", Severity: "FATAL", BatchIndex: -1,
			Message: "TLS endpoint did not select ALPN protocol \"pdb\": it is not a ParticleDB native-wire endpoint"}
	}
	return conn, nil
}

func (c *wireConn) handshake() error {
	caps := byte(capV1Legacy | capV2Mux | capPrepared | capNumericText | capStatementNotice)
	if c.cfg.TLS != nil && c.cfg.TLSUpgrade {
		caps |= capTLSNextProto
	}
	ext := byte(capExtTypedParams | capExtBulkIngest | capExtArrayParams | capExtBatchSQLState)
	// Only TimeZone travels in the HELLO (the server's startup parameter
	// block knows no other); the rest are SET after the handshake.
	var params [][2]string
	for k, v := range c.cfg.RuntimeParams {
		if strings.EqualFold(k, "TimeZone") {
			ext |= capExtSessionParams
			params = append(params, [2]string{"TimeZone", v})
		}
	}
	if _, err := c.nc.Write(encodeHello(caps, ext, params)); err != nil {
		return connErr("send HELLO", err)
	}
	c.br = bufio.NewReaderSize(c.nc, 64<<10)
	var h [v1HeaderSize]byte
	if _, err := io.ReadFull(c.br, h[:]); err != nil {
		return connErr("connection closed before HELLO_OK", err)
	}
	n := binary.LittleEndian.Uint32(h[:])
	if n > 1<<20 {
		return protoErr("HELLO_OK payload of %d bytes", n)
	}
	p := make([]byte, n)
	if _, err := io.ReadFull(c.br, p); err != nil {
		return connErr("connection closed before HELLO_OK", err)
	}
	switch h[6] {
	case fHelloOK:
	case fError:
		if len(p) >= 2 {
			e := serverError(binary.LittleEndian.Uint16(p), string(p[2:]))
			e.Severity = "FATAL"
			return e
		}
		return protoErr("empty ERROR in place of HELLO_OK")
	default:
		return protoErr("expected HELLO_OK, got frame type 0x%02x", h[6])
	}
	c.hello = parseHelloOK(p)
	if c.cfg.TLS != nil && c.cfg.TLSUpgrade {
		if !c.hello.tlsUpgradeOK() {
			return &Error{SQLState: "08004", Severity: "FATAL", BatchIndex: -1,
				Message: "TLS: the server did not agree to the in-band TLS upgrade; refusing to continue in plaintext"}
		}
		if c.br.Buffered() != 0 {
			return protoErr("bytes after HELLO_OK before the TLS handshake")
		}
		tc, err := tlsHandshake(context.Background(), c.nc, c.cfg, false)
		if err != nil {
			return err
		}
		c.nc = tc
		c.br = bufio.NewReaderSize(tc, 64<<10)
		c.transport = "tls"
	}
	if !c.hello.v2() {
		return &Error{SQLState: "08004", Severity: "FATAL", BatchIndex: -1,
			Message: "the server did not negotiate the multiplexed native protocol 2 (CAP_V2_MUX), which this driver requires"}
	}
	ms := c.hello.maxStreams
	if ms == 0 {
		ms = 1
	}
	if ms > 1<<16 {
		ms = 1 << 16
	}
	c.slots = make(chan struct{}, ms)
	if c.hello.authRequired() {
		if c.cfg.User == "" {
			return &Error{SQLState: "28000", Severity: "FATAL", BatchIndex: -1, Message: "the server requires authentication and no user was given"}
		}
		if len(c.cfg.User) > maxAuthFieldLen || len(c.cfg.Password) > maxAuthFieldLen {
			return &Error{SQLState: "28000", Severity: "FATAL", BatchIndex: -1, Message: "user or password longer than the server's 4096-byte limit"}
		}
		payload := encodeAuth(c.cfg.User, c.cfg.Password)
		buf := appendV2Header(nil, fAuth, flagEndStream, 1, len(payload))
		buf = append(buf, payload...)
		c.nextID = 3
		if _, err := c.nc.Write(buf); err != nil {
			return connErr("send AUTH", err)
		}
		f, err := c.readFrame()
		if err != nil {
			return connErr("connection closed during authentication", err)
		}
		switch f.typ {
		case fOK:
		case fError:
			if len(f.payload) >= 2 {
				e := serverError(binary.LittleEndian.Uint16(f.payload), string(f.payload[2:]))
				e.Severity = "FATAL"
				return e
			}
			return protoErr("empty ERROR answering AUTH")
		default:
			return protoErr("unexpected frame 0x%02x answering AUTH", f.typ)
		}
	}
	return nil
}

func (c *wireConn) applyRuntimeParams(ctx context.Context) error {
	for k, v := range c.cfg.RuntimeParams {
		if strings.EqualFold(k, "TimeZone") && c.hello.sessionParams() {
			continue
		}
		sql := "SET " + quoteIdent(k) + " TO " + quoteLiteral(v)
		if _, _, err := c.roundTrip(ctx, fQuery, 0, []byte(sql)); err != nil {
			return err
		}
	}
	return nil
}

// readFrame reads one protocol 2 frame (reader goroutine, or the handshake).
func (c *wireConn) readFrame() (frame, error) {
	h := &c.hdr
	if _, err := io.ReadFull(c.br, h[:]); err != nil {
		return frame{}, err
	}
	if h[0] != protoV2 {
		return frame{}, protoErr("frame with version %d", h[0])
	}
	n := int(binary.LittleEndian.Uint32(h[7:]))
	if n < 0 || n > maxPayload {
		return frame{}, protoErr("frame payload of %d bytes", n)
	}
	var p []byte
	if n > 0 {
		if n <= 4096 {
			if len(c.slab) < n {
				c.slab = make([]byte, 64<<10)
			}
			p = c.slab[:n:n]
			c.slab = c.slab[n:]
		} else {
			p = make([]byte, n)
		}
		if _, err := io.ReadFull(c.br, p); err != nil {
			return frame{}, err
		}
	}
	return frame{typ: h[1], flags: h[2], sid: binary.LittleEndian.Uint32(h[3:]), payload: p}, nil
}

func (c *wireConn) readLoop() {
	for {
		f, err := c.readFrame()
		if err != nil {
			var pe *Error
			if errors.As(err, &pe) {
				c.close(pe)
			} else {
				c.close(connErr("connection to the server was lost", err))
			}
			return
		}
		c.dispatch(f)
	}
}

func (c *wireConn) dispatch(f frame) {
	if f.sid == 0 {
		switch f.typ {
		case fPong:
			select {
			case c.pong <- struct{}{}:
			default:
			}
		case fGoaway:
			code := uint16(0)
			if len(f.payload) >= 6 {
				code = binary.LittleEndian.Uint16(f.payload[4:])
			}
			e := connErr(fmt.Sprintf("the server closed the connection (GOAWAY code %d)", code), nil)
			c.close(e)
		}
		return
	}
	c.smu.Lock()
	s := c.streams[f.sid]
	term := f.terminal()
	if s != nil && term {
		delete(c.streams, f.sid)
		c.nOpen.Add(-1)
	}
	c.smu.Unlock()
	if s == nil {
		return
	}
	if term {
		c.lastFlags.Store(uint32(f.flags))
		<-c.slots
	}
	s.push(f, term)
}

func (s *stream) push(f frame, term bool) {
	s.last.Store(time.Now().UnixNano())
	s.mu.Lock()
	if term {
		s.ended = true
	}
	if !s.discard {
		s.q = append(s.q, f)
		s.qbytes += len(f.payload)
		for !term && s.qbytes > maxStreamBuffer && !s.discard && s.err == nil && s.c.nOpen.Load() <= 1 {
			s.c.blocked.Store(s)
			if s.c.nOpen.Load() > 1 {
				break
			}
			s.space.Wait()
		}
		s.c.blocked.CompareAndSwap(s, nil)
	}
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// close fails every stream with err and closes the socket. Idempotent.
func (c *wireConn) close(err error) {
	c.closeOnce.Do(func() {
		c.closeErr = err
		close(c.done)
		c.nc.Close()
		c.smu.Lock()
		ss := c.streams
		c.streams = map[uint32]*stream{}
		c.smu.Unlock()
		for _, s := range ss {
			s.fail(err)
		}
	})
}

func (s *stream) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.space.Broadcast()
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (c *wireConn) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *wireConn) err() error {
	if c.closeErr != nil {
		return c.closeErr
	}
	return errClosed
}

// Close says goodbye and closes the socket.
func (c *wireConn) Close() error {
	if c.isClosed() {
		return nil
	}
	c.smu.Lock()
	last := c.nextID
	c.smu.Unlock()
	p := make([]byte, 6)
	binary.LittleEndian.PutUint32(p, last)
	_ = c.nc.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
	_ = c.writeFrame(fGoaway, 0, 0, p)
	c.close(errClosed)
	return nil
}

// writeFrame queues one frame; see the file comment for the combining rule.
// A frame appended while another goroutine flushes returns at once and is
// written by that goroutine; a write failure then closes the connection,
// which fails the frame's stream.
func (c *wireConn) writeFrame(typ, flags byte, sid uint32, payload []byte) error {
	c.wmu.Lock()
	if c.werr != nil {
		err := c.werr
		c.wmu.Unlock()
		return err
	}
	c.wbuf = appendV2Header(c.wbuf, typ, flags, sid, len(payload))
	c.wbuf = append(c.wbuf, payload...)
	if c.wflushing {
		c.wmu.Unlock()
		return nil
	}
	c.wflushing = true
	for len(c.wbuf) > 0 {
		buf := c.wbuf
		c.wbuf = c.wspare[:0]
		c.wmu.Unlock()
		_, err := c.nc.Write(buf)
		c.wmu.Lock()
		if cap(buf) <= 1<<20 {
			c.wspare = buf[:0]
		} else {
			c.wspare = nil
		}
		if err != nil {
			c.werr = connErr("write to the server failed", err)
			c.wbuf = nil
			break
		}
	}
	c.wflushing = false
	err := c.werr
	c.wmu.Unlock()
	if err != nil {
		c.close(err)
	}
	return err
}

// errNotSent marks a failure that happened before any byte of the request
// left the client, so the request may safely be retried on another
// connection.
type errNotSent struct{ err error }

func (e *errNotSent) Error() string { return e.err.Error() }
func (e *errNotSent) Unwrap() error { return e.err }

// start opens a stream and sends its request frame.
func (c *wireConn) start(ctx context.Context, typ, flags byte, payload []byte) (*stream, error) {
	select {
	case c.slots <- struct{}{}:
	default:
		select {
		case c.slots <- struct{}{}:
		case <-ctx.Done():
			return nil, &errNotSent{canceledErr(ctx.Err(), "")}
		case <-c.done:
			return nil, &errNotSent{c.err()}
		}
	}
	s := &stream{c: c, notify: make(chan struct{}, 1)}
	s.space.L = &s.mu
	s.q = s.qbuf[:0]
	c.smu.Lock()
	if c.isClosed() {
		c.smu.Unlock()
		<-c.slots
		return nil, &errNotSent{c.err()}
	}
	s.id = c.nextID
	c.nextID += 2
	c.streams[s.id] = s
	c.nOpen.Add(1)
	c.smu.Unlock()
	if b := c.blocked.Load(); b != nil && b != s {
		b.mu.Lock()
		b.space.Broadcast()
		b.mu.Unlock()
	}
	if err := c.writeFrame(typ, flags|flagEndStream, s.id, payload); err != nil {
		return nil, err
	}
	return s, nil
}

// next returns the stream's next frame.
func (s *stream) next(ctx context.Context) (frame, error) {
	for {
		s.mu.Lock()
		if s.head < len(s.q) {
			f := s.q[s.head]
			s.q[s.head] = frame{}
			s.head++
			s.qbytes -= len(f.payload)
			if s.head == len(s.q) {
				s.q = s.q[:0]
				s.head = 0
			}
			s.space.Signal()
			s.mu.Unlock()
			return f, nil
		}
		if s.err != nil {
			err := s.err
			s.mu.Unlock()
			return frame{}, err
		}
		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-ctx.Done():
			return frame{}, ctx.Err()
		}
	}
}

// resetStream sends RST_STREAM(CANCEL); the server ends a running statement
// with 57014 on the stream.
func (s *stream) resetStream() {
	s.mu.Lock()
	if s.reset || s.ended {
		s.mu.Unlock()
		return
	}
	s.reset = true
	s.mu.Unlock()
	p := []byte{byte(errCancel), byte(errCancel >> 8)}
	_ = s.c.writeFrame(fRstStreamC, 0, s.id, p)
}

// abandon drops the rest of the stream: it is reset and its frames are
// discarded as they arrive. A stream that never ends within the cancel grace
// closes the connection, because the server then still counts it.
func (s *stream) abandon() {
	s.mu.Lock()
	if s.ended || s.err != nil {
		s.discard = true
		s.q = nil
		s.mu.Unlock()
		return
	}
	s.discard = true
	s.q = nil
	s.qbytes = 0
	s.space.Broadcast()
	s.mu.Unlock()
	s.resetStream()
	s.last.Store(time.Now().UnixNano())
	s.watchAbandoned()
}

// watchAbandoned closes the connection when an abandoned stream stops making
// progress (no frame for the cancel grace) without ending: the server then
// still counts it against the stream cap. A stream that keeps draining (a
// large result already in flight) is left to finish.
func (s *stream) watchAbandoned() {
	c := s.c
	grace := c.cfg.cancelGrace()
	time.AfterFunc(grace, func() {
		s.mu.Lock()
		ended := s.ended || s.err != nil
		s.mu.Unlock()
		if ended {
			return
		}
		if time.Since(time.Unix(0, s.last.Load())) < grace {
			s.watchAbandoned()
			return
		}
		c.close(connErr("an abandoned stream got no answer after its reset; closing the connection", nil))
	})
}

// await waits for the terminal frame of s, collecting notices. When ctx ends
// first the stream is reset and the server's answer (57014, or the result if
// the statement had already finished) is awaited for the cancel grace; past
// it the connection is closed because the outcome is unknown.
func (s *stream) await(ctx context.Context, notices *[]Notice) (frame, error) {
	for {
		f, err := s.next(ctx)
		if err != nil {
			if ctx.Err() != nil && err == ctx.Err() {
				return s.afterCancel(ctx.Err(), notices)
			}
			return frame{}, err
		}
		if f.typ == fNotice {
			if notices != nil {
				*notices = append(*notices, decodeNotices(f.payload)...)
			}
			continue
		}
		return f, nil
	}
}

func (s *stream) afterCancel(ctxErr error, notices *[]Notice) (frame, error) {
	s.resetStream()
	gctx, cancel := context.WithTimeout(context.Background(), s.c.cfg.cancelGrace())
	defer cancel()
	for {
		f, err := s.next(gctx)
		if err != nil {
			if gctx.Err() != nil {
				e := connErr("statement canceled and the server did not answer within the cancel grace; its outcome is unknown, so the connection was closed", ctxErr)
				s.c.close(e)
				return frame{}, e
			}
			return frame{}, err
		}
		switch f.typ {
		case fNotice:
			if notices != nil {
				*notices = append(*notices, decodeNotices(f.payload)...)
			}
			continue
		case fError:
			e := frameError(f)
			if e.SQLState == "57014" {
				return frame{}, canceledErr(ctxErr, e.Message)
			}
			return frame{}, e
		case fRstStreamS:
			return frame{}, canceledErr(ctxErr, "")
		}
		// The statement had finished before the cancel landed: its real
		// outcome. A streamed result is drained; the caller asked to stop.
		if f.typ == fArrowStream && !f.terminal() {
			s.abandon()
			return frame{}, canceledErr(ctxErr, "")
		}
		return f, nil
	}
}

func frameError(f frame) *Error {
	if len(f.payload) < 2 {
		return serverError(1064, "")
	}
	return serverError(binary.LittleEndian.Uint16(f.payload), string(f.payload[2:]))
}

// roundTrip sends one request and returns its terminal frame. ERROR frames
// come back as *Error. A request the server refused for the stream limit
// never ran and is sent again.
func (c *wireConn) roundTrip(ctx context.Context, typ, flags byte, payload []byte) (frame, []Notice, error) {
	var notices []Notice
	for attempt := 0; ; attempt++ {
		s, err := c.start(ctx, typ, flags, payload)
		if err != nil {
			return frame{}, nil, err
		}
		f, err := s.await(ctx, &notices)
		if err != nil {
			return frame{}, notices, err
		}
		switch f.typ {
		case fError:
			return f, notices, frameError(f)
		case fRstStreamS:
			code := uint16(0)
			if len(f.payload) >= 2 {
				code = binary.LittleEndian.Uint16(f.payload)
			}
			if code == errStreamLimit && attempt < 100 {
				if err := sleepCtx(ctx, time.Duration(attempt+1)*time.Millisecond); err != nil {
					return frame{}, notices, canceledErr(err, "")
				}
				continue
			}
			e := serverError(code, fmt.Sprintf("stream reset by the server (code %d)", code))
			return f, notices, e
		}
		return f, notices, nil
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ping sends PING on the control stream and waits for PONG.
func (c *wireConn) ping(ctx context.Context) error {
	c.pingMu.Lock()
	defer c.pingMu.Unlock()
	select {
	case <-c.pong:
	default:
	}
	if err := c.writeFrame(fPing, flagEndStream, 0, nil); err != nil {
		return err
	}
	select {
	case <-c.pong:
		return nil
	case <-c.done:
		return c.err()
	case <-ctx.Done():
		return canceledErr(ctx.Err(), "")
	}
}

// txnStatus reports the transaction state of the session after the latest
// answered request (only meaningful with CAP_PREPARED).
func (c *wireConn) txnStatus() (open, failed bool) {
	f := c.lastFlags.Load()
	return f&flagTxnOpen != 0, f&flagTxnFailed != 0
}
