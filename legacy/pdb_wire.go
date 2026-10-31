// Package particledb provides a PDB native wire protocol client for Go.
//
// Uses PDB's binary protocol for maximum performance.
// Automatically uses Unix sockets on localhost, TCP for remote hosts.
//
//	db := particledb.NewPdbWireClient("localhost", 5440)
//	defer db.Close()
//	rows, _ := db.Query(ctx, "SELECT * FROM users")
//	db.Execute(ctx, "INSERT INTO users VALUES (1, 'alice')")
//	db.Batch(ctx, []string{"BEGIN", "UPDATE ...", "COMMIT"})
//
//	err := db.Transaction(ctx, func(tx *Tx) error {
//	    tx.Execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)")
//	    tx.Execute("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
//	    return nil
//	})
package legacy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Protocol constants
// ---------------------------------------------------------------------------

const (
	wireHeaderSize = 8

	// Client → Server
	msgWireHello = byte(0x00)
	msgWireQuery = byte(0x01)
	msgWireBatch = byte(0x02)
	msgWirePing  = byte(0x03)
	msgWireClose = byte(0x04)

	// Server → Client
	msgWireHelloOK     = byte(0x80)
	msgWireArrowResult = byte(0x81)
	msgWireOK          = byte(0x82)
	msgWireError       = byte(0x83)
	msgWirePong        = byte(0x84)
	msgWireBatchResult = byte(0x85)
	msgWireArrowStream = byte(0x86)

	// HELLO payload
	protoVersion   = byte(1)
	fmtArrowIPC    = byte(0x01)
	flagMoreChunks = byte(0x02)
	// flagTxnRolledBack is set on the OK / BATCH_RESULT answering a COMMIT
	// that ended a FAILED transaction block by rolling it back
	// (the server's FLAG_TXN_ROLLED_BACK): nothing was committed.
	flagTxnRolledBack = byte(0x40)

	// WireDefaultPort is the default PDB native wire port.
	WireDefaultPort = 5440

	defaultWirePoolSize     = 0 // 0 means auto (NumCPU, capped at 16)
	defaultWireTimeoutMs    = 30_000
	defaultWireMaxRetries   = 3
	defaultWireRetryDelayMs = 100
)

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

// PdbError is the base error type for all PDB wire protocol errors.
type PdbError struct {
	Code    uint16
	Message string
}

func (e *PdbError) Error() string {
	return fmt.Sprintf("pdb error (code=%d): %s", e.Code, e.Message)
}

// PdbConnectionError is returned when a network error prevents reaching the server.
type PdbConnectionError struct {
	Cause error
}

func (e *PdbConnectionError) Error() string {
	if e.Cause != nil {
		return "pdb connection error: " + e.Cause.Error()
	}
	return "pdb connection error"
}

func (e *PdbConnectionError) Unwrap() error { return e.Cause }

// PdbQueryError is returned when the server reports a SQL-level error.
type PdbQueryError struct {
	Code    uint16
	Message string
}

func (e *PdbQueryError) Error() string {
	return fmt.Sprintf("pdb query error (code=%d): %s", e.Code, e.Message)
}

// DiskFullError is returned when the server signals it has run out of disk space.
type DiskFullError struct {
	Message string
}

func (e *DiskFullError) Error() string { return "pdb disk full: " + e.Message }

// makePdbError maps error codes to specific error types.
func makePdbError(code uint16, msg string) error {
	if code == 28 {
		return &DiskFullError{Message: msg}
	}
	return &PdbQueryError{Code: code, Message: msg}
}

// ---------------------------------------------------------------------------
// ConnectionOptions
// ---------------------------------------------------------------------------

// ConnectionOptions configures the PDB wire client.
type ConnectionOptions struct {
	Host         string // Hostname or IP. Default: "localhost"
	Port         int    // Port number. Default: 5440
	PoolSize     int    // Max pooled connections. Default: min(NumCPU, 16)
	TimeoutMs    int    // Socket timeout in milliseconds. Default: 30000
	Transport    string // "auto" | "pdb_wire" | "pg" | "mysql". Default: "auto"
	MaxRetries   int    // Retries of a failure to OPEN a connection (dial, HELLO); a request already being sent is never retried. Default: 3
	RetryDelayMs int    // Base retry delay in milliseconds. Default: 100
	User         string // Sent with MSG_AUTH after HELLO when Password is non-empty.
	Password     string // Empty: no MSG_AUTH (a --no-auth server).
	// A local Host (localhost, 127.x, ::1) connects over the server's Unix
	// socket <UnixSocketDir>/.s.PDB.<Port> when that file exists, else TCP.
	// DisableUnixSocket always uses TCP.
	DisableUnixSocket bool
	// UnixSocketDir is the server's --unix-socket-dir. Default: "/tmp".
	UnixSocketDir string
}

// DefaultUnixSocketDir is the server's default --unix-socket-dir.
const DefaultUnixSocketDir = "/tmp"

// UnixSocketPath is the server's native-wire socket for port:
// <dir>/.s.PDB.<port> (dir "" = DefaultUnixSocketDir).
func UnixSocketPath(dir string, port int) string {
	if dir == "" {
		dir = DefaultUnixSocketDir
	}
	return filepath.Join(dir, fmt.Sprintf(".s.PDB.%d", port))
}

// IsLocalHost reports whether host is "localhost" or a loopback IP literal
// (127.0.0.0/8, ::1). No DNS lookup: only these get the Unix socket first.
func IsLocalHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

func defaultConnectionOptions() ConnectionOptions {
	poolSize := runtime.NumCPU()
	if poolSize > 16 {
		poolSize = 16
	}
	return ConnectionOptions{
		Host:         "localhost",
		Port:         WireDefaultPort,
		PoolSize:     poolSize,
		TimeoutMs:    defaultWireTimeoutMs,
		Transport:    "auto",
		MaxRetries:   defaultWireMaxRetries,
		RetryDelayMs: defaultWireRetryDelayMs,
	}
}

// HealthResult holds the result of a health check.
type HealthResult struct {
	Connected     bool
	LatencyMs     float64
	Protocol      string
	ServerVersion int
	PoolActive    int
	PoolIdle      int
}

// Row is a convenience alias for a single result row.
type Row = map[string]interface{}

// ---------------------------------------------------------------------------
// Internal metrics counters (atomic, no external dependency)
// ---------------------------------------------------------------------------

// wireMetrics holds all 12 client-side metrics using atomic counters.
type wireMetrics struct {
	queriesTotal       atomic.Int64 // total queries (ok)
	queriesError       atomic.Int64 // total queries (error)
	executesTotal      atomic.Int64
	executesError      atomic.Int64
	batchesTotal       atomic.Int64
	batchesError       atomic.Int64
	connectionsCreated atomic.Int64
	connectionsClosed  atomic.Int64
	connectionErrors   atomic.Int64
	retriesTotal       atomic.Int64
	bytesSent          atomic.Int64
	bytesReceived      atomic.Int64

	// For tracking pool state (gauge-like, updated with Add/Add(-1))
	connectionsActive atomic.Int64
	connectionsIdle   atomic.Int64

	// Query latency histogram (simplified: sum + count)
	queryDurationSum   atomic.Int64 // microseconds
	queryDurationCount atomic.Int64
}

// Metrics returns a Prometheus-compatible text exposition of all client metrics.
func (m *wireMetrics) generate() string {
	return fmt.Sprintf(`# HELP pdb_client_queries_total Total queries executed successfully
# TYPE pdb_client_queries_total counter
pdb_client_queries_total{type="query",status="ok"} %d
pdb_client_queries_total{type="query",status="error"} %d
pdb_client_queries_total{type="execute",status="ok"} %d
pdb_client_queries_total{type="execute",status="error"} %d
pdb_client_queries_total{type="batch",status="ok"} %d
pdb_client_queries_total{type="batch",status="error"} %d
# HELP pdb_client_connections_created_total Total connections created
# TYPE pdb_client_connections_created_total counter
pdb_client_connections_created_total %d
# HELP pdb_client_connections_closed_total Total connections closed
# TYPE pdb_client_connections_closed_total counter
pdb_client_connections_closed_total %d
# HELP pdb_client_connection_errors_total Total connection errors
# TYPE pdb_client_connection_errors_total counter
pdb_client_connection_errors_total %d
# HELP pdb_client_retries_total Total retry attempts
# TYPE pdb_client_retries_total counter
pdb_client_retries_total %d
# HELP pdb_client_bytes_sent_total Bytes sent to server
# TYPE pdb_client_bytes_sent_total counter
pdb_client_bytes_sent_total %d
# HELP pdb_client_bytes_received_total Bytes received from server
# TYPE pdb_client_bytes_received_total counter
pdb_client_bytes_received_total %d
# HELP pdb_client_connections_active Connections currently in use
# TYPE pdb_client_connections_active gauge
pdb_client_connections_active %d
# HELP pdb_client_connections_idle Connections idle in pool
# TYPE pdb_client_connections_idle gauge
pdb_client_connections_idle %d
# HELP pdb_client_query_duration_seconds_sum Sum of query durations in seconds
# TYPE pdb_client_query_duration_seconds_sum untyped
pdb_client_query_duration_seconds_sum %g
# HELP pdb_client_query_duration_seconds_count Number of query duration observations
# TYPE pdb_client_query_duration_seconds_count counter
pdb_client_query_duration_seconds_count %d
`,
		m.queriesTotal.Load(), m.queriesError.Load(),
		m.executesTotal.Load(), m.executesError.Load(),
		m.batchesTotal.Load(), m.batchesError.Load(),
		m.connectionsCreated.Load(),
		m.connectionsClosed.Load(),
		m.connectionErrors.Load(),
		m.retriesTotal.Load(),
		m.bytesSent.Load(),
		m.bytesReceived.Load(),
		m.connectionsActive.Load(),
		m.connectionsIdle.Load(),
		float64(m.queryDurationSum.Load())/1e6,
		m.queryDurationCount.Load(),
	)
}

// ---------------------------------------------------------------------------
// Single raw connection
// ---------------------------------------------------------------------------

type wireConn struct {
	c        net.Conn
	protocol string // "unix" or "tcp"
	streamID uint16
	metrics  *wireMetrics // byte counters; nil for a bare connection
}

// dialWire opens one connection: for a local host (unless disabled) the
// server's socket first when the file exists, falling back to TCP when the
// file is absent or the socket does not answer (a stale file); else TCP.
func dialWire(host string, port int, timeoutMs int, disableUnix bool, socketDir string) (*wireConn, error) {
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if !disableUnix && runtime.GOOS != "windows" && IsLocalHost(host) {
		path := UnixSocketPath(socketDir, port)
		if _, err := os.Stat(path); err == nil {
			d := net.Dialer{Timeout: timeout}
			c, err := d.Dial("unix", path)
			if err == nil {
				return &wireConn{c: c, protocol: "unix"}, nil
			}
		}
	}
	d := net.Dialer{Timeout: timeout}
	c, err := d.Dial("tcp", net.JoinHostPort(strings.Trim(host, "[]"), strconv.Itoa(port)))
	if err != nil {
		return nil, &PdbConnectionError{Cause: err}
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &wireConn{c: c, protocol: "tcp"}, nil
}

func (wc *wireConn) setTimeout(d time.Duration) {
	if d > 0 {
		_ = wc.c.SetDeadline(time.Now().Add(d))
	}
}

func (wc *wireConn) clearTimeout() {
	_ = wc.c.SetDeadline(time.Time{})
}

func (wc *wireConn) nextStreamID() uint16 {
	wc.streamID++
	return wc.streamID
}

// buildHeader encodes the 8-byte frame header: [len u32 LE][stream_id u16 LE][msg_type u8][flags u8]
func buildHeader(payloadLen int, streamID uint16, msgType, flags byte) []byte {
	hdr := make([]byte, wireHeaderSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(payloadLen))
	binary.LittleEndian.PutUint16(hdr[4:6], streamID)
	hdr[6] = msgType
	hdr[7] = flags
	return hdr
}

func (wc *wireConn) sendFrame(msgType byte, payload []byte) error {
	sid := wc.nextStreamID()
	hdr := buildHeader(len(payload), sid, msgType, 0)
	frame := make([]byte, 0, wireHeaderSize+len(payload))
	frame = append(frame, hdr...)
	frame = append(frame, payload...)
	n, err := wc.c.Write(frame)
	if wc.metrics != nil {
		wc.metrics.bytesSent.Add(int64(n))
	}
	return err
}

type wireFrame struct {
	MsgType byte
	Flags   byte
	Data    []byte
}

func (wc *wireConn) readFrame() (wireFrame, error) {
	hdr := make([]byte, wireHeaderSize)
	if _, err := io.ReadFull(wc.c, hdr); err != nil {
		return wireFrame{}, &PdbConnectionError{Cause: err}
	}
	plen := binary.LittleEndian.Uint32(hdr[0:4])
	// hdr[4:6] is stream_id (ignored on client)
	msgType := hdr[6]
	flags := hdr[7]
	if wc.metrics != nil {
		wc.metrics.bytesReceived.Add(int64(wireHeaderSize) + int64(plen))
	}

	var data []byte
	if plen > 0 {
		data = make([]byte, plen)
		if _, err := io.ReadFull(wc.c, data); err != nil {
			return wireFrame{}, &PdbConnectionError{Cause: err}
		}
	}

	if msgType == msgWireError {
		var code uint16
		msg := "unknown error"
		if len(data) >= 2 {
			code = binary.LittleEndian.Uint16(data[0:2])
			if len(data) > 2 {
				msg = string(data[2:])
			}
		}
		return wireFrame{}, makePdbError(code, msg)
	}

	return wireFrame{MsgType: msgType, Flags: flags, Data: data}, nil
}

// doHello performs the HELLO handshake after connecting.
func (wc *wireConn) doHello() (byte, error) {
	// payload: [version u8, num_formats u8, FMT_ARROW_IPC u8]
	payload := []byte{protoVersion, 1, fmtArrowIPC}
	if err := wc.sendFrame(msgWireHello, payload); err != nil {
		return 0, &PdbConnectionError{Cause: err}
	}
	f, err := wc.readFrame()
	if err != nil {
		return 0, err
	}
	if f.MsgType != msgWireHelloOK {
		return 0, &PdbConnectionError{Cause: fmt.Errorf("unexpected HELLO response: 0x%02x", f.MsgType)}
	}
	var serverVersion byte
	if len(f.Data) >= 1 {
		serverVersion = f.Data[0]
	}
	return serverVersion, nil
}

// sendQuery sends a QUERY frame and returns the collected response frames.
func (wc *wireConn) sendQuery(sql string) ([]wireFrame, error) {
	if err := wc.sendFrame(msgWireQuery, []byte(sql)); err != nil {
		return nil, &PdbConnectionError{Cause: err}
	}
	return wc.collectResponse()
}

// sendBatch sends a BATCH frame and returns the collected response frames.
func (wc *wireConn) sendBatch(sqls []string) ([]wireFrame, error) {
	// payload: [count u32 LE][len0 u32 LE][sql0 bytes]...
	size := 4
	for _, s := range sqls {
		size += 4 + len(s)
	}
	payload := make([]byte, 0, size)
	tmp := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, uint32(len(sqls)))
	payload = append(payload, tmp...)
	for _, s := range sqls {
		binary.LittleEndian.PutUint32(tmp, uint32(len(s)))
		payload = append(payload, tmp...)
		payload = append(payload, s...)
	}
	if err := wc.sendFrame(msgWireBatch, payload); err != nil {
		return nil, &PdbConnectionError{Cause: err}
	}
	return wc.collectResponse()
}

// collectResponse reads one or more frames, reassembling streamed Arrow responses.
func (wc *wireConn) collectResponse() ([]wireFrame, error) {
	f, err := wc.readFrame()
	if err != nil {
		return nil, err
	}
	if f.MsgType == msgWireArrowStream {
		// Collect stream chunks: read headers directly to access flags
		frames := []wireFrame{f}
		if f.Flags&flagMoreChunks != 0 {
			more, err := wc.collectStreamChunks()
			if err != nil {
				return nil, err
			}
			frames = append(frames, more...)
		}
		return frames, nil
	}
	return []wireFrame{f}, nil
}

// collectStreamChunks reads ARROW_STREAM frames until FLAG_MORE_CHUNKS is clear.
func (wc *wireConn) collectStreamChunks() ([]wireFrame, error) {
	var frames []wireFrame
	for {
		f, err := wc.readFrame()
		if err != nil {
			return nil, err
		}
		frames = append(frames, f)
		if f.Flags&flagMoreChunks == 0 {
			return frames, nil
		}
	}
}

func (wc *wireConn) doPing() (float64, error) {
	t0 := time.Now()
	if err := wc.sendFrame(msgWirePing, nil); err != nil {
		return 0, &PdbConnectionError{Cause: err}
	}
	f, err := wc.readFrame()
	if err != nil {
		return 0, err
	}
	if f.MsgType != msgWirePong {
		return 0, &PdbConnectionError{Cause: fmt.Errorf("expected PONG, got 0x%02x", f.MsgType)}
	}
	return float64(time.Since(t0).Microseconds()) / 1000.0, nil
}

func (wc *wireConn) doClose() {
	// Set a short deadline so the CLOSE frame write does not block if nobody
	// is reading (e.g. server already exited or connection is half-closed).
	_ = wc.c.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_ = wc.sendFrame(msgWireClose, nil)
	_ = wc.c.SetWriteDeadline(time.Time{})
	_ = wc.c.Close()
}

// ---------------------------------------------------------------------------
// Pooled connection entry
// ---------------------------------------------------------------------------

type pooledConn struct {
	wc            *wireConn
	serverVersion byte
	stmts         stmtCache // prepared statements on this connection (pdb_wire_prepared.go)
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

type wirePool struct {
	opts    ConnectionOptions
	metrics *wireMetrics

	mu    sync.Mutex
	idle  []*pooledConn
	total int // total connections created (active + idle)
}

func newWirePool(opts ConnectionOptions, m *wireMetrics) *wirePool {
	return &wirePool{opts: opts, metrics: m}
}

// acquire returns an idle connection or dials a new one.
// Blocks if the pool is full until a connection becomes available.
func (p *wirePool) acquire(ctx context.Context) (*pooledConn, error) {
	for {
		// Check context cancellation.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		p.mu.Lock()
		// Reuse an idle connection.
		if len(p.idle) > 0 {
			pc := p.idle[len(p.idle)-1]
			p.idle = p.idle[:len(p.idle)-1]
			p.mu.Unlock()
			pc.wc.metrics = p.metrics
			p.metrics.connectionsIdle.Add(-1)
			p.metrics.connectionsActive.Add(1)
			return pc, nil
		}
		// Create new connection if under limit.
		if p.total < p.opts.PoolSize {
			p.total++
			p.mu.Unlock()
			pc, err := p.dial()
			if err != nil {
				p.mu.Lock()
				p.total--
				p.mu.Unlock()
				// Nothing of the caller's request was written: retryable.
				return nil, &connectFailedError{err: err}
			}
			p.metrics.connectionsCreated.Add(1)
			p.metrics.connectionsActive.Add(1)
			return pc, nil
		}
		p.mu.Unlock()
		// Pool is full - yield briefly and retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// connectFailedError marks a failure to open a NEW connection (dial or HELLO).
// No byte of any request was written on it, so the request that needed it may
// be retried; it is unwrapped before it reaches the caller.
type connectFailedError struct{ err error }

func (e *connectFailedError) Error() string { return e.err.Error() }
func (e *connectFailedError) Unwrap() error { return e.err }

// release returns the connection to the pool, or closes it on error.
func (p *wirePool) release(pc *pooledConn, discard bool) {
	p.metrics.connectionsActive.Add(-1)
	if discard {
		pc.wc.doClose()
		p.metrics.connectionsClosed.Add(1)
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
		return
	}
	p.mu.Lock()
	if len(p.idle) < p.opts.PoolSize {
		p.idle = append(p.idle, pc)
		p.mu.Unlock()
		p.metrics.connectionsIdle.Add(1)
		return
	}
	p.mu.Unlock()
	pc.wc.doClose()
	p.metrics.connectionsClosed.Add(1)
	p.mu.Lock()
	p.total--
	p.mu.Unlock()
}

// closeAll closes all idle connections.
func (p *wirePool) closeAll() {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, pc := range idle {
		pc.wc.doClose()
		p.metrics.connectionsClosed.Add(1)
	}
}

// forEachIdle takes the idle connections out of the pool, calls fn on each
// (keep=false discards that connection) and returns the kept ones.
func (p *wirePool) forEachIdle(fn func(pc *pooledConn) (keep bool)) {
	p.mu.Lock()
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, pc := range idle {
		p.metrics.connectionsIdle.Add(-1)
		p.metrics.connectionsActive.Add(1)
		p.release(pc, !fn(pc))
	}
}

func (p *wirePool) idleCount() int {
	p.mu.Lock()
	n := len(p.idle)
	p.mu.Unlock()
	return n
}

func (p *wirePool) activeCount() int {
	return int(p.metrics.connectionsActive.Load())
}

func (p *wirePool) dial() (*pooledConn, error) {
	wc, err := dialWire(p.opts.Host, p.opts.Port, p.opts.TimeoutMs, p.opts.DisableUnixSocket, p.opts.UnixSocketDir)
	if err != nil {
		p.metrics.connectionErrors.Add(1)
		return nil, err
	}
	wc.metrics = p.metrics
	sv, err := wc.doHello()
	if err != nil {
		wc.doClose()
		p.metrics.connectionErrors.Add(1)
		return nil, err
	}
	if p.opts.Password != "" {
		if err := wc.doAuth(p.opts.User, p.opts.Password); err != nil {
			wc.doClose()
			p.metrics.connectionErrors.Add(1)
			return nil, err
		}
	}
	return &pooledConn{wc: wc, serverVersion: sv}, nil
}

// BATCH_RESULT entry types.
const (
	batchEntryArrow = byte(0x81)
	batchEntryOK    = byte(0x82)
	batchEntryError = byte(0x83)
)

// decodeBatchResult validates the reply to a batch of `sent` statements.
//
// Payload: [u32 LE count] then `count` entries of [u8 type][body]: OK = u64 LE
// affected rows, ARROW = u32 LE len + Arrow IPC, ERROR = u32 LE len + UTF-8
// message. The server stops at the first failing statement, so the failure is
// the LAST entry and count < sent.
//
// Returns *PdbQueryError (server message, code 0: entries carry no code) for an
// ERROR entry, and *PdbError for a reply that does not account for every
// statement sent.
func decodeBatchResult(frames []wireFrame, sent int) error {
	if len(frames) != 1 || frames[0].MsgType != msgWireBatchResult {
		got := "no frame"
		if len(frames) > 0 {
			got = fmt.Sprintf("0x%02x", frames[0].MsgType)
		}
		return &PdbError{Message: "protocol error: expected BATCH_RESULT (0x85) for a batch, got " + got}
	}
	d := frames[0].Data
	if len(d) < 4 {
		return &PdbError{Message: fmt.Sprintf("protocol error: BATCH_RESULT payload too short (%d bytes)", len(d))}
	}
	count := int(binary.LittleEndian.Uint32(d[0:4]))
	off := 4
	truncated := func(i int) error {
		return &PdbError{Message: fmt.Sprintf("protocol error: BATCH_RESULT truncated at entry %d", i)}
	}
	for i := 0; i < count; i++ {
		if off+1 > len(d) {
			return truncated(i)
		}
		kind := d[off]
		off++
		switch kind {
		case batchEntryOK:
			if off+8 > len(d) {
				return truncated(i)
			}
			off += 8
		case batchEntryArrow, batchEntryError:
			if off+4 > len(d) {
				return truncated(i)
			}
			n := int(binary.LittleEndian.Uint32(d[off : off+4]))
			off += 4
			if n < 0 || off+n > len(d) {
				return truncated(i)
			}
			if kind == batchEntryError {
				return &PdbQueryError{Message: string(d[off : off+n])}
			}
			off += n
		default:
			return &PdbError{Message: fmt.Sprintf("protocol error: unknown BATCH_RESULT entry type 0x%02x", kind)}
		}
	}
	if count != sent {
		return &PdbError{Message: fmt.Sprintf(
			"protocol error: BATCH_RESULT has %d entries for %d statements and no error entry", count, sent)}
	}
	return nil
}

// rollback sends ROLLBACK on this connection and reports whether the server
// acknowledged it, i.e. whether the connection is safe to reuse.
func (wc *wireConn) rollback() bool {
	frames, err := wc.sendBatch([]string{"ROLLBACK"})
	return err == nil && decodeBatchResult(frames, 1) == nil
}

// decodeOK extracts the affected-row count from an MSG_OK or MSG_BATCH_RESULT frame.
func decodeOK(f wireFrame) int64 {
	if len(f.Data) >= 8 {
		return int64(binary.LittleEndian.Uint64(f.Data[0:8]))
	}
	return 0
}

// ---------------------------------------------------------------------------
// WireTx - transaction helper for use inside Transaction()
// ---------------------------------------------------------------------------

// WireTx accumulates SQL statements for a single auto-batch transaction.
// All Execute calls are buffered; they are sent to the server atomically on commit.
type WireTx struct {
	stmts []string
}

// Execute enqueues a SQL statement within this transaction.
func (tx *WireTx) Execute(sql string) {
	tx.stmts = append(tx.stmts, sql)
}

// ---------------------------------------------------------------------------
// PdbWireClient - public API
// ---------------------------------------------------------------------------

// PdbWireClient is a pooled PDB native wire protocol client.
//
// Automatically uses Unix sockets when connecting to localhost, falling
// back to TCP for remote hosts. All methods accept context.Context for
// timeout and cancellation.
type PdbWireClient struct {
	opts    ConnectionOptions
	pool    *wirePool
	metrics *wireMetrics
	closed  atomic.Bool
}

// NewPdbWireClient creates a client with default options targeting localhost:5440.
func NewPdbWireClient(host string, port int) *PdbWireClient {
	opts := defaultConnectionOptions()
	opts.Host = host
	opts.Port = port
	return NewPdbWireClientWithOptions(opts)
}

// NewPdbWireClientWithOptions creates a client with explicit options.
func NewPdbWireClientWithOptions(opts ConnectionOptions) *PdbWireClient {
	if opts.Host == "" {
		opts.Host = "localhost"
	}
	if opts.Port == 0 {
		opts.Port = WireDefaultPort
	}
	if opts.PoolSize <= 0 {
		n := runtime.NumCPU()
		if n > 16 {
			n = 16
		}
		opts.PoolSize = n
	}
	if opts.TimeoutMs <= 0 {
		opts.TimeoutMs = defaultWireTimeoutMs
	}
	if opts.MaxRetries < 0 {
		opts.MaxRetries = defaultWireMaxRetries
	}
	if opts.RetryDelayMs <= 0 {
		opts.RetryDelayMs = defaultWireRetryDelayMs
	}
	if opts.Transport == "" {
		opts.Transport = "auto"
	}

	m := &wireMetrics{}
	return &PdbWireClient{
		opts:    opts,
		pool:    newWirePool(opts, m),
		metrics: m,
	}
}

// Query executes a statement and returns its rows as []map[string]interface{}
// (decoded from the Arrow IPC result; see arrow_ipc.go for the Go types).
// With args the statement is sent with MSG_PREPARE (cached per connection) and
// MSG_EXECUTE, the arguments ($1..$N) as a typed Arrow parameter batch; values
// are never interpolated into the SQL text. Without args it is a plain QUERY.
func (c *PdbWireClient) Query(ctx context.Context, sql string, args ...interface{}) ([]Row, error) {
	res, err := c.queryResult(ctx, sql, args, false)
	if err != nil {
		return nil, err
	}
	return res.rows, nil
}

// QueryColumns is Query that also returns the column names in result order.
func (c *PdbWireClient) QueryColumns(ctx context.Context, sql string, args ...interface{}) ([]string, []Row, error) {
	res, err := c.queryResult(ctx, sql, args, false)
	if err != nil {
		return nil, nil, err
	}
	return res.columns, res.rows, nil
}

// Execute runs a DML or DDL statement and returns the number of affected rows
// (0 for a row-returning statement). Args are bound as in Query.
func (c *PdbWireClient) Execute(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	return c.executeAffected(ctx, sql, args, false)
}

// Batch executes multiple SQL statements in a single round-trip.
func (c *PdbWireClient) Batch(ctx context.Context, sqls []string) error {
	if len(sqls) == 0 {
		return nil
	}
	t0 := time.Now()
	_, err := c.sendBatchWithRetry(ctx, sqls)
	elapsed := time.Since(t0)
	c.metrics.queryDurationSum.Add(elapsed.Microseconds())
	c.metrics.queryDurationCount.Add(1)
	if err != nil {
		c.metrics.batchesError.Add(1)
		return err
	}
	c.metrics.batchesTotal.Add(1)
	return nil
}

// Transaction executes fn within an auto-batch transaction.
// Statements enqueued via tx.Execute are sent atomically as a single
// batch (BEGIN … stmts … COMMIT). If fn returns a non-nil error, a
// ROLLBACK is sent instead.
func (c *PdbWireClient) Transaction(ctx context.Context, fn func(*WireTx) error) error {
	tx := &WireTx{stmts: []string{"BEGIN"}}
	if err := fn(tx); err != nil {
		rb := append(tx.stmts, "ROLLBACK")
		// Best-effort rollback - ignore error.
		_ = c.Batch(ctx, rb)
		return err
	}
	tx.stmts = append(tx.stmts, "COMMIT")
	return c.Batch(ctx, tx.stmts)
}

// Ping verifies server reachability. Returns nil on success.
func (c *PdbWireClient) Ping(ctx context.Context) error {
	pc, err := c.pool.acquire(ctx)
	if err != nil {
		return unwrapConnectFailed(err)
	}
	if dl, ok := ctx.Deadline(); ok {
		pc.wc.setTimeout(time.Until(dl))
	}
	_, pingErr := pc.wc.doPing()
	pc.wc.clearTimeout()
	if pingErr != nil {
		c.pool.release(pc, true)
		return pingErr
	}
	c.pool.release(pc, false)
	return nil
}

// Health performs a ping and returns a HealthResult with latency and pool stats.
func (c *PdbWireClient) Health(ctx context.Context) (*HealthResult, error) {
	pc, err := c.pool.acquire(ctx)
	if err != nil {
		return &HealthResult{Connected: false}, unwrapConnectFailed(err)
	}
	if dl, ok := ctx.Deadline(); ok {
		pc.wc.setTimeout(time.Until(dl))
	}
	latencyMs, pingErr := pc.wc.doPing()
	pc.wc.clearTimeout()
	if pingErr != nil {
		c.pool.release(pc, true)
		return &HealthResult{Connected: false}, pingErr
	}
	proto := pc.wc.protocol
	sv := int(pc.serverVersion)
	c.pool.release(pc, false)

	return &HealthResult{
		Connected:     true,
		LatencyMs:     math.Round(latencyMs*1000) / 1000,
		Protocol:      proto,
		ServerVersion: sv,
		PoolActive:    c.pool.activeCount(),
		PoolIdle:      c.pool.idleCount(),
	}, nil
}

// Metrics returns Prometheus text exposition format for all 12 client metrics.
func (c *PdbWireClient) Metrics() string {
	return c.metrics.generate()
}

// Close closes all pooled connections and releases resources.
// It is safe to call Close more than once.
func (c *PdbWireClient) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.pool.closeAll()
	return nil
}

// ---------------------------------------------------------------------------
// Retry logic
// ---------------------------------------------------------------------------

// acquireWithRetry acquires a pooled connection, retrying ONLY a failure to
// open a new one (dial refused / timed out, HELLO dropped) with exponential
// backoff and jitter. That failure happens before any byte of the caller's
// request is written, so retrying it cannot execute the request twice.
// MaxRetries / RetryDelayMs apply here and nowhere else.
func (c *PdbWireClient) acquireWithRetry(ctx context.Context) (*pooledConn, error) {
	baseDelay := time.Duration(c.opts.RetryDelayMs) * time.Millisecond
	var lastErr error
	for attempt := 0; attempt <= c.opts.MaxRetries; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(rand.Int63n(int64(baseDelay)/2 + 1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(baseDelay + jitter):
			}
			baseDelay = time.Duration(math.Min(float64(baseDelay)*2, float64(5*time.Second)))
			c.metrics.retriesTotal.Add(1)
		}
		pc, err := c.pool.acquire(ctx)
		if err == nil {
			return pc, nil
		}
		var cf *connectFailedError
		if !errors.As(err, &cf) {
			return nil, err
		}
		lastErr = cf.err
	}
	return nil, lastErr
}

// unwrapConnectFailed returns the underlying error of a connectFailedError so
// the internal marker never reaches a caller.
func unwrapConnectFailed(err error) error {
	var cf *connectFailedError
	if errors.As(err, &cf) {
		return cf.err
	}
	return err
}

// ambiguous closes pc after a transport failure or deadline expiry that
// happened once its request had started to be written. The server may have
// executed -- and committed -- the request, so it is never resent; the socket
// may still deliver the late reply (or sit inside a transaction), so it is
// closed, never pooled. The error is always a *PdbConnectionError.
func (c *PdbWireClient) ambiguous(pc *pooledConn, err error) error {
	c.pool.release(pc, true)
	c.metrics.connectionErrors.Add(1)
	var ce *PdbConnectionError
	if errors.As(err, &ce) {
		return err
	}
	return &PdbConnectionError{Cause: err}
}

// sendBatchWithRetry sends one BATCH. Only the pre-send phase is retried.
func (c *PdbWireClient) sendBatchWithRetry(ctx context.Context, sqls []string) ([]wireFrame, error) {
	pc, err := c.acquireWithRetry(ctx)
	if err != nil {
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		pc.wc.setTimeout(time.Until(dl))
	}
	frames, err := pc.wc.sendBatch(sqls)
	if err != nil {
		pc.wc.clearTimeout()
		if isServerError(err) {
			// A top-level server error frame: the stream is in sync.
			c.pool.release(pc, false)
			return nil, err
		}
		return nil, c.ambiguous(pc, err)
	}
	err = decodeBatchResult(frames, len(sqls))
	if err == nil {
		pc.wc.clearTimeout()
		c.pool.release(pc, false)
		return frames, nil
	}
	// A statement failed (or the reply is malformed). Neither is retried:
	// earlier statements in the batch already ran.
	discard := true
	if _, isQueryErr := err.(*PdbQueryError); isQueryErr {
		// The server stopped at the failing statement; if the batch opened a
		// transaction the connection is now inside a failed one. ROLLBACK (a
		// no-op outside a transaction) before reuse; discard the connection
		// if that does not succeed.
		discard = !pc.wc.rollback()
	}
	pc.wc.clearTimeout()
	c.pool.release(pc, discard)
	return nil, err
}

// isServerError reports whether err is a complete server error answer (the
// stream is still in sync), as opposed to a transport failure.
func isServerError(err error) bool {
	switch err.(type) {
	case *PdbQueryError, *DiskFullError:
		return true
	}
	return false
}

// isTransientError returns true for connection-level errors that are safe to retry.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	switch err.(type) {
	case *PdbConnectionError:
		return true
	}
	return false
}
