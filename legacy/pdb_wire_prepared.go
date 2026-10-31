package legacy

// Native prepared statements and batch execution over the PDB wire:
// MSG_PREPARE / MSG_EXECUTE / MSG_EXECUTE_BATCH / MSG_CLOSE_STMT, with the
// parameters sent as a typed Arrow IPC stream (arrow_ipc.go) - values are
// never spliced into SQL text.
//
// Prepared statements live on a server CONNECTION. Each pooled connection
// keeps its own cache (stmtCache) keyed by SQL text: the first use of a SQL
// string on a connection prepares it there, later uses reuse the handle. The
// cache is bounded (maxCachedStmts); the oldest entry is released with
// MSG_CLOSE_STMT when it is full.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const (
	msgWirePrepare      = byte(0x05)
	msgWireExecute      = byte(0x06)
	msgWireExecuteBatch = byte(0x07)
	msgWireCloseStmt    = byte(0x08)
	msgWireAuth         = byte(0x09)
	msgWirePrepareOK    = byte(0x89)

	// maxCachedStmts bounds the per-connection prepared-statement cache (the
	// server allows 4096 per connection).
	maxCachedStmts = 256

	// errCodeNoSuchStmt is the server's code for EXECUTE of an unknown name
	// (e.g. after DISCARD ALL deallocated it).
	errCodeNoSuchStmt = 26

	// maxParamFrameBytes caps one EXECUTE_BATCH frame (the server refuses
	// frames above 16 MiB); a larger batch is split into several frames.
	maxParamFrameBytes = 12 << 20

	// maxFramePayload is the server's frame payload cap (16 MiB); a larger
	// frame is refused.
	maxFramePayload = 16 << 20
)

// PdbParamError is returned when parameters cannot be sent: an unsupported Go
// type, a wrong parameter count, or inconsistent column types across batch
// rows. Nothing was sent to the server.
type PdbParamError struct {
	Message string
}

func (e *PdbParamError) Error() string { return "pdb parameter error: " + e.Message }

// BatchRowError is one failed parameter row of an ExecuteBatch.
type BatchRowError struct {
	Row     int    // index into the rows passed to ExecuteBatch
	Message string // server error message
}

// BatchResult reports a native ExecuteBatch, one entry per parameter row the
// server attempted.
//
// Outside an explicit transaction every row autocommits independently, so a
// failed row does not stop the others. Inside a transaction (Client.Transaction)
// the server stops at the first failed row and the transaction is aborted.
type BatchResult struct {
	// RowsAffected holds, per attempted row, the affected-row count (-1 for a
	// failed row).
	RowsAffected []int64
	// Errors lists the failed rows in order.
	Errors []BatchRowError
	// Attempted is the number of rows the server attempted; it is less than
	// the number of rows sent when a transaction stopped at a failure.
	Attempted int
}

// TotalAffected sums the affected-row counts of the successful rows.
func (r *BatchResult) TotalAffected() int64 {
	var n int64
	for _, a := range r.RowsAffected {
		if a > 0 {
			n += a
		}
	}
	return n
}

// PdbBatchError is returned by ExecuteBatch when at least one row failed or
// the server stopped before attempting every row. Result carries the per-row
// outcome. errors.As(err, **PdbQueryError) yields the first row's error.
type PdbBatchError struct {
	Result *BatchResult
	Sent   int
}

func (e *PdbBatchError) Error() string {
	if len(e.Result.Errors) == 0 {
		return fmt.Sprintf("pdb batch error: server attempted %d of %d rows", e.Result.Attempted, e.Sent)
	}
	first := e.Result.Errors[0]
	return fmt.Sprintf("pdb batch error: %d of %d rows failed (attempted %d); first failure at row %d: %s",
		len(e.Result.Errors), e.Sent, e.Result.Attempted, first.Row, first.Message)
}

// Unwrap returns the first failed row as a *PdbQueryError.
func (e *PdbBatchError) Unwrap() error {
	if len(e.Result.Errors) == 0 {
		return nil
	}
	return &PdbQueryError{Message: e.Result.Errors[0].Message}
}

// ---------------------------------------------------------------------------
// Per-connection statement cache
// ---------------------------------------------------------------------------

type stmtInfo struct {
	name    string
	nParams int
}

type stmtCache struct {
	bySQL map[string]*stmtInfo
	order []string // SQL texts, oldest first
	seq   uint64
}

func (sc *stmtCache) get(sql string) *stmtInfo {
	if sc.bySQL == nil {
		return nil
	}
	return sc.bySQL[sql]
}

func (sc *stmtCache) remove(sql string) *stmtInfo {
	info := sc.bySQL[sql]
	if info == nil {
		return nil
	}
	delete(sc.bySQL, sql)
	for i, s := range sc.order {
		if s == sql {
			sc.order = append(sc.order[:i], sc.order[i+1:]...)
			break
		}
	}
	return info
}

// ---------------------------------------------------------------------------
// wireConn: raw prepared-statement messages
// ---------------------------------------------------------------------------

func appendLenPrefixed(b []byte, s string) []byte {
	var n [4]byte
	binary.LittleEndian.PutUint32(n[:], uint32(len(s)))
	b = append(b, n[:]...)
	return append(b, s...)
}

// doAuth sends MSG_AUTH; the server answers MSG_OK or an error (and closes).
func (wc *wireConn) doAuth(user, password string) error {
	payload := appendLenPrefixed(nil, user)
	payload = appendLenPrefixed(payload, password)
	if err := wc.sendFrame(msgWireAuth, payload); err != nil {
		return &PdbConnectionError{Cause: err}
	}
	f, err := wc.readFrame()
	if err != nil {
		return err
	}
	if f.MsgType != msgWireOK {
		return &PdbConnectionError{Cause: fmt.Errorf("unexpected AUTH response: 0x%02x", f.MsgType)}
	}
	return nil
}

func (wc *wireConn) prepareStmt(name, sql string) (int, error) {
	payload := appendLenPrefixed(make([]byte, 0, 8+len(name)+len(sql)), name)
	payload = appendLenPrefixed(payload, sql)
	if err := wc.sendFrame(msgWirePrepare, payload); err != nil {
		return 0, &PdbConnectionError{Cause: err}
	}
	f, err := wc.readFrame()
	if err != nil {
		return 0, err
	}
	if f.MsgType != msgWirePrepareOK || len(f.Data) < 2 {
		return 0, &PdbConnectionError{Cause: fmt.Errorf("unexpected PREPARE response: 0x%02x (%d bytes)", f.MsgType, len(f.Data))}
	}
	return int(binary.LittleEndian.Uint16(f.Data[0:2])), nil
}

func (wc *wireConn) closeStmt(name string) error {
	if err := wc.sendFrame(msgWireCloseStmt, appendLenPrefixed(nil, name)); err != nil {
		return &PdbConnectionError{Cause: err}
	}
	f, err := wc.readFrame()
	if err != nil {
		return err
	}
	if f.MsgType != msgWireOK {
		return &PdbConnectionError{Cause: fmt.Errorf("unexpected CLOSE_STMT response: 0x%02x", f.MsgType)}
	}
	return nil
}

func (wc *wireConn) executeStmt(msgType byte, name string, params []byte) ([]wireFrame, error) {
	payload := appendLenPrefixed(make([]byte, 0, 4+len(name)+len(params)), name)
	payload = append(payload, params...)
	if err := wc.sendFrame(msgType, payload); err != nil {
		return nil, &PdbConnectionError{Cause: err}
	}
	return wc.collectResponse()
}

// ---------------------------------------------------------------------------
// pooledConn: cached prepare + execute
// ---------------------------------------------------------------------------

// ensurePrepared returns the handle for sql on this connection, preparing it
// (and evicting the oldest cached statement when the cache is full).
func (pc *pooledConn) ensurePrepared(sql string) (*stmtInfo, error) {
	sc := &pc.stmts
	if info := sc.get(sql); info != nil {
		return info, nil
	}
	if sc.bySQL == nil {
		sc.bySQL = make(map[string]*stmtInfo)
	}
	if len(sc.order) >= maxCachedStmts {
		oldest := sc.order[0]
		info := sc.remove(oldest)
		if err := pc.wc.closeStmt(info.name); err != nil {
			return nil, err
		}
	}
	sc.seq++
	name := fmt.Sprintf("pdbgo_%d", sc.seq)
	n, err := pc.wc.prepareStmt(name, sql)
	if err != nil {
		return nil, err
	}
	info := &stmtInfo{name: name, nParams: n}
	sc.bySQL[sql] = info
	sc.order = append(sc.order, sql)
	return info, nil
}

func checkParamCount(info *stmtInfo, got int) error {
	if got != info.nParams {
		return &PdbParamError{Message: fmt.Sprintf("statement expects %d parameter(s), got %d", info.nParams, got)}
	}
	return nil
}

func isNoSuchStmt(err error) bool {
	var qe *PdbQueryError
	return errors.As(err, &qe) && qe.Code == errCodeNoSuchStmt && strings.Contains(qe.Message, "prepared statement")
}

// execPrepared runs sql with one parameter row via PREPARE (cached) + EXECUTE.
// A statement the server no longer knows (DISCARD ALL) is re-prepared once:
// an EXECUTE refused for an unknown name ran nothing, so resending is safe.
func (pc *pooledConn) execPrepared(sql string, args []interface{}) ([]wireFrame, error) {
	for attempt := 0; ; attempt++ {
		info, err := pc.ensurePrepared(sql)
		if err != nil {
			return nil, err
		}
		if err := checkParamCount(info, len(args)); err != nil {
			return nil, err
		}
		var params []byte
		if info.nParams > 0 {
			if params, err = encodeArrowParams([][]interface{}{args}, info.nParams); err != nil {
				return nil, &PdbParamError{Message: err.Error()}
			}
		}
		frames, err := pc.wc.executeStmt(msgWireExecute, info.name, params)
		if err != nil && attempt == 0 && isNoSuchStmt(err) {
			pc.stmts.remove(sql)
			continue
		}
		return frames, err
	}
}

// execPreparedBatch runs sql once per row via EXECUTE_BATCH, splitting rows
// into frames under maxParamFrameBytes. It stops sending after a frame with a
// failed row when inTxn (the server has aborted the transaction).
func (pc *pooledConn) execPreparedBatch(sql string, rows [][]interface{}, inTxn bool) (*BatchResult, error) {
	res := &BatchResult{}
	if len(rows) == 0 {
		return res, nil
	}
	info, err := pc.ensurePrepared(sql)
	if err != nil {
		return nil, err
	}
	for i, r := range rows {
		if len(r) != info.nParams {
			return nil, &PdbParamError{Message: fmt.Sprintf("row %d: statement expects %d parameter(s), got %d", i, info.nParams, len(r))}
		}
	}
	if info.nParams == 0 {
		// A zero-parameter statement runs once per EXECUTE_BATCH frame
		// whatever the row count, so run it once per row with EXECUTE.
		for i := range rows {
			frames, err := pc.execPrepared(sql, nil)
			if err != nil {
				if isServerError(err) {
					res.RowsAffected = append(res.RowsAffected, -1)
					res.Errors = append(res.Errors, BatchRowError{Row: i, Message: errMessage(err)})
					res.Attempted++
					if inTxn {
						break
					}
					continue
				}
				return nil, err
			}
			res.RowsAffected = append(res.RowsAffected, affectedFromFrames(frames))
			res.Attempted++
		}
		return finishBatch(res, len(rows))
	}

	// Encode EVERY frame before sending the first one: outside a transaction
	// each sent frame's rows commit, so a value or size problem found while
	// encoding frame k must surface before frames 0..k-1 have committed
	// (and would otherwise be reported without them).
	type batchFrame struct {
		start, end int
		params     []byte
	}
	var plan []batchFrame
	start := 0
	chunk := len(rows)
	for start < len(rows) {
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		params, err := encodeArrowParams(rows[start:end], info.nParams)
		if err != nil {
			return nil, &PdbParamError{Message: err.Error()}
		}
		if len(params) > maxParamFrameBytes && end-start > 1 {
			chunk = (end - start + 1) / 2
			continue
		}
		if 4+len(info.name)+len(params) > maxFramePayload {
			return nil, &PdbParamError{Message: fmt.Sprintf(
				"row %d alone encodes to %d bytes, over the server's %d-byte frame limit; nothing was sent",
				start, len(params), maxFramePayload)}
		}
		plan = append(plan, batchFrame{start, end, params})
		start = end
	}

	for _, fr := range plan {
		start, end, params := fr.start, fr.end, fr.params
		frames, err := pc.wc.executeStmt(msgWireExecuteBatch, info.name, params)
		if err != nil && isNoSuchStmt(err) {
			pc.stmts.remove(sql)
			if info, err = pc.ensurePrepared(sql); err != nil {
				return nil, err
			}
			frames, err = pc.wc.executeStmt(msgWireExecuteBatch, info.name, params)
		}
		if err != nil {
			return nil, err
		}
		entries, err := decodeBatchEntries(frames)
		if err != nil {
			return nil, err
		}
		if len(entries) > end-start {
			return nil, &PdbError{Message: fmt.Sprintf("protocol error: BATCH_RESULT has %d entries for %d rows", len(entries), end-start)}
		}
		failed := false
		for j, e := range entries {
			if e.err != "" {
				res.RowsAffected = append(res.RowsAffected, -1)
				res.Errors = append(res.Errors, BatchRowError{Row: start + j, Message: e.err})
				failed = true
			} else {
				res.RowsAffected = append(res.RowsAffected, e.affected)
			}
		}
		res.Attempted += len(entries)
		if len(entries) < end-start && !failed {
			return nil, &PdbError{Message: fmt.Sprintf("protocol error: BATCH_RESULT has %d entries for %d rows and no error entry", len(entries), end-start)}
		}
		if failed && inTxn {
			break
		}
	}
	return finishBatch(res, len(rows))
}

func finishBatch(res *BatchResult, sent int) (*BatchResult, error) {
	if len(res.Errors) > 0 || res.Attempted != sent {
		return res, &PdbBatchError{Result: res, Sent: sent}
	}
	return res, nil
}

func errMessage(err error) string {
	var qe *PdbQueryError
	if errors.As(err, &qe) {
		return qe.Message
	}
	return err.Error()
}

type batchEntry struct {
	affected int64
	err      string
}

// decodeBatchEntries decodes every BATCH_RESULT entry (see decodeBatchResult
// for the layout). An Arrow entry (a row-returning statement) counts 0 rows.
func decodeBatchEntries(frames []wireFrame) ([]batchEntry, error) {
	if len(frames) != 1 || frames[0].MsgType != msgWireBatchResult {
		got := "no frame"
		if len(frames) > 0 {
			got = fmt.Sprintf("0x%02x", frames[0].MsgType)
		}
		return nil, &PdbError{Message: "protocol error: expected BATCH_RESULT (0x85), got " + got}
	}
	d := frames[0].Data
	if len(d) < 4 {
		return nil, &PdbError{Message: "protocol error: BATCH_RESULT payload too short"}
	}
	count := int(binary.LittleEndian.Uint32(d[0:4]))
	off := 4
	out := make([]batchEntry, 0, count)
	for i := 0; i < count; i++ {
		bad := &PdbError{Message: fmt.Sprintf("protocol error: BATCH_RESULT truncated at entry %d", i)}
		if off+1 > len(d) {
			return nil, bad
		}
		kind := d[off]
		off++
		switch kind {
		case batchEntryOK:
			if off+8 > len(d) {
				return nil, bad
			}
			out = append(out, batchEntry{affected: int64(binary.LittleEndian.Uint64(d[off:]))})
			off += 8
		case batchEntryArrow, batchEntryError:
			if off+4 > len(d) {
				return nil, bad
			}
			n := int(binary.LittleEndian.Uint32(d[off:]))
			off += 4
			if n < 0 || off+n > len(d) {
				return nil, bad
			}
			if kind == batchEntryError {
				msg := string(d[off : off+n])
				if msg == "" {
					msg = "unknown error"
				}
				out = append(out, batchEntry{affected: -1, err: msg})
			} else {
				out = append(out, batchEntry{})
			}
			off += n
		default:
			return nil, &PdbError{Message: fmt.Sprintf("protocol error: unknown BATCH_RESULT entry type 0x%02x", kind)}
		}
	}
	return out, nil
}

// affectedFromFrames returns the MSG_OK affected count, or 0 for a
// row-returning reply.
func affectedFromFrames(frames []wireFrame) int64 {
	if len(frames) > 0 && frames[0].MsgType == msgWireOK {
		return decodeOK(frames[0])
	}
	return 0
}

// resultFromFrames decodes an ARROW_RESULT / ARROW_STREAM reply into columns
// and rows (an MSG_OK reply is an empty result).
func resultFromFrames(frames []wireFrame) (*arrowResult, error) {
	var arrowFrames []wireFrame
	for _, f := range frames {
		if f.MsgType == msgWireArrowResult || f.MsgType == msgWireArrowStream {
			arrowFrames = append(arrowFrames, f)
		}
	}
	if len(arrowFrames) == 0 {
		return &arrowResult{}, nil
	}
	var combined []byte
	if len(arrowFrames) == 1 {
		combined = arrowFrames[0].Data
	} else {
		for _, f := range arrowFrames {
			combined = append(combined, f.Data...)
		}
	}
	if !isArrowIPCStream(combined) {
		// Not an IPC stream at all: expose the raw bytes (legacy behaviour).
		return &arrowResult{columns: []string{"_raw_arrow_ipc"}, rows: []Row{{"_raw_arrow_ipc": combined}}}, nil
	}
	return decodeArrowStream(combined)
}

// ---------------------------------------------------------------------------
// Pool-level execution
// ---------------------------------------------------------------------------

// withConn runs fn on a pooled connection and decides the connection's fate:
//   - nil error, *PdbParamError: returned to the pool;
//   - *PdbBatchError (rows failed inside a BATCH_RESULT, stream in sync):
//     ROLLBACK (a no-op outside a transaction) and pool it if that succeeds;
//   - a server error frame: discarded (it may sit inside a failed transaction
//     opened through this single-statement API), as sendWithRetry does;
//   - anything else (transport, protocol): ambiguous, discarded, never resent.
func (c *PdbWireClient) withConn(ctx context.Context, fn func(pc *pooledConn) error) error {
	pc, err := c.acquireWithRetry(ctx)
	if err != nil {
		return err
	}
	if dl, ok := ctx.Deadline(); ok {
		pc.wc.setTimeout(time.Until(dl))
	}
	err = fn(pc)
	var be *PdbBatchError
	var pe *PdbParamError
	switch {
	case err == nil, errors.As(err, &pe):
		pc.wc.clearTimeout()
		c.pool.release(pc, false)
	case errors.As(err, &be):
		ok := pc.wc.rollback()
		pc.wc.clearTimeout()
		c.pool.release(pc, !ok)
	case isServerError(err):
		pc.wc.clearTimeout()
		c.pool.release(pc, true)
	default:
		pc.wc.clearTimeout()
		var pdbErr *PdbError
		if errors.As(err, &pdbErr) {
			c.pool.release(pc, true)
			return err
		}
		return c.ambiguous(pc, err)
	}
	return err
}

// queryResult runs sql (see pooledConn.run) and decodes the result.
func (c *PdbWireClient) queryResult(ctx context.Context, sql string, args []interface{}, prepared bool) (*arrowResult, error) {
	t0 := time.Now()
	var res *arrowResult
	err := c.withConn(ctx, func(pc *pooledConn) error {
		frames, err := pc.run(sql, args, prepared)
		if err != nil {
			return err
		}
		res, err = resultFromFrames(frames)
		return err
	})
	c.metrics.queryDurationSum.Add(time.Since(t0).Microseconds())
	c.metrics.queryDurationCount.Add(1)
	if err != nil {
		c.metrics.queriesError.Add(1)
		return nil, err
	}
	c.metrics.queriesTotal.Add(1)
	return res, nil
}

// executeAffected runs sql and returns its affected-row count.
func (c *PdbWireClient) executeAffected(ctx context.Context, sql string, args []interface{}, prepared bool) (int64, error) {
	t0 := time.Now()
	var n int64
	err := c.withConn(ctx, func(pc *pooledConn) error {
		frames, err := pc.run(sql, args, prepared)
		n = affectedFromFrames(frames)
		return err
	})
	c.metrics.queryDurationSum.Add(time.Since(t0).Microseconds())
	c.metrics.queryDurationCount.Add(1)
	if err != nil {
		c.metrics.executesError.Add(1)
		return 0, err
	}
	c.metrics.executesTotal.Add(1)
	return n, nil
}

// run sends sql as a plain QUERY (no args and not prepared) or as a cached
// PREPARE + EXECUTE.
func (pc *pooledConn) run(sql string, args []interface{}, prepared bool) ([]wireFrame, error) {
	if !prepared && len(args) == 0 {
		return pc.wc.sendQuery(sql)
	}
	return pc.execPrepared(sql, args)
}

// ExecuteBatch runs sql once per parameter row in one MSG_EXECUTE_BATCH round
// trip (split into several frames above ~12 MiB of parameters). Parameters
// travel as a typed Arrow IPC stream. Outside a transaction each row
// autocommits independently; see BatchResult. If any row failed, the result
// is returned together with a *PdbBatchError.
func (c *PdbWireClient) ExecuteBatch(ctx context.Context, sql string, rows [][]interface{}) (*BatchResult, error) {
	t0 := time.Now()
	var res *BatchResult
	err := c.withConn(ctx, func(pc *pooledConn) error {
		var err error
		res, err = pc.execPreparedBatch(sql, rows, false)
		return err
	})
	c.metrics.queryDurationSum.Add(time.Since(t0).Microseconds())
	c.metrics.queryDurationCount.Add(1)
	if err != nil {
		c.metrics.batchesError.Add(1)
		var be *PdbBatchError
		if errors.As(err, &be) {
			return be.Result, err
		}
		return nil, err
	}
	c.metrics.batchesTotal.Add(1)
	return res, nil
}

// ---------------------------------------------------------------------------
// PdbStmt - a named prepared statement handle
// ---------------------------------------------------------------------------

// PdbStmt is a prepared statement. It is safe for concurrent use: each call
// runs on some pooled connection, preparing the SQL there on first use.
type PdbStmt struct {
	c       *PdbWireClient
	sql     string
	nParams int
	closed  atomic.Bool
}

// Prepare validates sql on the server (MSG_PREPARE) and returns a handle.
// Parameters are $1..$N.
func (c *PdbWireClient) Prepare(ctx context.Context, sql string) (*PdbStmt, error) {
	var n int
	err := c.withConn(ctx, func(pc *pooledConn) error {
		info, err := pc.ensurePrepared(sql)
		if err == nil {
			n = info.nParams
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return &PdbStmt{c: c, sql: sql, nParams: n}, nil
}

var errStmtClosed = &PdbParamError{Message: "statement is closed"}

// NumParams returns the number of parameters the server reported.
func (s *PdbStmt) NumParams() int { return s.nParams }

// SQL returns the statement text.
func (s *PdbStmt) SQL() string { return s.sql }

// Query executes the statement with one parameter row and returns its rows.
func (s *PdbStmt) Query(ctx context.Context, args ...interface{}) ([]Row, error) {
	if s.closed.Load() {
		return nil, errStmtClosed
	}
	res, err := s.c.queryResult(ctx, s.sql, args, true)
	if err != nil {
		return nil, err
	}
	return res.rows, nil
}

// Execute executes the statement with one parameter row and returns the
// affected-row count.
func (s *PdbStmt) Execute(ctx context.Context, args ...interface{}) (int64, error) {
	if s.closed.Load() {
		return 0, errStmtClosed
	}
	return s.c.executeAffected(ctx, s.sql, args, true)
}

// ExecuteBatch executes the statement once per row via MSG_EXECUTE_BATCH.
func (s *PdbStmt) ExecuteBatch(ctx context.Context, rows [][]interface{}) (*BatchResult, error) {
	if s.closed.Load() {
		return nil, errStmtClosed
	}
	return s.c.ExecuteBatch(ctx, s.sql, rows)
}

// Close releases the statement (MSG_CLOSE_STMT) on every idle pooled
// connection that has it. A connection that is busy at the time keeps it
// until it is evicted from that connection's cache or the connection closes.
func (s *PdbStmt) Close() error {
	if s.closed.Swap(true) {
		return nil
	}
	s.c.pool.forEachIdle(func(pc *pooledConn) bool {
		info := pc.stmts.remove(s.sql)
		if info == nil {
			return true
		}
		return pc.wc.closeStmt(info.name) == nil
	})
	return nil
}
