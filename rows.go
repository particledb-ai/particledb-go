package particledb

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"time"
)

// CommandTag is the outcome of a statement that returns no rows.
type CommandTag struct {
	rows       int64
	select_    bool
	rolledBack bool
}

// RowsAffected is the number of rows the statement changed (for a query,
// the number of rows it returned).
func (t CommandTag) RowsAffected() int64 { return t.rows }

// Select reports whether the statement returned rows.
func (t CommandTag) Select() bool { return t.select_ }

// RolledBack reports that a COMMIT ended a failed transaction by rolling it
// back (PostgreSQL's ROLLBACK command tag).
func (t CommandTag) RolledBack() bool { return t.rolledBack }

func tagFromOK(f frame) CommandTag {
	t := CommandTag{rolledBack: f.flags&flagTxnRolledBack != 0}
	if len(f.payload) >= 8 {
		t.rows = int64(binary.LittleEndian.Uint64(f.payload))
	}
	return t
}

// Rows is a forward-only result. Rows are decoded one Arrow record batch at
// a time as they arrive, so a large result is never held whole in memory.
// Always Close a Rows (or iterate it to the end); a Rows that is closed early
// tells the server to stop and discards the rest.
type Rows struct {
	conn    *Conn
	s       *stream
	ctx     context.Context
	fields  []FieldDescription
	schema  *arrowSchema
	batch   arrowBatch
	row     int
	buf     []byte
	acc     []byte
	ended   bool
	done    bool
	err     error
	closed  bool
	tag     CommandTag
	notices []Notice
	loc     *time.Location
	onClose func(error)
	nrows   int64
	pooled  *Conn // shared pool connection whose in-flight count this Rows holds
	colBuf  [4]arrowCol
}

// newRows builds a Rows from the first frame of a request.
func newRows(ctx context.Context, conn *Conn, s *stream, f frame, notices []Notice) (*Rows, error) {
	r := &Rows{}
	if err := r.init(ctx, conn, s, f, notices); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Rows) init(ctx context.Context, conn *Conn, s *stream, f frame, notices []Notice) error {
	*r = Rows{conn: conn, s: s, ctx: ctx, row: -1, notices: notices}
	r.batch.cols = r.colBuf[:0]
	if conn != nil {
		r.loc = conn.cfg.location()
	}
	switch f.typ {
	case fArrowResult:
		r.buf, r.ended = f.payload, true
	case fArrowStream:
		r.buf, r.ended = f.payload, f.terminal()
	case fOK:
		r.ended, r.done = true, true
		r.tag = tagFromOK(f)
		return nil
	default:
		return protoErr("unexpected answer 0x%02x to a query", f.typ)
	}
	if err := r.readSchema(); err != nil {
		r.Close()
		return err
	}
	return nil
}

// rowsFromArrow is a Rows over a complete Arrow IPC payload (a batch entry).
func rowsFromArrow(conn *Conn, payload []byte) (*Rows, error) {
	r := &Rows{conn: conn, row: -1, buf: payload, ended: true, ctx: context.Background()}
	if conn != nil {
		r.loc = conn.cfg.location()
	}
	if err := r.readSchema(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Rows) readSchema() (err error) {
	defer recoverArrow(&err)
	for r.schema == nil {
		m, ok, err := r.message()
		if err != nil {
			return err
		}
		if !ok {
			return protoErr("result ended before its schema")
		}
		if m.kind == hdrSchema {
			r.setSchema(m)
		}
	}
	return nil
}

func (r *Rows) setSchema(m ipcMessage) {
	if r.conn != nil {
		r.schema, r.fields = r.conn.schemaFor(m)
	} else {
		r.schema = parseSchema(m.header)
		r.fields = make([]FieldDescription, len(r.schema.fields))
		for i, f := range r.schema.fields {
			r.fields[i] = describeField(f)
		}
	}
}

// message returns the next IPC message, reading frames as needed. ok is
// false at the end of the stream.
func (r *Rows) message() (ipcMessage, bool, error) {
	for {
		if r.done {
			return ipcMessage{}, false, nil
		}
		m, used, ok := nextMessage(r.buf)
		if ok {
			r.buf = r.buf[used:]
			if m.eos {
				r.done = true
				return m, false, nil
			}
			return m, true, nil
		}
		if r.ended {
			if len(r.buf) == 0 {
				r.done = true
				return ipcMessage{}, false, nil
			}
			return ipcMessage{}, false, protoErr("result stream truncated (%d bytes left)", len(r.buf))
		}
		f, err := r.s.next(r.ctx)
		if err != nil {
			if r.ctx.Err() != nil && err == r.ctx.Err() {
				r.s.abandon()
				r.ended, r.done = true, true
				return ipcMessage{}, false, canceledErr(err, "")
			}
			return ipcMessage{}, false, err
		}
		switch f.typ {
		case fNotice:
			ns := decodeNotices(f.payload)
			r.notices = append(r.notices, ns...)
			if r.conn != nil && r.conn.cfg.OnNotice != nil {
				for _, n := range ns {
					r.conn.cfg.OnNotice(r.conn, n)
				}
			}
			continue
		case fError:
			r.ended, r.done = true, true
			return ipcMessage{}, false, frameError(f)
		case fArrowStream, fArrowResult:
		default:
			r.ended, r.done = true, true
			return ipcMessage{}, false, protoErr("unexpected frame 0x%02x inside a result stream", f.typ)
		}
		if f.terminal() {
			r.ended = true
		}
		if len(r.buf) == 0 {
			r.buf = f.payload
			continue
		}
		// A message spans frames: accumulate.
		need := len(r.buf) + len(f.payload)
		if cap(r.acc) < need {
			nb := make([]byte, len(r.buf), need+need/2)
			copy(nb, r.buf)
			r.acc = nb
		} else {
			n := copy(r.acc[:cap(r.acc)], r.buf)
			r.acc = r.acc[:n]
		}
		r.acc = append(r.acc, f.payload...)
		r.buf = r.acc
	}
}

// FieldDescriptions describes the result columns.
func (r *Rows) FieldDescriptions() []FieldDescription { return r.fields }

// Columns returns the column names.
func (r *Rows) Columns() []string {
	out := make([]string, len(r.fields))
	for i := range r.fields {
		out[i] = r.fields[i].Name
	}
	return out
}

// Next advances to the next row. It returns false at the end of the result
// or on an error (see Err); either way the Rows is closed.
func (r *Rows) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	r.row++
	for r.row >= r.batch.rows {
		ok, err := r.nextBatch()
		if err != nil {
			r.err = err
			r.Close()
			return false
		}
		if !ok {
			r.Close()
			return false
		}
		r.row = 0
	}
	r.nrows++
	return true
}

func (r *Rows) nextBatch() (ok bool, err error) {
	defer recoverArrow(&err)
	for {
		m, more, err := r.message()
		if err != nil || !more {
			return false, err
		}
		switch m.kind {
		case hdrSchema:
			r.setSchema(m)
		case hdrRecordBatch:
			decodeBatch(r.schema, m.header, m.body, &r.batch)
			return true, nil
		case hdrDictionary:
			return false, protoErr("dictionary batches are not supported")
		}
	}
}

// Err is the error that ended iteration, if any.
func (r *Rows) Err() error { return r.err }

// Close releases the result. Closing before the end tells the server to stop
// sending it.
func (r *Rows) Close() {
	if r.closed {
		return
	}
	r.closed = true
	if r.s != nil && !r.ended {
		r.s.abandon()
	}
	r.tag.select_ = r.schema != nil
	if r.schema != nil {
		r.tag.rows = r.nrows
	}
	r.batch.rows = 0
	r.buf, r.acc = nil, nil
	if r.pooled != nil {
		r.pooled.inflight.Add(-1)
		r.pooled = nil
	}
	if r.onClose != nil {
		f := r.onClose
		r.onClose = nil
		f(r.err)
	}
}

// CommandTag is the statement's outcome; for a query it is complete after
// the Rows is closed.
func (r *Rows) CommandTag() CommandTag { return r.tag }

// Notices returns the server's advisories for the statement received so far.
func (r *Rows) Notices() []Notice { return r.notices }

func (r *Rows) cell(j int) cell { return cell{c: &r.batch.cols[j], fd: &r.fields[j], i: r.row} }

// Values returns the current row as Go values (int64, float64, float32,
// string, bool, []byte, time.Time, Decimal, UUID, Vector, json.RawMessage,
// []any, nil).
func (r *Rows) Values() ([]any, error) {
	if r.row < 0 || r.row >= r.batch.rows {
		return nil, errors.New("particledb: Values called without a current row")
	}
	out := make([]any, len(r.fields))
	for j := range r.fields {
		out[j] = r.cell(j).value(true, r.loc)
	}
	return out, nil
}

// Scan copies the current row into dest. Each dest is a pointer: to a Go
// type the column converts to (string, []byte, int*, uint*, float*, bool,
// time.Time, Decimal, UUID, Vector, json.RawMessage, any), to an sql.Scanner,
// to a pointer of any of these (nil for NULL), or nil to skip the column.
// A JSON column also scans into any other type through json.Unmarshal, and
// an array column into a slice.
func (r *Rows) Scan(dest ...any) error {
	if r.row < 0 || r.row >= r.batch.rows {
		return errors.New("particledb: Scan called without a current row")
	}
	if len(dest) != len(r.fields) {
		return fmt.Errorf("particledb: Scan got %d destinations for %d columns", len(dest), len(r.fields))
	}
	for j, d := range dest {
		if d == nil {
			continue
		}
		if err := scanCell(r.cell(j), d, r.loc); err != nil {
			return fmt.Errorf("particledb: column %d (%s): %w", j, r.fields[j].Name, err)
		}
	}
	return nil
}

var errNullScan = errors.New("cannot scan NULL into a non-pointer destination (use a pointer or sql.Null type)")

func scanCell(x cell, dest any, loc *time.Location) error {
	if x.null() {
		switch d := dest.(type) {
		case *any:
			*d = nil
			return nil
		case sql.Scanner:
			return d.Scan(nil)
		case *[]byte:
			*d = nil
			return nil
		case *json.RawMessage:
			*d = nil
			return nil
		}
		rv := reflect.ValueOf(dest)
		if rv.Kind() == reflect.Pointer && !rv.IsNil() {
			e := rv.Elem()
			if e.Kind() == reflect.Pointer || e.Kind() == reflect.Slice || e.Kind() == reflect.Map || e.Kind() == reflect.Interface {
				e.Set(reflect.Zero(e.Type()))
				return nil
			}
		}
		return errNullScan
	}
	k := x.fd.kind
	c, i := x.c, x.i
	switch d := dest.(type) {
	case *string:
		*d = x.text(loc)
		return nil
	case *int64:
		v, err := x.int64()
		*d = v
		return err
	case *int:
		v, err := x.int64()
		*d = int(v)
		return err
	case *int32:
		v, err := x.intRange(math.MinInt32, math.MaxInt32)
		*d = int32(v)
		return err
	case *int16:
		v, err := x.intRange(math.MinInt16, math.MaxInt16)
		*d = int16(v)
		return err
	case *float64:
		v, err := x.float()
		*d = v
		return err
	case *float32:
		v, err := x.float()
		*d = float32(v)
		return err
	case *bool:
		switch k {
		case ckBool:
			*d = c.boolAt(i)
			return nil
		case ckText:
			b, err := strconv.ParseBool(x.text(loc))
			*d = b
			return err
		}
		return fmt.Errorf("cannot scan %s into *bool", x.fd.DatabaseTypeName)
	case *[]byte:
		switch k {
		case ckBytes, ckText, ckJSON, ckNumericText, ckUUID, ckArray, ckTime:
			b := c.bytesAt(i)
			*d = append((*d)[:0], b...)
			if *d == nil {
				*d = []byte{}
			}
			return nil
		}
		*d = []byte(x.text(loc))
		return nil
	case *json.RawMessage:
		b := []byte(x.text(loc))
		if k == ckJSON || k == ckText || k == ckBytes {
			b = append([]byte(nil), c.bytesAt(i)...)
		}
		*d = b
		return nil
	case *time.Time:
		switch k {
		case ckDate, ckTimestamp, ckTimestampTZ:
			*d = x.time(loc)
			return nil
		case ckText:
			t, err := parseTimeText(x.text(loc))
			*d = t
			return err
		}
		return fmt.Errorf("cannot scan %s into *time.Time", x.fd.DatabaseTypeName)
	case *Decimal:
		switch k {
		case ckDecimal, ckInt:
			*d = x.decimal()
			return nil
		case ckNumericText, ckText:
			v, err := ParseDecimal(string(c.bytesAt(i)))
			*d = v
			return err
		case ckFloat32, ckFloat64:
			v, err := ParseDecimal(x.text(loc))
			*d = v
			return err
		}
		return fmt.Errorf("cannot scan %s into *Decimal", x.fd.DatabaseTypeName)
	case *UUID:
		if k == ckBytes && len(c.bytesAt(i)) == 16 {
			copy(d[:], c.bytesAt(i))
			return nil
		}
		u, err := ParseUUID(x.text(loc))
		*d = u
		return err
	case *Vector:
		if k == ckVector {
			*d = x.vector()
			return nil
		}
		v, err := ParseVector(x.text(loc))
		*d = v
		return err
	case *big.Int:
		v, ok := new(big.Int).SetString(x.text(loc), 10)
		if !ok {
			return fmt.Errorf("cannot scan %q into *big.Int", x.text(loc))
		}
		d.Set(v)
		return nil
	case *any:
		*d = x.value(true, loc)
		return nil
	case sql.Scanner:
		return d.Scan(x.value(false, loc))
	}
	return scanReflect(x, dest, loc)
}

func scanReflect(x cell, dest any, loc *time.Location) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("destination %T is not a non-nil pointer", dest)
	}
	e := rv.Elem()
	// pointer to pointer: allocate
	if e.Kind() == reflect.Pointer {
		nv := reflect.New(e.Type().Elem())
		if err := scanCell(x, nv.Interface(), loc); err != nil {
			return err
		}
		e.Set(nv)
		return nil
	}
	if x.fd.kind == ckJSON {
		return json.Unmarshal(x.c.bytesAt(x.i), dest)
	}
	if x.fd.kind == ckArray && e.Kind() == reflect.Slice {
		return scanArrayText(string(x.c.bytesAt(x.i)), dest)
	}
	switch e.Kind() {
	case reflect.String:
		e.SetString(x.text(loc))
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		lo, hi := int64(math.MinInt64), int64(math.MaxInt64)
		switch e.Type().Bits() {
		case 8:
			lo, hi = math.MinInt8, math.MaxInt8
		case 16:
			lo, hi = math.MinInt16, math.MaxInt16
		case 32:
			lo, hi = math.MinInt32, math.MaxInt32
		}
		v, err := x.intRange(lo, hi)
		if err != nil {
			return err
		}
		e.SetInt(v)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := x.int64()
		if err != nil {
			return err
		}
		if v < 0 || (e.Type().Bits() < 64 && uint64(v) > (uint64(1)<<e.Type().Bits())-1) {
			return fmt.Errorf("value %d out of range for %s", v, e.Type())
		}
		e.SetUint(uint64(v))
		return nil
	case reflect.Float32, reflect.Float64:
		v, err := x.float()
		if err != nil {
			return err
		}
		e.SetFloat(v)
		return nil
	case reflect.Bool:
		if x.fd.kind == ckBool {
			e.SetBool(x.c.boolAt(x.i))
			return nil
		}
	case reflect.Interface:
		e.Set(reflect.ValueOf(x.value(true, loc)))
		return nil
	}
	return fmt.Errorf("cannot scan %s into %T", x.fd.DatabaseTypeName, dest)
}

func (x cell) int64() (int64, error) {
	switch x.fd.kind {
	case ckInt:
		return x.c.intAt(x.i), nil
	case ckDecimal, ckNumericText, ckText:
		s := x.text(nil)
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			d, derr := ParseDecimal(s)
			if derr == nil && d.special == 0 {
				r := d.Rat()
				if r.IsInt() && r.Num().IsInt64() {
					return r.Num().Int64(), nil
				}
			}
			return 0, fmt.Errorf("cannot convert %q to an integer", s)
		}
		return v, nil
	case ckBool:
		if x.c.boolAt(x.i) {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("cannot scan %s into an integer", x.fd.DatabaseTypeName)
}

func (x cell) intRange(lo, hi int64) (int64, error) {
	v, err := x.int64()
	if err != nil {
		return 0, err
	}
	if v < lo || v > hi {
		return 0, fmt.Errorf("value %d out of range [%d, %d]", v, lo, hi)
	}
	return v, nil
}

func (x cell) float() (float64, error) {
	switch x.fd.kind {
	case ckFloat64:
		return x.c.floatAt(x.i), nil
	case ckFloat32:
		return float64(x.c.float32At(x.i)), nil
	case ckInt:
		return float64(x.c.intAt(x.i)), nil
	case ckDecimal:
		return x.decimal().Float64(), nil
	case ckNumericText, ckText:
		return strconv.ParseFloat(string(x.c.bytesAt(x.i)), 64)
	}
	return 0, fmt.Errorf("cannot scan %s into a float", x.fd.DatabaseTypeName)
}

// Row is the single-row result of QueryRow. Errors are deferred to Scan.
type Row struct {
	rows *Rows
	err  error
	own  Rows
}

// Scan scans the first row and closes the result. It returns sql.ErrNoRows
// (also matched by ErrNoRows) when there is none.
func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	defer r.rows.Close()
	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return ErrNoRows
	}
	if err := r.rows.Scan(dest...); err != nil {
		return err
	}
	r.rows.Close()
	return r.rows.Err()
}

// ErrNoRows is returned by Row.Scan when the query returned no rows. It is
// database/sql's sql.ErrNoRows, so errors.Is works with either.
var ErrNoRows = sql.ErrNoRows
