package particledb

// CopyFrom: native bulk ingest (F_BULK). Rows are packed into Arrow record
// batches on the client and streamed to the server, which converts them to
// the table's declared types, checks constraints and makes the load durable
// through the same WAL records INSERT writes. A load is atomic: every row or
// none.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// Identifier is a possibly schema-qualified name: Identifier{"public", "t"}.
type Identifier []string

// Sanitize returns the quoted, dot-joined form.
func (id Identifier) Sanitize() string {
	parts := make([]string, len(id))
	for i, p := range id {
		parts[i] = quoteIdent(p)
	}
	return strings.Join(parts, ".")
}

// CopyFromSource yields rows for CopyFrom.
type CopyFromSource interface {
	// Next advances to the next row; false at the end or on an error.
	Next() bool
	// Values returns the current row.
	Values() ([]any, error)
	// Err returns the error that stopped Next.
	Err() error
}

type rowsSource struct {
	rows [][]any
	i    int
}

func (s *rowsSource) Next() bool             { s.i++; return s.i <= len(s.rows) }
func (s *rowsSource) Values() ([]any, error) { return s.rows[s.i-1], nil }
func (s *rowsSource) Err() error             { return nil }

// CopyFromRows is a CopyFromSource over in-memory rows.
func CopyFromRows(rows [][]any) CopyFromSource { return &rowsSource{rows: rows} }

type sliceSource struct {
	n    int
	i    int
	next func(int) ([]any, error)
	cur  []any
	err  error
}

func (s *sliceSource) Next() bool {
	if s.err != nil || s.i >= s.n {
		return false
	}
	s.cur, s.err = s.next(s.i)
	s.i++
	return s.err == nil
}
func (s *sliceSource) Values() ([]any, error) { return s.cur, s.err }
func (s *sliceSource) Err() error             { return s.err }

// CopyFromSlice is a CopyFromSource of n rows produced by next(i).
func CopyFromSlice(n int, next func(i int) ([]any, error)) CopyFromSource {
	return &sliceSource{n: n, next: next}
}

type funcSource struct {
	f    func() ([]any, error)
	cur  []any
	err  error
	done bool
}

func (s *funcSource) Next() bool {
	if s.done {
		return false
	}
	s.cur, s.err = s.f()
	if s.err != nil || s.cur == nil {
		s.done = true
		return false
	}
	return true
}
func (s *funcSource) Values() ([]any, error) { return s.cur, nil }
func (s *funcSource) Err() error             { return s.err }

// CopyFromFunc is a CopyFromSource that calls f until it returns a nil row
// or an error.
func CopyFromFunc(f func() (row []any, err error)) CopyFromSource { return &funcSource{f: f} }

var (
	copyBatchRows  = 65536
	copyBatchBytes = 6 << 20
	copyInFlight   = 4
)

// CopyFrom loads rows into table through the native bulk ingest path and
// returns the number of rows loaded. columns names the target columns (the
// others take their defaults); nil means every column of the table, in
// order. Outside a transaction the load commits on its own; inside one it is
// a statement of that transaction.
func (c *Conn) CopyFrom(ctx context.Context, table Identifier, columns []string, src CopyFromSource) (int64, error) {
	if !c.wc.hello.bulkIngest() {
		return 0, clientErr("0A000", "the server does not offer native bulk ingest")
	}
	if len(table) == 0 {
		return 0, clientErr("42602", "CopyFrom needs a table name")
	}
	c.inflight.Add(1)
	defer c.inflight.Add(-1)
	meta, err := c.tableMeta(ctx, table)
	if err != nil {
		return 0, err
	}
	if len(columns) == 0 {
		for i := range meta {
			columns = append(columns, meta[i].Name)
		}
	}
	// A float bound for a numeric column travels as its text (the spelling
	// the statement path renders), so the column's input function reads the
	// exact decimal; the Float64 conversion path does not keep every digit.
	floatText := make([]bool, len(columns))
	for j, col := range columns {
		for i := range meta {
			if strings.EqualFold(meta[i].Name, col) && meta[i].DatabaseTypeName == "NUMERIC" {
				floatText[j] = true
			}
		}
	}
	// A lazy BEGIN must run before the load (F_BULK carries no BEGIN flag).
	c.txMu.Lock()
	if c.txLazy {
		if _, _, err := c.wc.roundTrip(ctx, fQuery, 0, []byte("BEGIN")); err != nil {
			c.txMu.Unlock()
			return 0, err
		}
		c.setLazy(false)
	}
	c.txMu.Unlock()

	relation := table.Sanitize()
	begin := []byte{bulkBegin}
	begin = appendU16Str(begin, relation)
	begin = binary.LittleEndian.AppendUint16(begin, uint16(len(columns)))
	for _, col := range columns {
		begin = appendU16Str(begin, col)
	}
	begin = append(begin, 0)
	begin = binary.LittleEndian.AppendUint64(begin, 0)
	if _, _, err := c.wc.roundTrip(ctx, fBulk, 0, begin); err != nil {
		if s := SQLState(err); s == "42703" || s == "42P01" {
			c.forgetTable(table)
		}
		return 0, err
	}
	abort := func(err error) (int64, error) {
		ctx2, cancel := context.WithTimeout(context.Background(), c.cfg.cancelGrace())
		_, _, _ = c.wc.roundTrip(ctx2, fBulk, 0, []byte{bulkAbort})
		cancel()
		return 0, err
	}

	enc := newBatchEncoder(columns)
	enc.floatText = floatText
	var pending []*stream
	var frames uint64
	var firstErr error
	await := func(s *stream) {
		f, err := s.await(ctx, nil)
		if err == nil && f.typ == fError {
			err = frameError(f)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	flush := func() error {
		if enc.rows == 0 {
			return nil
		}
		payload := enc.encode()
		s, err := c.wc.start(ctx, fBulk, 0, payload)
		if err != nil {
			return err
		}
		frames++
		pending = append(pending, s)
		for len(pending) >= copyInFlight {
			await(pending[0])
			pending = pending[1:]
		}
		return firstErr
	}
	for src.Next() {
		row, err := src.Values()
		if err != nil {
			return abort(err)
		}
		if len(row) != len(columns) {
			for _, s := range pending {
				await(s)
			}
			return abort(clientErr("22023", "CopyFrom row has %d values for %d columns", len(row), len(columns)))
		}
		if err := enc.add(row); err != nil {
			for _, s := range pending {
				await(s)
			}
			return abort(err)
		}
		if enc.rows >= copyBatchRows || enc.bytes >= copyBatchBytes {
			if err := flush(); err != nil {
				for _, s := range pending {
					await(s)
				}
				return abort(err)
			}
		}
	}
	if err := src.Err(); err != nil {
		for _, s := range pending {
			await(s)
		}
		return abort(err)
	}
	if err := flush(); err != nil {
		for _, s := range pending {
			await(s)
		}
		return abort(err)
	}
	for _, s := range pending {
		await(s)
	}
	if firstErr != nil {
		return abort(firstErr)
	}
	end := []byte{bulkEnd}
	end = binary.LittleEndian.AppendUint64(end, frames)
	f, _, err := c.wc.roundTrip(ctx, fBulk, 0, end)
	if err != nil {
		return abort(err)
	}
	return tagFromOK(f).rows, nil
}

func appendU16Str(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func (c *Conn) tableColumns(ctx context.Context, table Identifier) ([]string, error) {
	fds, err := c.tableMeta(ctx, table)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(fds))
	for i := range fds {
		out[i] = fds[i].Name
	}
	return out, nil
}

// tableMeta describes a table's columns (cached per connection; a failed
// load drops the entry, so a changed table is described again).
func (c *Conn) tableMeta(ctx context.Context, table Identifier) ([]FieldDescription, error) {
	key := table.Sanitize()
	c.schemaMu.Lock()
	if fds, ok := c.tables[key]; ok {
		c.schemaMu.Unlock()
		return fds, nil
	}
	c.schemaMu.Unlock()
	rows, err := c.Query(ctx, "SELECT * FROM "+key+" LIMIT 0")
	if err != nil {
		return nil, err
	}
	fds := append([]FieldDescription(nil), rows.FieldDescriptions()...)
	rows.Close()
	c.schemaMu.Lock()
	if c.tables == nil || len(c.tables) > 256 {
		c.tables = map[string][]FieldDescription{}
	}
	c.tables[key] = fds
	c.schemaMu.Unlock()
	return fds, nil
}

func (c *Conn) forgetTable(table Identifier) {
	c.schemaMu.Lock()
	delete(c.tables, table.Sanitize())
	c.schemaMu.Unlock()
}

// ---------------------------------------------------------------------------
// Column builders
// ---------------------------------------------------------------------------

type ckind uint8

const (
	bUnset ckind = iota
	bInt
	bFloat
	bBool
	bText
	bBinary
)

type colBuilder struct {
	kind     ckind
	n        int
	validity []byte
	nulls    int
	ints     []int64
	floats   []float64
	bools    []byte
	offs     []int32 // len n+1 for text/binary
	data     []byte
}

type batchEncoder struct {
	names     []string
	floatText []bool
	cols      []colBuilder
	rows      int
	bytes     int
	w         fbWriter
}

func newBatchEncoder(names []string) *batchEncoder {
	return &batchEncoder{names: names, cols: make([]colBuilder, len(names))}
}

func setBit(b []byte, i int) []byte {
	for len(b) <= i>>3 {
		b = append(b, 0)
	}
	b[i>>3] |= 1 << (uint(i) & 7)
	return b
}

func growBits(b []byte, n int) []byte {
	for len(b) < (n+7)/8 {
		b = append(b, 0)
	}
	return b
}

// add appends one row.
func (e *batchEncoder) add(row []any) error {
	for j, v := range row {
		if err := e.cols[j].add(v, j, e.floatText != nil && e.floatText[j]); err != nil {
			return err
		}
	}
	e.rows++
	if e.rows&1023 == 0 {
		b := 0
		for j := range e.cols {
			b += 8*e.rows + len(e.cols[j].data)
		}
		e.bytes = b
	}
	return nil
}

func (cb *colBuilder) add(v any, j int, floatText bool) error {
	k, iv, fv, bv, sv, err := copyValue(v)
	if err != nil {
		return badParam(j, "%v", err)
	}
	if floatText && k == bFloat {
		k, sv = bText, numericFloatText(fv)
	}
	if k == bUnset {
		cb.nulls++
		cb.appendSlot()
		return nil
	}
	switch {
	case cb.kind == bUnset:
		cb.setKind(k)
	case cb.kind == k:
	case cb.kind == bInt && k == bFloat:
		cb.toFloat()
	case cb.kind == bFloat && k == bInt:
		fv, k = float64(iv), bFloat
	default:
		if cb.kind != bText {
			cb.toText()
		}
		if k != bText {
			sv, k = textOf(k, iv, fv, bv, sv), bText
		}
	}
	r := cb.n
	cb.appendSlot()
	cb.validity = setBit(growBits(cb.validity, cb.n), r)
	switch cb.kind {
	case bInt:
		cb.ints[r] = iv
	case bFloat:
		cb.floats[r] = fv
	case bBool:
		if bv {
			cb.bools = setBit(cb.bools, r)
		}
	case bText, bBinary:
		cb.data = append(cb.data, sv...)
		if len(cb.data) > math.MaxInt32 {
			return badParam(j, "column data exceeds 2 GiB in one batch")
		}
		cb.offs[r+1] = int32(len(cb.data))
	}
	return nil
}

// appendSlot appends one empty (zero / empty-string) slot.
func (cb *colBuilder) appendSlot() {
	cb.n++
	cb.validity = growBits(cb.validity, cb.n)
	switch cb.kind {
	case bInt:
		cb.ints = append(cb.ints, 0)
	case bFloat:
		cb.floats = append(cb.floats, 0)
	case bBool:
		cb.bools = growBits(cb.bools, cb.n)
	case bText, bBinary:
		if len(cb.offs) == 0 {
			cb.offs = append(cb.offs, 0)
		}
		cb.offs = append(cb.offs, int32(len(cb.data)))
	}
}

// setKind fixes the column's kind, giving the earlier (NULL) slots storage.
func (cb *colBuilder) setKind(k ckind) {
	cb.kind = k
	n := cb.n
	switch k {
	case bInt:
		cb.ints = append(cb.ints[:0], make([]int64, n)...)
	case bFloat:
		cb.floats = append(cb.floats[:0], make([]float64, n)...)
	case bBool:
		cb.bools = growBits(cb.bools[:0], n)
	case bText, bBinary:
		cb.offs = append(cb.offs[:0], make([]int32, n+1)...)
		cb.data = cb.data[:0]
	}
}

func (cb *colBuilder) toFloat() {
	cb.floats = cb.floats[:0]
	for _, v := range cb.ints {
		cb.floats = append(cb.floats, float64(v))
	}
	cb.ints = cb.ints[:0]
	cb.kind = bFloat
}

func (cb *colBuilder) valid(i int) bool {
	return i>>3 < len(cb.validity) && cb.validity[i>>3]&(1<<(uint(i)&7)) != 0
}

// toText converts the column's values so far to their text form.
func (cb *colBuilder) toText() {
	offs := make([]int32, 1, cb.n+1)
	var data []byte
	for i := 0; i < cb.n; i++ {
		if cb.valid(i) {
			switch cb.kind {
			case bInt:
				data = strconv.AppendInt(data, cb.ints[i], 10)
			case bFloat:
				data = append(data, formatFloat(cb.floats[i], 64)...)
			case bBool:
				if cb.bools[i>>3]&(1<<(uint(i)&7)) != 0 {
					data = append(data, "true"...)
				} else {
					data = append(data, "false"...)
				}
			case bBinary:
				data = append(data, byteaHex(cb.data[cb.offs[i]:cb.offs[i+1]])...)
			}
		}
		offs = append(offs, int32(len(data)))
	}
	cb.offs, cb.data, cb.ints, cb.floats, cb.bools = offs, data, cb.ints[:0], cb.floats[:0], cb.bools[:0]
	cb.kind = bText
}

// numericFloatText is the decimal a float parameter stands for in a numeric
// column (NaN and the infinities by name).
func numericFloatText(f float64) string {
	if s := formatFloat(f, 64); s == "NaN" || s == "Infinity" || s == "-Infinity" {
		return s
	}
	return javaDoubleText(f)
}

func textOf(k ckind, iv int64, fv float64, bv bool, sv string) string {
	switch k {
	case bInt:
		return strconv.FormatInt(iv, 10)
	case bFloat:
		return formatFloat(fv, 64)
	case bBool:
		if bv {
			return "true"
		}
		return "false"
	case bBinary:
		return byteaHex([]byte(sv))
	}
	return sv
}

// copyValue classifies one CopyFrom value. Typed values without an exact
// Arrow counterpart travel as their PostgreSQL text, which the server parses
// with the column type's input function (the same as COPY text format).
func copyValue(v any) (ckind, int64, float64, bool, string, error) {
	switch x := v.(type) {
	case nil:
		return bUnset, 0, 0, false, "", nil
	case int64:
		return bInt, x, 0, false, "", nil
	case int:
		return bInt, int64(x), 0, false, "", nil
	case int32:
		return bInt, int64(x), 0, false, "", nil
	case int16:
		return bInt, int64(x), 0, false, "", nil
	case int8:
		return bInt, int64(x), 0, false, "", nil
	case uint32:
		return bInt, int64(x), 0, false, "", nil
	case uint16:
		return bInt, int64(x), 0, false, "", nil
	case uint8:
		return bInt, int64(x), 0, false, "", nil
	case float64:
		return bFloat, 0, x, false, "", nil
	case float32:
		return bFloat, 0, float64(x), false, "", nil
	case bool:
		return bBool, 0, 0, x, "", nil
	case string:
		return bText, 0, 0, false, x, nil
	case []byte:
		if x == nil {
			return bUnset, 0, 0, false, "", nil
		}
		return bBinary, 0, 0, false, string(x), nil
	case json.RawMessage:
		if x == nil {
			return bUnset, 0, 0, false, "", nil
		}
		return bText, 0, 0, false, string(x), nil
	case time.Time:
		return bText, 0, 0, false, x.Format("2006-01-02 15:04:05.999999Z07:00"), nil
	case Date:
		return bText, 0, 0, false, x.Format("2006-01-02"), nil
	case Timestamp:
		return bText, 0, 0, false, x.Format("2006-01-02 15:04:05.999999"), nil
	case Decimal:
		return bText, 0, 0, false, x.String(), nil
	case UUID:
		return bText, 0, 0, false, x.String(), nil
	case Vector:
		if x == nil {
			return bUnset, 0, 0, false, "", nil
		}
		return bText, 0, 0, false, x.String(), nil
	case *big.Int:
		if x == nil {
			return bUnset, 0, 0, false, "", nil
		}
		return bText, 0, 0, false, x.String(), nil
	}
	p, err := classify(v, 0)
	if err != nil {
		return 0, 0, 0, false, "", err
	}
	switch p.tag {
	case tagNull:
		return bUnset, 0, 0, false, "", nil
	case tagInt8:
		return bInt, p.i, 0, false, "", nil
	case tagFloat8, tagFloat4:
		return bFloat, 0, p.f, false, "", nil
	case tagBool:
		return bBool, 0, 0, p.i != 0, "", nil
	case tagText, tagJSON, tagJSONB:
		return bText, 0, 0, false, p.s, nil
	case tagBytea:
		return bBinary, 0, 0, false, string(p.b), nil
	case tagNumeric:
		return bText, 0, 0, false, p.dec.String(), nil
	case tagUUID:
		var u UUID
		copy(u[:], p.b)
		return bText, 0, 0, false, u.String(), nil
	case tagTimestampTZ:
		return bText, 0, 0, false, timestamptzText(p.i, p.off), nil
	case tagTimestamp:
		return bText, 0, 0, false, timestampText(p.i), nil
	case tagDate:
		return bText, 0, 0, false, dateText(p.i), nil
	case tagArray:
		return bText, 0, 0, false, arrayExternalText(p.arr), nil
	}
	return 0, 0, 0, false, "", errors.New("unsupported CopyFrom value")
}

func arrowTypeOf(k ckind) (uint8, *fbTable) {
	switch k {
	case bInt:
		return atInt, &fbTable{fields: []fbField{fbI32(64), fbBool(true)}}
	case bFloat:
		return atFloatingPoint, &fbTable{fields: []fbField{fbI16(2)}}
	case bBool:
		return atBool, &fbTable{}
	case bBinary:
		return atBinary, &fbTable{}
	}
	return atUtf8, &fbTable{}
}

// encode emits the batch as an Arrow IPC stream (prefixed with the DATA
// subop) and resets the builders.
func (e *batchEncoder) encode() []byte {
	n := e.rows
	for j := range e.cols {
		if cb := &e.cols[j]; cb.kind == bUnset {
			cb.setKind(bText)
		}
	}
	fields := make(fbTableVec, len(e.cols))
	for j := range e.cols {
		tag, typ := arrowTypeOf(e.cols[j].kind)
		fields[j] = &fbTable{fields: []fbField{
			fbObj(fbString(e.names[j])),
			fbBool(true),
			fbU8(tag),
			fbObj(typ),
			fbAbsent(),
			fbObj(fbTableVec{}),
		}}
	}
	schema := &fbTable{fields: []fbField{fbI16(0), fbObj(fields)}}
	out := []byte{bulkData}
	out = encodeMessage(out, append([]byte(nil), e.w.finish(messageTable(hdrSchema, schema, 0))...), nil)

	bodyLen := 0
	for j := range e.cols {
		bodyLen += e.cols[j].bodySize(n)
	}
	body := make([]byte, 0, bodyLen)
	nodes := make([]byte, 0, 16*len(e.cols))
	bufs := make([]byte, 0, 48*len(e.cols))
	nbuf := 0
	addBuf := func(b []byte, size int) {
		var x [16]byte
		binary.LittleEndian.PutUint64(x[0:8], uint64(len(body)))
		binary.LittleEndian.PutUint64(x[8:16], uint64(size))
		bufs = append(bufs, x[:]...)
		body = append(body, b...)
		for i := len(b); i < size; i++ {
			body = append(body, 0)
		}
		for len(body)%8 != 0 {
			body = append(body, 0)
		}
		nbuf++
	}
	for j := range e.cols {
		cb := &e.cols[j]
		var x [16]byte
		binary.LittleEndian.PutUint64(x[0:8], uint64(n))
		binary.LittleEndian.PutUint64(x[8:16], uint64(cb.nulls))
		nodes = append(nodes, x[:]...)
		if cb.nulls > 0 {
			addBuf(cb.validity, (n+7)/8)
		} else {
			addBuf(nil, 0)
		}
		switch cb.kind {
		case bInt:
			start := len(body)
			addBuf(nil, 8*n)
			for i, v := range cb.ints {
				binary.LittleEndian.PutUint64(body[start+8*i:], uint64(v))
			}
		case bFloat:
			start := len(body)
			addBuf(nil, 8*n)
			for i, v := range cb.floats {
				binary.LittleEndian.PutUint64(body[start+8*i:], math.Float64bits(v))
			}
		case bBool:
			addBuf(cb.bools, (n+7)/8)
		default:
			start := len(body)
			addBuf(nil, 4*(n+1))
			for i, v := range cb.offs {
				binary.LittleEndian.PutUint32(body[start+4*i:], uint32(v))
			}
			addBuf(cb.data, len(cb.data))
		}
	}
	rb := &fbTable{fields: []fbField{
		fbI64(int64(n)),
		fbObj(fbStructVec{n: len(e.cols), data: nodes}),
		fbObj(fbStructVec{n: nbuf, data: bufs}),
	}}
	out = encodeMessage(out, e.w.finish(messageTable(hdrRecordBatch, rb, int64(len(body)))), body)
	out = append(out, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0)
	// reset, keeping capacity
	for j := range e.cols {
		cb := &e.cols[j]
		*cb = colBuilder{ints: cb.ints[:0], floats: cb.floats[:0], data: cb.data[:0], offs: cb.offs[:0], validity: cb.validity[:0], bools: cb.bools[:0]}
		for i := range cb.validity[:cap(cb.validity)] {
			cb.validity[:cap(cb.validity)][i] = 0
		}
		for i := range cb.bools[:cap(cb.bools)] {
			cb.bools[:cap(cb.bools)][i] = 0
		}
	}
	e.rows, e.bytes = 0, 0
	return out
}

func (cb *colBuilder) bodySize(n int) int {
	r8 := func(x int) int { return (x + 7) &^ 7 }
	s := r8((n + 7) / 8)
	switch cb.kind {
	case bInt, bFloat:
		s += r8(8 * n)
	case bBool:
		s += r8((n + 7) / 8)
	default:
		s += r8(4*(n+1)) + r8(len(cb.data))
	}
	return s
}
