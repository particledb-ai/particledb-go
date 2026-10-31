package legacy

// Arrow IPC stream codec for the PDB native wire, with no external dependency.
//
// WRITER (encodeArrowParams): the parameter payload of MSG_EXECUTE /
// MSG_EXECUTE_BATCH - one Schema message, one RecordBatch message, the
// end-of-stream marker. Column types: Int64, Float64, Utf8, Boolean, Binary,
// Timestamp(microsecond, "+00:00"); any value may be NULL. These are exactly the
// types the server's parameter decoder binds (decode_arrow_param); NULL is
// sent as a null slot of a typed column, never as the Arrow Null type (the
// server refuses that type).
//
// READER (decodeArrowStream): result streams the server writes (V5 metadata,
// uncompressed, dictionaries already decoded server-side). Supported column
// types: Null, Bool, Int(8..64, signed/unsigned), FloatingPoint(16/32/64),
// Utf8, LargeUtf8, Binary, LargeBinary, FixedSizeBinary, Decimal(128/256),
// Date, Time, Timestamp, Duration, Interval, List, LargeList, FixedSizeList,
// Struct, Map. Anything else (views, unions, run-end encoding, a dictionary
// batch, a compressed body) is an error, never a silently wrong value.
//
// Flatbuffers are written forward by hand (fbTable/fbWriter) and read with
// bounds-checked accessors (fbTab).

import (
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
)

// Arrow flatbuffer enum values (format/Schema.fbs, format/Message.fbs).
const (
	arrowTypeNull            = 1
	arrowTypeInt             = 2
	arrowTypeFloatingPoint   = 3
	arrowTypeBinary          = 4
	arrowTypeUtf8            = 5
	arrowTypeBool            = 6
	arrowTypeDecimal         = 7
	arrowTypeDate            = 8
	arrowTypeTime            = 9
	arrowTypeTimestamp       = 10
	arrowTypeInterval        = 11
	arrowTypeList            = 12
	arrowTypeStruct          = 13
	arrowTypeFixedSizeBinary = 15
	arrowTypeFixedSizeList   = 16
	arrowTypeMap             = 17
	arrowTypeDuration        = 18
	arrowTypeLargeBinary     = 19
	arrowTypeLargeUtf8       = 20
	arrowTypeLargeList       = 21

	arrowHeaderSchema          = 1
	arrowHeaderDictionaryBatch = 2
	arrowHeaderRecordBatch     = 3

	arrowMetadataV5 = 4

	arrowContinuation = 0xFFFFFFFF
)

// ===========================================================================
// Flatbuffer writer (forward layout: vtable, then table, then children, so
// every uoffset points forward)
// ===========================================================================

type fbKind int

const (
	fbNone fbKind = iota
	fbScalar
	fbRef
)

type fbField struct {
	kind fbKind
	size int // scalar byte width: 1, 2, 4 or 8
	val  uint64
	ref  interface{} // *fbTable | fbString | fbTableVec | fbStructVec
}

type fbTable struct{ fields []fbField }

type fbString string

type fbTableVec []*fbTable

// fbStructVec is a vector of inline structs whose elements need 8-byte alignment.
type fbStructVec struct {
	n    int
	data []byte
}

func fbU8(v uint8) fbField        { return fbField{kind: fbScalar, size: 1, val: uint64(v)} }
func fbI16(v int16) fbField       { return fbField{kind: fbScalar, size: 2, val: uint64(uint16(v))} }
func fbI32(v int32) fbField       { return fbField{kind: fbScalar, size: 4, val: uint64(uint32(v))} }
func fbI64(v int64) fbField       { return fbField{kind: fbScalar, size: 8, val: uint64(v)} }
func fbObj(o interface{}) fbField { return fbField{kind: fbRef, size: 4, ref: o} }
func fbAbsent() fbField           { return fbField{} }
func fbBool(v bool) fbField {
	if v {
		return fbU8(1)
	}
	return fbU8(0)
}

type fbWriter struct{ buf []byte }

func (w *fbWriter) pad(align int) {
	for len(w.buf)%align != 0 {
		w.buf = append(w.buf, 0)
	}
}

func (w *fbWriter) put16(pos int, v uint16) { binary.LittleEndian.PutUint16(w.buf[pos:], v) }
func (w *fbWriter) put32(pos int, v uint32) { binary.LittleEndian.PutUint32(w.buf[pos:], v) }

func (w *fbWriter) grow(n int) int {
	pos := len(w.buf)
	w.buf = append(w.buf, make([]byte, n)...)
	return pos
}

// finish serializes root as a complete flatbuffer.
func (w *fbWriter) finish(root *fbTable) []byte {
	w.buf = w.buf[:0]
	w.grow(4)
	t := w.writeObj(root)
	w.put32(0, uint32(t))
	return w.buf
}

func (w *fbWriter) writeObj(o interface{}) int {
	switch v := o.(type) {
	case *fbTable:
		return w.writeTable(v)
	case fbString:
		w.pad(4)
		pos := w.grow(4 + len(v) + 1)
		w.put32(pos, uint32(len(v)))
		copy(w.buf[pos+4:], v)
		return pos
	case fbTableVec:
		w.pad(4)
		pos := w.grow(4 + 4*len(v))
		w.put32(pos, uint32(len(v)))
		for i, t := range v {
			slot := pos + 4 + 4*i
			child := w.writeObj(t)
			w.put32(slot, uint32(child-slot))
		}
		return pos
	case fbStructVec:
		// Length prefix at P with (P+4) % 8 == 0 so the elements are 8-aligned.
		w.pad(4)
		if (len(w.buf)+4)%8 != 0 {
			w.grow(4)
		}
		pos := w.grow(4)
		w.put32(pos, uint32(v.n))
		w.buf = append(w.buf, v.data...)
		return pos
	}
	panic(fmt.Sprintf("fbWriter: unsupported object %T", o))
}

func (w *fbWriter) writeTable(t *fbTable) int {
	nslots := len(t.fields)
	for nslots > 0 && t.fields[nslots-1].kind == fbNone {
		nslots--
	}
	// vtable: [u16 vtable bytes][u16 inline table bytes][u16 field offset]*
	w.pad(2)
	vt := w.grow(4 + 2*nslots)
	w.put16(vt, uint16(4+2*nslots))

	has8 := false
	for _, f := range t.fields[:nslots] {
		if f.kind != fbNone && f.size == 8 {
			has8 = true
		}
	}
	// Table start T is 4-aligned; with an 8-byte field T % 8 == 4 so the
	// fields (which start right after the 4-byte soffset) are 8-aligned.
	w.pad(4)
	if has8 && len(w.buf)%8 != 4 {
		w.grow(4)
	}
	tpos := w.grow(4)
	w.put32(tpos, uint32(int32(tpos-vt)))

	fieldPos := make([]int, nslots)
	for _, size := range []int{8, 4, 2, 1} {
		for i, f := range t.fields[:nslots] {
			if f.kind == fbNone || f.size != size {
				continue
			}
			p := w.grow(size)
			fieldPos[i] = p
			switch size {
			case 1:
				w.buf[p] = byte(f.val)
			case 2:
				w.put16(p, uint16(f.val))
			case 4:
				w.put32(p, uint32(f.val))
			case 8:
				binary.LittleEndian.PutUint64(w.buf[p:], f.val)
			}
		}
	}
	w.put16(vt+2, uint16(len(w.buf)-tpos))
	for i, f := range t.fields[:nslots] {
		if f.kind != fbNone {
			w.put16(vt+4+2*i, uint16(fieldPos[i]-tpos))
		}
	}
	for i, f := range t.fields[:nslots] {
		if f.kind == fbRef {
			child := w.writeObj(f.ref)
			w.put32(fieldPos[i], uint32(child-fieldPos[i]))
		}
	}
	return tpos
}

// ===========================================================================
// Arrow IPC writer (parameter batches)
// ===========================================================================

// paramKind is the Arrow type a parameter column is sent as.
type paramKind int

const (
	pkUnset paramKind = iota // every value in the column is NULL (sent as Utf8)
	pkInt64
	pkFloat64
	pkUtf8
	pkBool
	pkBinary
	pkTimestamp
)

// normalizeParam converts a Go value to (kind, canonical value). nil means NULL.
func normalizeParam(v interface{}) (paramKind, interface{}, error) {
	switch x := v.(type) {
	case nil:
		return pkUnset, nil, nil
	case int:
		return pkInt64, int64(x), nil
	case int8:
		return pkInt64, int64(x), nil
	case int16:
		return pkInt64, int64(x), nil
	case int32:
		return pkInt64, int64(x), nil
	case int64:
		return pkInt64, x, nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return 0, nil, fmt.Errorf("uint value %d overflows int64", x)
		}
		return pkInt64, int64(x), nil
	case uint8:
		return pkInt64, int64(x), nil
	case uint16:
		return pkInt64, int64(x), nil
	case uint32:
		return pkInt64, int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, nil, fmt.Errorf("uint64 value %d overflows int64", x)
		}
		return pkInt64, int64(x), nil
	case float32:
		return pkFloat64, float64(x), nil
	case float64:
		return pkFloat64, x, nil
	case string:
		return pkUtf8, x, nil
	case []byte:
		if x == nil {
			return pkUnset, nil, nil
		}
		return pkBinary, x, nil
	case bool:
		return pkBool, x, nil
	case time.Time:
		return pkTimestamp, x.UTC().UnixMicro(), nil
	case *string:
		if x == nil {
			return pkUnset, nil, nil
		}
		return pkUtf8, *x, nil
	case *int64:
		if x == nil {
			return pkUnset, nil, nil
		}
		return pkInt64, *x, nil
	case *float64:
		if x == nil {
			return pkUnset, nil, nil
		}
		return pkFloat64, *x, nil
	case *bool:
		if x == nil {
			return pkUnset, nil, nil
		}
		return pkBool, *x, nil
	}
	if dv, ok := v.(driver.Valuer); ok {
		inner, err := dv.Value()
		if err != nil {
			return 0, nil, err
		}
		if _, again := inner.(driver.Valuer); again {
			return 0, nil, fmt.Errorf("driver.Valuer %T returned another Valuer", v)
		}
		return normalizeParam(inner)
	}
	return 0, nil, fmt.Errorf("unsupported parameter type %T (supported: integers, floats, string, []byte, bool, time.Time, nil, driver.Valuer)", v)
}

// paramColumn is one encoded parameter column.
type paramColumn struct {
	kind  paramKind
	vals  []interface{}
	nulls int
}

// maxExactFloatInt is 2^53: every integer of at most this magnitude is exact
// in a float64.
const maxExactFloatInt = int64(1) << 53

// buildParamColumns transposes rows into typed columns. Every row must have
// nCols values. A column's type is the type of its non-NULL values; integers
// and floats mixed in one column are sent as Float64 (an integer above 2^53,
// which a float64 cannot hold exactly, is refused); any other mix is an error.
func buildParamColumns(rows [][]interface{}, nCols int) ([]paramColumn, error) {
	cols := make([]paramColumn, nCols)
	for c := range cols {
		cols[c].vals = make([]interface{}, len(rows))
	}
	for r, row := range rows {
		if len(row) != nCols {
			return nil, fmt.Errorf("particledb: row %d has %d values, expected %d", r, len(row), nCols)
		}
		for c, v := range row {
			k, nv, err := normalizeParam(v)
			if err != nil {
				return nil, fmt.Errorf("particledb: row %d parameter $%d: %w", r, c+1, err)
			}
			col := &cols[c]
			col.vals[r] = nv
			if k == pkUnset {
				col.nulls++
				continue
			}
			switch {
			case col.kind == pkUnset || col.kind == k:
				col.kind = k
			case (col.kind == pkInt64 && k == pkFloat64) || (col.kind == pkFloat64 && k == pkInt64):
				col.kind = pkFloat64
			default:
				return nil, fmt.Errorf("particledb: parameter $%d mixes %s and %s values across rows", c+1, col.kind, k)
			}
		}
	}
	for c := range cols {
		if cols[c].kind == pkFloat64 {
			for r, v := range cols[c].vals {
				if i, ok := v.(int64); ok {
					// Promoting an integer above 2^53 to Float64 would bind a
					// different value than the caller passed: refuse it.
					if i > maxExactFloatInt || i < -maxExactFloatInt {
						return nil, fmt.Errorf("particledb: row %d parameter $%d: integer %d mixed with floats "+
							"in one column cannot be sent as Float64 exactly (|v| > 2^53); pass a float64 or keep the column integer", r, c+1, i)
					}
					cols[c].vals[r] = float64(i)
				}
			}
		}
	}
	return cols, nil
}

func (k paramKind) String() string {
	switch k {
	case pkInt64:
		return "int64"
	case pkFloat64:
		return "float64"
	case pkUtf8:
		return "string"
	case pkBool:
		return "bool"
	case pkBinary:
		return "[]byte"
	case pkTimestamp:
		return "time.Time"
	}
	return "NULL"
}

// arrowTypeFor returns (Type union tag, type table) for a parameter kind.
func arrowTypeFor(k paramKind) (uint8, *fbTable) {
	switch k {
	case pkInt64:
		return arrowTypeInt, &fbTable{fields: []fbField{fbI32(64), fbBool(true)}}
	case pkFloat64:
		return arrowTypeFloatingPoint, &fbTable{fields: []fbField{fbI16(2)}} // DOUBLE
	case pkBool:
		return arrowTypeBool, &fbTable{}
	case pkBinary:
		return arrowTypeBinary, &fbTable{}
	case pkTimestamp:
		// MICROSECOND, timezone "+00:00" (the server resolves offset zones only).
		return arrowTypeTimestamp, &fbTable{fields: []fbField{fbI16(2), fbObj(fbString("+00:00"))}}
	}
	return arrowTypeUtf8, &fbTable{} // pkUtf8 and the all-NULL column
}

// encodeMessage frames one IPC message: continuation marker, metadata length,
// metadata padded to 8 bytes, body.
func encodeMessage(out []byte, meta []byte, body []byte) []byte {
	padded := (len(meta) + 7) &^ 7
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], arrowContinuation)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(padded))
	out = append(out, hdr[:]...)
	out = append(out, meta...)
	out = append(out, make([]byte, padded-len(meta))...)
	return append(out, body...)
}

func messageTable(headerType uint8, header *fbTable, bodyLen int64) *fbTable {
	return &fbTable{fields: []fbField{
		fbI16(arrowMetadataV5),
		fbU8(headerType),
		fbObj(header),
		fbI64(bodyLen),
	}}
}

// encodeArrowParams encodes rows (each with nCols values) as an Arrow IPC
// stream: Schema, one RecordBatch, end-of-stream. Columns are named p1..pN.
func encodeArrowParams(rows [][]interface{}, nCols int) ([]byte, error) {
	cols, err := buildParamColumns(rows, nCols)
	if err != nil {
		return nil, err
	}
	nrows := len(rows)

	// --- Schema message ---
	fields := make(fbTableVec, nCols)
	for c := range cols {
		tag, typ := arrowTypeFor(cols[c].kind)
		fields[c] = &fbTable{fields: []fbField{
			fbObj(fbString(fmt.Sprintf("p%d", c+1))),
			fbBool(true),
			fbU8(tag),
			fbObj(typ),
			fbAbsent(),
			fbObj(fbTableVec{}),
		}}
	}
	schema := &fbTable{fields: []fbField{fbI16(0), fbObj(fields)}}
	var w fbWriter
	out := encodeMessage(nil, append([]byte(nil), w.finish(messageTable(arrowHeaderSchema, schema, 0))...), nil)

	// --- RecordBatch message ---
	var body []byte
	var nodes, bufs []byte
	addBuf := func(b []byte) {
		var e [16]byte
		binary.LittleEndian.PutUint64(e[0:8], uint64(len(body)))
		binary.LittleEndian.PutUint64(e[8:16], uint64(len(b)))
		bufs = append(bufs, e[:]...)
		body = append(body, b...)
		for len(body)%8 != 0 {
			body = append(body, 0)
		}
	}
	nbuf := 0
	for c := range cols {
		col := &cols[c]
		var e [16]byte
		binary.LittleEndian.PutUint64(e[0:8], uint64(nrows))
		binary.LittleEndian.PutUint64(e[8:16], uint64(col.nulls))
		nodes = append(nodes, e[:]...)

		if col.nulls > 0 {
			bm := make([]byte, (nrows+7)/8)
			for r, v := range col.vals {
				if v != nil {
					bm[r/8] |= 1 << (r % 8)
				}
			}
			addBuf(bm)
		} else {
			addBuf(nil)
		}
		nbuf++
		switch col.kind {
		case pkInt64, pkTimestamp:
			b := make([]byte, 8*nrows)
			for r, v := range col.vals {
				if v != nil {
					binary.LittleEndian.PutUint64(b[8*r:], uint64(v.(int64)))
				}
			}
			addBuf(b)
			nbuf++
		case pkFloat64:
			b := make([]byte, 8*nrows)
			for r, v := range col.vals {
				if v != nil {
					binary.LittleEndian.PutUint64(b[8*r:], math.Float64bits(v.(float64)))
				}
			}
			addBuf(b)
			nbuf++
		case pkBool:
			b := make([]byte, (nrows+7)/8)
			for r, v := range col.vals {
				if v != nil && v.(bool) {
					b[r/8] |= 1 << (r % 8)
				}
			}
			addBuf(b)
			nbuf++
		default: // pkUtf8, pkBinary, pkUnset: [i32 offsets][data]
			offs := make([]byte, 4*(nrows+1))
			var data []byte
			for r, v := range col.vals {
				switch s := v.(type) {
				case string:
					data = append(data, s...)
				case []byte:
					data = append(data, s...)
				}
				if len(data) > math.MaxInt32 {
					return nil, errors.New("particledb: a parameter column exceeds 2 GiB")
				}
				binary.LittleEndian.PutUint32(offs[4*(r+1):], uint32(len(data)))
			}
			addBuf(offs)
			addBuf(data)
			nbuf += 2
		}
	}
	rb := &fbTable{fields: []fbField{
		fbI64(int64(nrows)),
		fbObj(fbStructVec{n: nCols, data: nodes}),
		fbObj(fbStructVec{n: nbuf, data: bufs}),
	}}
	out = encodeMessage(out, w.finish(messageTable(arrowHeaderRecordBatch, rb, int64(len(body)))), body)

	// --- End of stream ---
	out = append(out, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0)
	return out, nil
}

// ===========================================================================
// Flatbuffer reader
// ===========================================================================

// errArrowDecode is raised (via panic) by bounds-checked reads and recovered
// into an error at the decodeArrowStream boundary.
type errArrowDecode struct{ msg string }

func arrowFail(format string, args ...interface{}) {
	panic(errArrowDecode{msg: fmt.Sprintf(format, args...)})
}

type fbTab struct {
	b   []byte
	pos int
}

func fbCheck(b []byte, pos, n int) {
	if pos < 0 || n < 0 || pos+n > len(b) || pos+n < pos {
		arrowFail("flatbuffer read out of bounds (pos %d, len %d, buffer %d)", pos, n, len(b))
	}
}

func fbU32(b []byte, pos int) uint32 { fbCheck(b, pos, 4); return binary.LittleEndian.Uint32(b[pos:]) }
func fbU16(b []byte, pos int) uint16 { fbCheck(b, pos, 2); return binary.LittleEndian.Uint16(b[pos:]) }

func fbRoot(b []byte) fbTab { return fbTab{b: b, pos: int(fbU32(b, 0))} }

func (t fbTab) slot(i int) int {
	vt := t.pos - int(int32(fbU32(t.b, t.pos)))
	vtSize := int(fbU16(t.b, vt))
	o := 4 + 2*i
	if o+2 > vtSize {
		return 0
	}
	return int(fbU16(t.b, vt+o))
}

func (t fbTab) u8(i int, def uint8) uint8 {
	if o := t.slot(i); o != 0 {
		fbCheck(t.b, t.pos+o, 1)
		return t.b[t.pos+o]
	}
	return def
}

func (t fbTab) i16(i int, def int16) int16 {
	if o := t.slot(i); o != 0 {
		return int16(fbU16(t.b, t.pos+o))
	}
	return def
}

func (t fbTab) i32(i int, def int32) int32 {
	if o := t.slot(i); o != 0 {
		return int32(fbU32(t.b, t.pos+o))
	}
	return def
}

func (t fbTab) i64(i int, def int64) int64 {
	if o := t.slot(i); o != 0 {
		fbCheck(t.b, t.pos+o, 8)
		return int64(binary.LittleEndian.Uint64(t.b[t.pos+o:]))
	}
	return def
}

func (t fbTab) ref(i int) (int, bool) {
	o := t.slot(i)
	if o == 0 {
		return 0, false
	}
	p := t.pos + o
	return p + int(fbU32(t.b, p)), true
}

func (t fbTab) table(i int) (fbTab, bool) {
	p, ok := t.ref(i)
	if !ok {
		return fbTab{}, false
	}
	return fbTab{b: t.b, pos: p}, true
}

func (t fbTab) str(i int) string {
	p, ok := t.ref(i)
	if !ok {
		return ""
	}
	n := int(fbU32(t.b, p))
	fbCheck(t.b, p+4, n)
	return string(t.b[p+4 : p+4+n])
}

// vec returns (first element position, element count).
func (t fbTab) vec(i int) (int, int) {
	p, ok := t.ref(i)
	if !ok {
		return 0, 0
	}
	return p + 4, int(fbU32(t.b, p))
}

func (t fbTab) vecTable(i, j int) fbTab {
	start, n := t.vec(i)
	if j >= n {
		arrowFail("flatbuffer vector index %d out of range %d", j, n)
	}
	p := start + 4*j
	return fbTab{b: t.b, pos: p + int(fbU32(t.b, p))}
}

// ===========================================================================
// Arrow IPC reader (results)
// ===========================================================================

type arrowField struct {
	name     string
	typ      uint8
	t        fbTab // type table
	children []*arrowField
}

func parseArrowField(f fbTab) *arrowField {
	if _, ok := f.table(4); ok {
		arrowFail("dictionary-encoded field %q is not supported", f.str(0))
	}
	af := &arrowField{name: f.str(0), typ: f.u8(2, 0)}
	if tt, ok := f.table(3); ok {
		af.t = tt
	}
	_, n := f.vec(5)
	for j := 0; j < n; j++ {
		af.children = append(af.children, parseArrowField(f.vecTable(5, j)))
	}
	return af
}

// batchCursor walks a RecordBatch's field nodes and buffers in schema pre-order.
type batchCursor struct {
	rb       fbTab
	body     []byte
	node     int
	buf      int
	nodesPos int
	nNodes   int
	bufsPos  int
	nBufs    int
}

func (c *batchCursor) nextNode() (length, nulls int) {
	if c.node >= c.nNodes {
		arrowFail("record batch has fewer field nodes than the schema needs")
	}
	p := c.nodesPos + 16*c.node
	fbCheck(c.rb.b, p, 16)
	c.node++
	length = int(int64(binary.LittleEndian.Uint64(c.rb.b[p:])))
	nulls = int(int64(binary.LittleEndian.Uint64(c.rb.b[p+8:])))
	if length < 0 || nulls < 0 {
		arrowFail("negative field node length")
	}
	return
}

func (c *batchCursor) nextBuf() []byte {
	if c.buf >= c.nBufs {
		arrowFail("record batch has fewer buffers than the schema needs")
	}
	p := c.bufsPos + 16*c.buf
	fbCheck(c.rb.b, p, 16)
	c.buf++
	off := int(int64(binary.LittleEndian.Uint64(c.rb.b[p:])))
	n := int(int64(binary.LittleEndian.Uint64(c.rb.b[p+8:])))
	fbCheck(c.body, off, n)
	return c.body[off : off+n]
}

func bitSet(b []byte, i int) bool {
	if i/8 >= len(b) {
		arrowFail("bitmap too short")
	}
	return b[i/8]&(1<<(i%8)) != 0
}

func need(b []byte, n int, what string) {
	if len(b) < n {
		arrowFail("%s buffer too short (%d < %d)", what, len(b), n)
	}
}

// decodeColumn decodes `length` values of field f (and its children).
func (c *batchCursor) decodeColumn(f *arrowField) []interface{} {
	length, nulls := c.nextNode()
	out := make([]interface{}, length)
	if f.typ == arrowTypeNull {
		return out
	}
	validity := c.nextBuf()
	valid := func(i int) bool { return nulls == 0 || len(validity) == 0 || bitSet(validity, i) }
	if nulls > 0 && len(validity) == 0 {
		arrowFail("field %q has %d nulls but no validity bitmap", f.name, nulls)
	}

	switch f.typ {
	case arrowTypeBool:
		data := c.nextBuf()
		need(data, (length+7)/8, "bool")
		for i := range out {
			if valid(i) {
				out[i] = bitSet(data, i)
			}
		}
	case arrowTypeInt:
		width := int(f.t.i32(0, 0))
		signed := f.t.u8(1, 0) != 0
		bw := width / 8
		if bw != 1 && bw != 2 && bw != 4 && bw != 8 {
			arrowFail("unsupported Int bit width %d", width)
		}
		data := c.nextBuf()
		need(data, bw*length, "int")
		for i := range out {
			if !valid(i) {
				continue
			}
			p := data[bw*i:]
			switch {
			case bw == 1 && signed:
				out[i] = int64(int8(p[0]))
			case bw == 1:
				out[i] = int64(p[0])
			case bw == 2 && signed:
				out[i] = int64(int16(binary.LittleEndian.Uint16(p)))
			case bw == 2:
				out[i] = int64(binary.LittleEndian.Uint16(p))
			case bw == 4 && signed:
				out[i] = int64(int32(binary.LittleEndian.Uint32(p)))
			case bw == 4:
				out[i] = int64(binary.LittleEndian.Uint32(p))
			case signed:
				out[i] = int64(binary.LittleEndian.Uint64(p))
			default:
				u := binary.LittleEndian.Uint64(p)
				if u <= math.MaxInt64 {
					out[i] = int64(u)
				} else {
					out[i] = u
				}
			}
		}
	case arrowTypeFloatingPoint:
		prec := f.t.i16(0, 0)
		data := c.nextBuf()
		bw := map[int16]int{0: 2, 1: 4, 2: 8}[prec]
		if bw == 0 {
			arrowFail("unsupported float precision %d", prec)
		}
		need(data, bw*length, "float")
		for i := range out {
			if !valid(i) {
				continue
			}
			switch bw {
			case 2:
				out[i] = halfToFloat64(binary.LittleEndian.Uint16(data[2*i:]))
			case 4:
				out[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(data[4*i:])))
			default:
				out[i] = math.Float64frombits(binary.LittleEndian.Uint64(data[8*i:]))
			}
		}
	case arrowTypeUtf8, arrowTypeBinary, arrowTypeLargeUtf8, arrowTypeLargeBinary:
		large := f.typ == arrowTypeLargeUtf8 || f.typ == arrowTypeLargeBinary
		offs := c.nextBuf()
		data := c.nextBuf()
		ow := 4
		if large {
			ow = 8
		}
		if length > 0 {
			need(offs, ow*(length+1), "offsets")
		}
		off := func(i int) int {
			if large {
				return int(int64(binary.LittleEndian.Uint64(offs[8*i:])))
			}
			return int(int32(binary.LittleEndian.Uint32(offs[4*i:])))
		}
		for i := range out {
			if !valid(i) {
				continue
			}
			s, e := off(i), off(i+1)
			if s < 0 || e < s || e > len(data) {
				arrowFail("field %q: bad offsets %d..%d (data %d)", f.name, s, e, len(data))
			}
			if f.typ == arrowTypeUtf8 || f.typ == arrowTypeLargeUtf8 {
				out[i] = string(data[s:e])
			} else {
				out[i] = append(make([]byte, 0, e-s), data[s:e]...)
			}
		}
	case arrowTypeFixedSizeBinary:
		bw := int(f.t.i32(0, 0))
		data := c.nextBuf()
		need(data, bw*length, "fixed-size binary")
		for i := range out {
			if valid(i) {
				out[i] = append(make([]byte, 0, bw), data[bw*i:bw*(i+1)]...)
			}
		}
	case arrowTypeDecimal:
		scale := int(f.t.i32(1, 0))
		bits := int(f.t.i32(2, 128))
		bw := bits / 8
		if bw != 16 && bw != 32 {
			arrowFail("unsupported Decimal bit width %d", bits)
		}
		data := c.nextBuf()
		need(data, bw*length, "decimal")
		for i := range out {
			if valid(i) {
				out[i] = formatDecimal(data[bw*i:bw*(i+1)], scale)
			}
		}
	case arrowTypeDate:
		unit := f.t.i16(0, 1) // default MILLISECOND
		data := c.nextBuf()
		for i := range out {
			if !valid(i) {
				continue
			}
			if unit == 0 {
				need(data, 4*length, "date32")
				days := int64(int32(binary.LittleEndian.Uint32(data[4*i:])))
				out[i] = time.Unix(days*86400, 0).UTC()
			} else {
				need(data, 8*length, "date64")
				out[i] = time.UnixMilli(int64(binary.LittleEndian.Uint64(data[8*i:]))).UTC()
			}
		}
	case arrowTypeTime:
		unit := f.t.i16(0, 1)
		bits := int(f.t.i32(1, 32))
		data := c.nextBuf()
		need(data, bits/8*length, "time")
		for i := range out {
			if !valid(i) {
				continue
			}
			var v int64
			if bits == 64 {
				v = int64(binary.LittleEndian.Uint64(data[8*i:]))
			} else {
				v = int64(int32(binary.LittleEndian.Uint32(data[4*i:])))
			}
			out[i] = formatTimeOfDay(time.Duration(v) * unitDuration(unit))
		}
	case arrowTypeTimestamp:
		unit := f.t.i16(0, 0)
		data := c.nextBuf()
		need(data, 8*length, "timestamp")
		for i := range out {
			if valid(i) {
				v := int64(binary.LittleEndian.Uint64(data[8*i:]))
				out[i] = timeFromUnit(v, unit)
			}
		}
	case arrowTypeDuration:
		unit := f.t.i16(0, 1)
		data := c.nextBuf()
		need(data, 8*length, "duration")
		for i := range out {
			if valid(i) {
				out[i] = time.Duration(int64(binary.LittleEndian.Uint64(data[8*i:]))) * unitDuration(unit)
			}
		}
	case arrowTypeInterval:
		unit := f.t.i16(0, 0)
		data := c.nextBuf()
		bw := map[int16]int{0: 4, 1: 8, 2: 16}[unit]
		if bw == 0 {
			arrowFail("unsupported Interval unit %d", unit)
		}
		need(data, bw*length, "interval")
		for i := range out {
			if !valid(i) {
				continue
			}
			p := data[bw*i:]
			switch unit {
			case 0:
				out[i] = fmt.Sprintf("%d mons", int32(binary.LittleEndian.Uint32(p)))
			case 1:
				out[i] = fmt.Sprintf("%d days %s", int32(binary.LittleEndian.Uint32(p)),
					time.Duration(int32(binary.LittleEndian.Uint32(p[4:])))*time.Millisecond)
			default:
				out[i] = fmt.Sprintf("%d mons %d days %s", int32(binary.LittleEndian.Uint32(p)),
					int32(binary.LittleEndian.Uint32(p[4:])), time.Duration(int64(binary.LittleEndian.Uint64(p[8:]))))
			}
		}
	case arrowTypeList, arrowTypeLargeList, arrowTypeMap:
		if len(f.children) != 1 {
			arrowFail("list field %q needs exactly one child", f.name)
		}
		large := f.typ == arrowTypeLargeList
		offs := c.nextBuf()
		child := c.decodeColumn(f.children[0])
		ow := 4
		if large {
			ow = 8
		}
		if length > 0 {
			need(offs, ow*(length+1), "list offsets")
		}
		for i := range out {
			if !valid(i) {
				continue
			}
			var s, e int
			if large {
				s, e = int(int64(binary.LittleEndian.Uint64(offs[8*i:]))), int(int64(binary.LittleEndian.Uint64(offs[8*i+8:])))
			} else {
				s, e = int(int32(binary.LittleEndian.Uint32(offs[4*i:]))), int(int32(binary.LittleEndian.Uint32(offs[4*i+4:])))
			}
			if s < 0 || e < s || e > len(child) {
				arrowFail("list field %q: bad offsets", f.name)
			}
			out[i] = append(make([]interface{}, 0, e-s), child[s:e]...)
		}
	case arrowTypeFixedSizeList:
		if len(f.children) != 1 {
			arrowFail("list field %q needs exactly one child", f.name)
		}
		n := int(f.t.i32(0, 0))
		child := c.decodeColumn(f.children[0])
		if len(child) < n*length {
			arrowFail("fixed-size list %q: child too short", f.name)
		}
		for i := range out {
			if valid(i) {
				out[i] = append(make([]interface{}, 0, n), child[n*i:n*(i+1)]...)
			}
		}
	case arrowTypeStruct:
		kids := make([][]interface{}, len(f.children))
		for j, ch := range f.children {
			kids[j] = c.decodeColumn(ch)
			if len(kids[j]) < length {
				arrowFail("struct %q: child too short", f.name)
			}
		}
		for i := range out {
			if !valid(i) {
				continue
			}
			m := make(map[string]interface{}, len(f.children))
			for j, ch := range f.children {
				m[ch.name] = kids[j][i]
			}
			out[i] = m
		}
	default:
		arrowFail("unsupported Arrow type tag %d for column %q", f.typ, f.name)
	}
	return out
}

func unitDuration(unit int16) time.Duration {
	switch unit {
	case 0:
		return time.Second
	case 1:
		return time.Millisecond
	case 2:
		return time.Microsecond
	}
	return time.Nanosecond
}

func timeFromUnit(v int64, unit int16) time.Time {
	switch unit {
	case 0:
		return time.Unix(v, 0).UTC()
	case 1:
		return time.UnixMilli(v).UTC()
	case 2:
		return time.UnixMicro(v).UTC()
	}
	return time.Unix(0, v).UTC()
}

func formatTimeOfDay(d time.Duration) string {
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	d -= s * time.Second
	if d == 0 {
		return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
	}
	frac := strings.TrimRight(fmt.Sprintf("%09d", int64(d)), "0")
	return fmt.Sprintf("%02d:%02d:%02d.%s", h, m, s, frac)
}

// formatDecimal renders a little-endian two's-complement decimal as text.
func formatDecimal(le []byte, scale int) string {
	be := make([]byte, len(le))
	for i := range le {
		be[len(le)-1-i] = le[i]
	}
	v := new(big.Int).SetBytes(be)
	if be[0]&0x80 != 0 {
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(8*len(le))))
	}
	neg := v.Sign() < 0
	digits := new(big.Int).Abs(v).String()
	if scale > 0 {
		for len(digits) <= scale {
			digits = "0" + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	} else if scale < 0 {
		digits += strings.Repeat("0", -scale)
	}
	if neg {
		return "-" + digits
	}
	return digits
}

func halfToFloat64(h uint16) float64 {
	sign := float64(1)
	if h&0x8000 != 0 {
		sign = -1
	}
	exp := int((h >> 10) & 0x1f)
	frac := float64(h & 0x3ff)
	switch exp {
	case 0:
		return sign * math.Ldexp(frac, -24)
	case 31:
		if frac == 0 {
			return sign * math.Inf(1)
		}
		return math.NaN()
	}
	return sign * math.Ldexp(1+frac/1024, exp-15)
}

// arrowResult is a decoded result stream.
type arrowResult struct {
	columns []string
	rows    []Row
}

// isArrowIPCStream reports whether data starts like an IPC stream message.
func isArrowIPCStream(data []byte) bool {
	return len(data) >= 8 && binary.LittleEndian.Uint32(data) == arrowContinuation
}

// decodeArrowStream decodes a complete Arrow IPC stream into named columns
// and rows. Any malformed or unsupported content is an error.
func decodeArrowStream(data []byte) (res *arrowResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(errArrowDecode); ok {
				res, err = nil, fmt.Errorf("arrow ipc: %s", e.msg)
				return
			}
			panic(r)
		}
	}()
	res = &arrowResult{}
	var fields []*arrowField
	off := 0
	for off < len(data) {
		fbCheck(data, off, 4)
		metaLen := binary.LittleEndian.Uint32(data[off:])
		off += 4
		if metaLen == arrowContinuation {
			fbCheck(data, off, 4)
			metaLen = binary.LittleEndian.Uint32(data[off:])
			off += 4
		}
		if metaLen == 0 {
			break // end of stream
		}
		if int32(metaLen) < 0 {
			arrowFail("negative metadata length")
		}
		fbCheck(data, off, int(metaLen))
		meta := data[off : off+int(metaLen)]
		off += int(metaLen)
		msg := fbRoot(meta)
		bodyLen := msg.i64(3, 0)
		if bodyLen < 0 {
			arrowFail("negative body length")
		}
		fbCheck(data, off, int(bodyLen))
		body := data[off : off+int(bodyLen)]
		off += int(bodyLen)
		header, ok := msg.table(2)
		switch msg.u8(1, 0) {
		case arrowHeaderSchema:
			if !ok {
				arrowFail("schema message without header")
			}
			if header.i16(0, 0) != 0 {
				arrowFail("big-endian streams are not supported")
			}
			_, n := header.vec(1)
			fields = make([]*arrowField, n)
			res.columns = make([]string, n)
			for j := 0; j < n; j++ {
				fields[j] = parseArrowField(header.vecTable(1, j))
				res.columns[j] = fields[j].name
			}
		case arrowHeaderRecordBatch:
			if !ok || fields == nil {
				arrowFail("record batch before schema")
			}
			if _, compressed := header.table(3); compressed {
				arrowFail("compressed record batches are not supported")
			}
			cur := &batchCursor{rb: header, body: body}
			cur.nodesPos, cur.nNodes = header.vec(1)
			cur.bufsPos, cur.nBufs = header.vec(2)
			nrows := int(header.i64(0, 0))
			cols := make([][]interface{}, len(fields))
			for j, f := range fields {
				cols[j] = cur.decodeColumn(f)
				if len(cols[j]) < nrows {
					arrowFail("column %q has %d values for %d rows", f.name, len(cols[j]), nrows)
				}
			}
			for i := 0; i < nrows; i++ {
				row := make(Row, len(fields))
				for j, f := range fields {
					row[f.name] = cols[j][i]
				}
				res.rows = append(res.rows, row)
			}
		case arrowHeaderDictionaryBatch:
			arrowFail("dictionary batches are not supported")
		default:
			arrowFail("unsupported message header type %d", msg.u8(1, 0))
		}
	}
	return res, nil
}
