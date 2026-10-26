package particledb

// A small, allocation-light Arrow IPC stream reader for query results.
//
// Results arrive as an Arrow IPC stream: a schema message, record batches,
// and an optional end-of-stream marker. Columns are read in place: a decoded
// column is a set of slices into the frame payload, and a cell is converted
// only when it is scanned. Nothing here allocates per row.

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	arrowContinuation = 0xFFFFFFFF

	hdrSchema      = 1
	hdrDictionary  = 2
	hdrRecordBatch = 3

	atNull            = 1
	atInt             = 2
	atFloatingPoint   = 3
	atBinary          = 4
	atUtf8            = 5
	atBool            = 6
	atDecimal         = 7
	atDate            = 8
	atTime            = 9
	atTimestamp       = 10
	atInterval        = 11
	atList            = 12
	atStruct          = 13
	atFixedSizeBinary = 15
	atFixedSizeList   = 16
	atMap             = 17
	atDuration        = 18
	atLargeBinary     = 19
	atLargeUtf8       = 20
	atLargeList       = 21
)

// arrowError is raised by bounds-checked reads and recovered at the API edge.
type arrowError struct{ msg string }

func arrowFail(format string, args ...any) { panic(arrowError{fmt.Sprintf(format, args...)}) }

// recoverArrow turns an arrowError panic into *err.
func recoverArrow(err *error) {
	if r := recover(); r != nil {
		if e, ok := r.(arrowError); ok {
			*err = protoErr("malformed Arrow result: %s", e.msg)
			return
		}
		panic(r)
	}
}

type fbt struct {
	b   []byte
	pos int
}

func fbChk(b []byte, pos, n int) {
	if pos < 0 || n < 0 || pos+n > len(b) || pos+n < pos {
		arrowFail("flatbuffer read out of bounds (pos %d, len %d, buffer %d)", pos, n, len(b))
	}
}

func fbU32(b []byte, pos int) uint32 { fbChk(b, pos, 4); return binary.LittleEndian.Uint32(b[pos:]) }
func fbU16(b []byte, pos int) uint16 { fbChk(b, pos, 2); return binary.LittleEndian.Uint16(b[pos:]) }
func fbRoot(b []byte) fbt            { return fbt{b: b, pos: int(fbU32(b, 0))} }

func (t fbt) slot(i int) int {
	vt := t.pos - int(int32(fbU32(t.b, t.pos)))
	n := int(fbU16(t.b, vt))
	o := 4 + 2*i
	if o+2 > n {
		return 0
	}
	return int(fbU16(t.b, vt+o))
}

func (t fbt) u8(i int, def uint8) uint8 {
	if o := t.slot(i); o != 0 {
		fbChk(t.b, t.pos+o, 1)
		return t.b[t.pos+o]
	}
	return def
}

func (t fbt) i16(i int, def int16) int16 {
	if o := t.slot(i); o != 0 {
		return int16(fbU16(t.b, t.pos+o))
	}
	return def
}

func (t fbt) i32(i int, def int32) int32 {
	if o := t.slot(i); o != 0 {
		return int32(fbU32(t.b, t.pos+o))
	}
	return def
}

func (t fbt) i64(i int, def int64) int64 {
	if o := t.slot(i); o != 0 {
		fbChk(t.b, t.pos+o, 8)
		return int64(binary.LittleEndian.Uint64(t.b[t.pos+o:]))
	}
	return def
}

func (t fbt) ref(i int) (int, bool) {
	o := t.slot(i)
	if o == 0 {
		return 0, false
	}
	p := t.pos + o
	return p + int(fbU32(t.b, p)), true
}

func (t fbt) table(i int) (fbt, bool) {
	p, ok := t.ref(i)
	if !ok {
		return fbt{}, false
	}
	return fbt{b: t.b, pos: p}, true
}

func (t fbt) str(i int) string {
	p, ok := t.ref(i)
	if !ok {
		return ""
	}
	n := int(fbU32(t.b, p))
	fbChk(t.b, p+4, n)
	return string(t.b[p+4 : p+4+n])
}

func (t fbt) vec(i int) (int, int) {
	p, ok := t.ref(i)
	if !ok {
		return 0, 0
	}
	return p + 4, int(fbU32(t.b, p))
}

func (t fbt) vecTable(i, j int) fbt {
	start, n := t.vec(i)
	if j >= n {
		arrowFail("flatbuffer vector index %d out of range %d", j, n)
	}
	p := start + 4*j
	return fbt{b: t.b, pos: p + int(fbU32(t.b, p))}
}

// arrowField is one schema field with its type parameters and metadata.
type arrowField struct {
	name     string
	nullable bool
	typ      uint8
	bitWidth int   // Int, Decimal, Time
	signed   bool  // Int
	prec     int16 // FloatingPoint precision; Decimal precision in dprec
	dprec    int
	dscale   int
	unit     int16  // Date, Time, Timestamp, Duration, Interval
	tz       string // Timestamp
	size     int    // FixedSizeBinary width, FixedSizeList length
	decl     string // pdb:decl metadata (declared SQL type)
	children []*arrowField
}

type arrowSchema struct {
	fields []*arrowField
}

func parseField(f fbt) *arrowField {
	if _, ok := f.table(4); ok {
		arrowFail("dictionary-encoded field %q is not supported", f.str(0))
	}
	af := &arrowField{name: f.str(0), nullable: f.u8(1, 0) != 0, typ: f.u8(2, 0)}
	tt, hasT := f.table(3)
	switch af.typ {
	case atInt:
		if hasT {
			af.bitWidth = int(tt.i32(0, 0))
			af.signed = tt.u8(1, 0) != 0
		}
		if bw := af.bitWidth; bw != 8 && bw != 16 && bw != 32 && bw != 64 {
			arrowFail("unsupported Int bit width %d", bw)
		}
	case atFloatingPoint:
		if hasT {
			af.prec = tt.i16(0, 0)
		}
	case atDecimal:
		if hasT {
			af.dprec = int(tt.i32(0, 0))
			af.dscale = int(tt.i32(1, 0))
			af.bitWidth = int(tt.i32(2, 128))
		} else {
			af.bitWidth = 128
		}
		if af.bitWidth != 128 && af.bitWidth != 256 {
			arrowFail("unsupported Decimal bit width %d", af.bitWidth)
		}
	case atDate:
		af.unit = 1
		if hasT {
			af.unit = tt.i16(0, 1)
		}
	case atTime:
		af.unit, af.bitWidth = 1, 32
		if hasT {
			af.unit = tt.i16(0, 1)
			af.bitWidth = int(tt.i32(1, 32))
		}
	case atTimestamp:
		if hasT {
			af.unit = tt.i16(0, 0)
			af.tz = tt.str(1)
		}
	case atDuration, atInterval:
		if hasT {
			af.unit = tt.i16(0, 0)
		}
	case atFixedSizeBinary, atFixedSizeList:
		if hasT {
			af.size = int(tt.i32(0, 0))
		}
	}
	_, nc := f.vec(5)
	for j := 0; j < nc; j++ {
		af.children = append(af.children, parseField(f.vecTable(5, j)))
	}
	_, nm := f.vec(6)
	for j := 0; j < nm; j++ {
		kv := f.vecTable(6, j)
		if kv.str(0) == "pdb:decl" {
			af.decl = kv.str(1)
		}
	}
	return af
}

func parseSchema(header fbt) *arrowSchema {
	if header.i16(0, 0) != 0 {
		arrowFail("big-endian streams are not supported")
	}
	_, n := header.vec(1)
	s := &arrowSchema{fields: make([]*arrowField, n)}
	for j := 0; j < n; j++ {
		s.fields[j] = parseField(header.vecTable(1, j))
	}
	return s
}

// arrowCol is one column of one record batch: slices into the body.
type arrowCol struct {
	f        *arrowField
	n        int
	nulls    int
	validity []byte
	data     []byte
	offs     []byte
	child    *arrowCol
	children []*arrowCol
}

func (c *arrowCol) isNull(i int) bool {
	if c.f.typ == atNull {
		return true
	}
	if c.nulls == 0 || len(c.validity) == 0 {
		return false
	}
	return c.validity[i>>3]&(1<<(uint(i)&7)) == 0
}

func (c *arrowCol) boolAt(i int) bool { return c.data[i>>3]&(1<<(uint(i)&7)) != 0 }

func (c *arrowCol) intAt(i int) int64 {
	switch c.f.bitWidth {
	case 64:
		return int64(binary.LittleEndian.Uint64(c.data[8*i:]))
	case 32:
		if c.f.signed {
			return int64(int32(binary.LittleEndian.Uint32(c.data[4*i:])))
		}
		return int64(binary.LittleEndian.Uint32(c.data[4*i:]))
	case 16:
		if c.f.signed {
			return int64(int16(binary.LittleEndian.Uint16(c.data[2*i:])))
		}
		return int64(binary.LittleEndian.Uint16(c.data[2*i:]))
	default:
		if c.f.signed {
			return int64(int8(c.data[i]))
		}
		return int64(c.data[i])
	}
}

func (c *arrowCol) floatAt(i int) float64 {
	switch c.f.prec {
	case 2:
		return math.Float64frombits(binary.LittleEndian.Uint64(c.data[8*i:]))
	case 1:
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(c.data[4*i:])))
	default:
		return halfToFloat64(binary.LittleEndian.Uint16(c.data[2*i:]))
	}
}

func (c *arrowCol) float32At(i int) float32 {
	if c.f.prec == 1 {
		return math.Float32frombits(binary.LittleEndian.Uint32(c.data[4*i:]))
	}
	return float32(c.floatAt(i))
}

// bytesAt returns the variable-width value at i (a slice of the body).
func (c *arrowCol) bytesAt(i int) []byte {
	var s, e int
	if c.f.typ == atLargeUtf8 || c.f.typ == atLargeBinary {
		s = int(int64(binary.LittleEndian.Uint64(c.offs[8*i:])))
		e = int(int64(binary.LittleEndian.Uint64(c.offs[8*i+8:])))
	} else if c.f.typ == atFixedSizeBinary {
		w := c.f.size
		return c.data[w*i : w*(i+1)]
	} else {
		s = int(int32(binary.LittleEndian.Uint32(c.offs[4*i:])))
		e = int(int32(binary.LittleEndian.Uint32(c.offs[4*i+4:])))
	}
	if s < 0 || e < s || e > len(c.data) {
		panic(arrowError{fmt.Sprintf("column %q: bad offsets %d..%d (data %d)", c.f.name, s, e, len(c.data))})
	}
	return c.data[s:e]
}

// listRange returns the child index range of list value i.
func (c *arrowCol) listRange(i int) (int, int) {
	if c.f.typ == atFixedSizeList {
		return c.f.size * i, c.f.size * (i + 1)
	}
	var s, e int
	if c.f.typ == atLargeList {
		s = int(int64(binary.LittleEndian.Uint64(c.offs[8*i:])))
		e = int(int64(binary.LittleEndian.Uint64(c.offs[8*i+8:])))
	} else {
		s = int(int32(binary.LittleEndian.Uint32(c.offs[4*i:])))
		e = int(int32(binary.LittleEndian.Uint32(c.offs[4*i+4:])))
	}
	if s < 0 || e < s || e > c.child.n {
		panic(arrowError{fmt.Sprintf("list column %q: bad offsets", c.f.name)})
	}
	return s, e
}

// batchReader walks a RecordBatch's nodes and buffers in schema pre-order.
type batchReader struct {
	rb       fbt
	body     []byte
	node     int
	buf      int
	nodesPos int
	nNodes   int
	bufsPos  int
	nBufs    int
}

func (r *batchReader) nextNode() (int, int) {
	if r.node >= r.nNodes {
		arrowFail("record batch has fewer field nodes than the schema needs")
	}
	p := r.nodesPos + 16*r.node
	fbChk(r.rb.b, p, 16)
	r.node++
	n := int(int64(binary.LittleEndian.Uint64(r.rb.b[p:])))
	nulls := int(int64(binary.LittleEndian.Uint64(r.rb.b[p+8:])))
	if n < 0 || nulls < 0 {
		arrowFail("negative field node length")
	}
	return n, nulls
}

func (r *batchReader) nextBuf() []byte {
	if r.buf >= r.nBufs {
		arrowFail("record batch has fewer buffers than the schema needs")
	}
	p := r.bufsPos + 16*r.buf
	fbChk(r.rb.b, p, 16)
	r.buf++
	off := int(int64(binary.LittleEndian.Uint64(r.rb.b[p:])))
	n := int(int64(binary.LittleEndian.Uint64(r.rb.b[p+8:])))
	fbChk(r.body, off, n)
	return r.body[off : off+n : off+n]
}

func needLen(b []byte, n int, what string) {
	if len(b) < n {
		arrowFail("%s buffer too short (%d < %d)", what, len(b), n)
	}
}

func (r *batchReader) column(f *arrowField, into *arrowCol) {
	n, nulls := r.nextNode()
	*into = arrowCol{f: f, n: n, nulls: nulls}
	if f.typ == atNull {
		return
	}
	into.validity = r.nextBuf()
	if nulls > 0 && len(into.validity) > 0 {
		needLen(into.validity, (n+7)/8, "validity")
	}
	if nulls > 0 && len(into.validity) == 0 {
		arrowFail("column %q has %d nulls but no validity bitmap", f.name, nulls)
	}
	switch f.typ {
	case atBool:
		into.data = r.nextBuf()
		needLen(into.data, (n+7)/8, "bool")
	case atInt:
		into.data = r.nextBuf()
		needLen(into.data, f.bitWidth/8*n, "int")
	case atFloatingPoint:
		into.data = r.nextBuf()
		w := 0
		switch f.prec {
		case 0:
			w = 2
		case 1:
			w = 4
		case 2:
			w = 8
		default:
			arrowFail("unsupported float precision %d", f.prec)
		}
		needLen(into.data, w*n, "float")
	case atDecimal:
		into.data = r.nextBuf()
		needLen(into.data, f.bitWidth/8*n, "decimal")
	case atDate:
		into.data = r.nextBuf()
		if f.unit == 0 {
			needLen(into.data, 4*n, "date32")
		} else {
			needLen(into.data, 8*n, "date64")
		}
	case atTime:
		into.data = r.nextBuf()
		needLen(into.data, f.bitWidth/8*n, "time")
	case atTimestamp, atDuration:
		into.data = r.nextBuf()
		needLen(into.data, 8*n, "timestamp")
	case atInterval:
		into.data = r.nextBuf()
	case atUtf8, atBinary, atLargeUtf8, atLargeBinary:
		into.offs = r.nextBuf()
		into.data = r.nextBuf()
		w := 4
		if f.typ == atLargeUtf8 || f.typ == atLargeBinary {
			w = 8
		}
		if n > 0 {
			needLen(into.offs, w*(n+1), "offsets")
		}
	case atFixedSizeBinary:
		into.data = r.nextBuf()
		needLen(into.data, f.size*n, "fixed-size binary")
	case atList, atLargeList, atMap:
		if len(f.children) != 1 {
			arrowFail("list column %q needs one child", f.name)
		}
		into.offs = r.nextBuf()
		w := 4
		if f.typ == atLargeList {
			w = 8
		}
		if n > 0 {
			needLen(into.offs, w*(n+1), "list offsets")
		}
		into.child = &arrowCol{}
		r.column(f.children[0], into.child)
	case atFixedSizeList:
		if len(f.children) != 1 {
			arrowFail("list column %q needs one child", f.name)
		}
		into.child = &arrowCol{}
		r.column(f.children[0], into.child)
		if into.child.n < f.size*n {
			arrowFail("fixed-size list %q: child too short", f.name)
		}
	case atStruct:
		into.children = make([]*arrowCol, len(f.children))
		for j, ch := range f.children {
			into.children[j] = &arrowCol{}
			r.column(ch, into.children[j])
		}
	default:
		arrowFail("unsupported Arrow type %d for column %q", f.typ, f.name)
	}
}

// arrowBatch is one decoded record batch.
type arrowBatch struct {
	rows int
	cols []arrowCol
}

func decodeBatch(s *arrowSchema, header fbt, body []byte, into *arrowBatch) {
	if _, compressed := header.table(3); compressed {
		arrowFail("compressed record batches are not supported")
	}
	r := batchReader{rb: header, body: body}
	r.nodesPos, r.nNodes = header.vec(1)
	r.bufsPos, r.nBufs = header.vec(2)
	into.rows = int(header.i64(0, 0))
	if into.rows < 0 {
		arrowFail("negative row count")
	}
	if cap(into.cols) >= len(s.fields) {
		into.cols = into.cols[:len(s.fields)]
	} else {
		into.cols = make([]arrowCol, len(s.fields))
	}
	for j, f := range s.fields {
		r.column(f, &into.cols[j])
		if into.cols[j].n < into.rows {
			arrowFail("column %q has %d values for %d rows", f.name, into.cols[j].n, into.rows)
		}
	}
}

// ipcMessage is one framed message of an IPC stream.
type ipcMessage struct {
	kind   uint8
	header fbt
	meta   []byte
	body   []byte
	eos    bool
}

// nextMessage parses the message at the head of b. ok is false when b does
// not hold a whole message yet.
func nextMessage(b []byte) (m ipcMessage, used int, ok bool) {
	if len(b) < 4 {
		return m, 0, false
	}
	off := 0
	ml := binary.LittleEndian.Uint32(b)
	off = 4
	if ml == arrowContinuation {
		if len(b) < 8 {
			return m, 0, false
		}
		ml = binary.LittleEndian.Uint32(b[4:])
		off = 8
	}
	if ml == 0 {
		m.eos = true
		return m, off, true
	}
	if int32(ml) < 0 {
		arrowFail("negative metadata length")
	}
	if len(b) < off+int(ml) {
		return m, 0, false
	}
	m.meta = b[off : off+int(ml)]
	off += int(ml)
	msg := fbRoot(m.meta)
	bl := msg.i64(3, 0)
	if bl < 0 || bl > maxPayload*64 {
		arrowFail("bad body length %d", bl)
	}
	if len(b) < off+int(bl) {
		return m, 0, false
	}
	m.body = b[off : off+int(bl)]
	off += int(bl)
	m.kind = msg.u8(1, 0)
	h, hasH := msg.table(2)
	if !hasH {
		arrowFail("message without header")
	}
	m.header = h
	return m, off, true
}

func halfToFloat64(h uint16) float64 {
	sign := 1.0
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
