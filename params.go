package particledb

// Parameter binding: Go values to the native wire's typed EXECUTE parameters
// (tags 0..17), and to the identical SQL literal spelling the server renders
// them with (used for statements inside one BATCH frame).
//
// Tags (docs/architecture/PDB_WIRE_V2.md, "Prepared statements and typed
// parameters"): 0 NULL, 1 int8, 2 text, 3 bool, 4 float8, 5 float4,
// 6 numeric, 7 date, 8 timestamp, 9 timestamptz, 10 time, 11 uuid, 12 bytea,
// 13 int2, 14 int4, 15 json, 16 jsonb, 17 array.

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	tagNull        = 0
	tagInt8        = 1
	tagText        = 2
	tagBool        = 3
	tagFloat8      = 4
	tagFloat4      = 5
	tagNumeric     = 6
	tagDate        = 7
	tagTimestamp   = 8
	tagTimestampTZ = 9
	tagUUID        = 11
	tagBytea       = 12
	tagInt2        = 13
	tagInt4        = 14
	tagJSON        = 15
	tagJSONB       = 16
	tagArray       = 17

	maxArrayDims = 6
)

// pval is one classified parameter value.
type pval struct {
	tag  byte
	i    int64   // int, bool (0/1), date days, timestamp micros
	f    float64 // float8 / float4
	s    string  // text, json
	b    []byte  // bytea, uuid (16)
	off  int32   // timestamptz offset seconds east
	dec  Decimal
	arr  *arrayParam
	null bool
}

type arrayParam struct {
	elemTag byte
	dims    []int
	elems   []pval
}

func badParam(i int, format string, args ...any) error {
	return &Error{SQLState: "22023", Severity: "ERROR", Message: fmt.Sprintf("parameter $%d: ", i+1) + fmt.Sprintf(format, args...), BatchIndex: -1}
}

var (
	typeTime       = reflect.TypeOf(time.Time{})
	typeRawMessage = reflect.TypeOf(json.RawMessage(nil))
)

// classify turns a Go value into a pval. idx is for error messages.
func classify(v any, idx int) (pval, error) {
	switch x := v.(type) {
	case nil:
		return pval{tag: tagNull, null: true}, nil
	case int64:
		return pval{tag: tagInt8, i: x}, nil
	case int:
		return pval{tag: tagInt8, i: int64(x)}, nil
	case int32:
		return pval{tag: tagInt8, i: int64(x)}, nil
	case int16:
		return pval{tag: tagInt8, i: int64(x)}, nil
	case int8:
		return pval{tag: tagInt8, i: int64(x)}, nil
	case uint8:
		return pval{tag: tagInt8, i: int64(x)}, nil
	case uint16:
		return pval{tag: tagInt8, i: int64(x)}, nil
	case uint32:
		return pval{tag: tagInt8, i: int64(x)}, nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return pval{}, badParam(idx, "uint %d is out of the int8 range", x)
		}
		return pval{tag: tagInt8, i: int64(x)}, nil
	case uint64:
		if x > math.MaxInt64 {
			return pval{}, badParam(idx, "uint64 %d is out of the int8 range", x)
		}
		return pval{tag: tagInt8, i: int64(x)}, nil
	case string:
		return pval{tag: tagText, s: x}, nil
	case bool:
		if x {
			return pval{tag: tagBool, i: 1}, nil
		}
		return pval{tag: tagBool}, nil
	case float64:
		return pval{tag: tagFloat8, f: x}, nil
	case float32:
		return pval{tag: tagFloat4, f: float64(x)}, nil
	case []byte:
		if x == nil {
			return pval{tag: tagNull, null: true}, nil
		}
		return pval{tag: tagBytea, b: x}, nil
	case json.RawMessage:
		if x == nil {
			return pval{tag: tagNull, null: true}, nil
		}
		return pval{tag: tagJSONB, s: string(x)}, nil
	case time.Time:
		_, off := x.Zone()
		return pval{tag: tagTimestampTZ, i: unixMicros(x), off: int32(off)}, nil
	case Date:
		y, m, d := x.Time.Date()
		return pval{tag: tagDate, i: daysFromCivil(int64(y), int(m), d)}, nil
	case Timestamp:
		return pval{tag: tagTimestamp, i: wallMicros(x.Time)}, nil
	case Decimal:
		return pval{tag: tagNumeric, dec: x}, nil
	case *Decimal:
		if x == nil {
			return pval{tag: tagNull, null: true}, nil
		}
		return pval{tag: tagNumeric, dec: *x}, nil
	case UUID:
		b := x
		return pval{tag: tagUUID, b: b[:]}, nil
	case Vector:
		if x == nil {
			return pval{tag: tagNull, null: true}, nil
		}
		return pval{tag: tagText, s: x.String()}, nil
	case *big.Int:
		if x == nil {
			return pval{tag: tagNull, null: true}, nil
		}
		return pval{tag: tagNumeric, dec: NewDecimalBig(x, 0)}, nil
	case arrayArg:
		return classifyArray(x.v, idx)
	case bigDecimal:
		if reflect.ValueOf(x).Kind() == reflect.Pointer && reflect.ValueOf(x).IsNil() {
			return pval{tag: tagNull, null: true}, nil
		}
		c, e := x.Coefficient(), x.Exponent()
		if e > 0 {
			c = new(big.Int).Mul(c, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(e)), nil))
			e = 0
		}
		return pval{tag: tagNumeric, dec: NewDecimalBig(c, -e)}, nil
	case driver.Valuer:
		rv := reflect.ValueOf(x)
		if rv.Kind() == reflect.Pointer && rv.IsNil() {
			return pval{tag: tagNull, null: true}, nil
		}
		dv, err := x.Value()
		if err != nil {
			return pval{}, badParam(idx, "%v", err)
		}
		if _, again := dv.(driver.Valuer); again {
			return pval{}, badParam(idx, "Value() of %T returned another Valuer", v)
		}
		return classify(dv, idx)
	}
	return classifyReflect(v, idx)
}

func classifyReflect(v any, idx int) (pval, error) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return pval{tag: tagNull, null: true}, nil
		}
		return classify(rv.Elem().Interface(), idx)
	case reflect.Array:
		if rv.Len() == 16 && rv.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, 16)
			reflect.Copy(reflect.ValueOf(b), rv)
			return pval{tag: tagUUID, b: b}, nil
		}
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			if rv.IsNil() {
				return pval{tag: tagNull, null: true}, nil
			}
			return pval{tag: tagBytea, b: rv.Bytes()}, nil
		}
		return classifyArray(v, idx)
	case reflect.String:
		return pval{tag: tagText, s: rv.String()}, nil
	case reflect.Bool:
		return classify(rv.Bool(), idx)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return pval{tag: tagInt8, i: rv.Int()}, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return pval{}, badParam(idx, "unsigned %d is out of the int8 range", u)
		}
		return pval{tag: tagInt8, i: int64(u)}, nil
	case reflect.Float32:
		return pval{tag: tagFloat4, f: rv.Float()}, nil
	case reflect.Float64:
		return pval{tag: tagFloat8, f: rv.Float()}, nil
	}
	return pval{}, badParam(idx, "unsupported Go type %T", v)
}

// classifyArray turns a (possibly nested) slice into an array parameter.
func classifyArray(v any, idx int) (pval, error) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return pval{tag: tagNull, null: true}, nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return pval{}, badParam(idx, "Array() needs a slice, got %T", v)
	}
	if rv.Kind() == reflect.Slice && rv.IsNil() {
		return pval{tag: tagNull, null: true}, nil
	}
	ap := &arrayParam{}
	// dimensions from the first element path
	t := rv.Type()
	for t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		if t.Elem().Kind() == reflect.Uint8 && (t.Kind() == reflect.Slice || t.Len() == 16) {
			break // []byte / [16]byte are scalar elements
		}
		if t == typeRawMessage {
			break
		}
		t = t.Elem()
		if len(ap.dims) == maxArrayDims {
			return pval{}, badParam(idx, "array has more than %d dimensions", maxArrayDims)
		}
		ap.dims = append(ap.dims, -1)
	}
	if err := ap.walk(rv, 0, idx); err != nil {
		return pval{}, err
	}
	for _, d := range ap.dims {
		if d == 0 {
			ap.dims = nil // an empty array
			ap.elems = nil
			break
		}
	}
	if ap.elemTag == 0 {
		ap.elemTag = elemTagForType(t)
		if ap.elemTag == 0 {
			ap.elemTag = tagText
		}
	}
	return pval{tag: tagArray, arr: ap}, nil
}

func elemTagForType(t reflect.Type) byte {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t {
	case typeTime:
		return tagTimestampTZ
	case reflect.TypeOf(Decimal{}):
		return tagNumeric
	case reflect.TypeOf(UUID{}):
		return tagUUID
	case reflect.TypeOf(Date{}):
		return tagDate
	case reflect.TypeOf(Timestamp{}):
		return tagTimestamp
	case typeRawMessage:
		return tagJSONB
	}
	switch t.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Uint8:
		return tagInt2
	case reflect.Int32, reflect.Uint16:
		return tagInt4
	case reflect.Int, reflect.Int64, reflect.Uint32, reflect.Uint, reflect.Uint64:
		return tagInt8
	case reflect.String:
		return tagText
	case reflect.Bool:
		return tagBool
	case reflect.Float64:
		return tagFloat8
	case reflect.Float32:
		return tagFloat4
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return tagBytea
		}
	case reflect.Array:
		if t.Len() == 16 && t.Elem().Kind() == reflect.Uint8 {
			return tagUUID
		}
	}
	return 0
}

func (ap *arrayParam) walk(rv reflect.Value, depth, idx int) error {
	if depth < len(ap.dims) {
		for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
			if rv.IsNil() {
				return badParam(idx, "a NULL sub-array in a multidimensional array")
			}
			rv = rv.Elem()
		}
		n := rv.Len()
		if ap.dims[depth] == -1 {
			ap.dims[depth] = n
		} else if ap.dims[depth] != n {
			return badParam(idx, "multidimensional arrays must be rectangular")
		}
		for i := 0; i < n; i++ {
			if err := ap.walk(rv.Index(i), depth+1, idx); err != nil {
				return err
			}
		}
		return nil
	}
	if len(ap.elems) >= 1<<20 {
		return badParam(idx, "array has more than 1048576 elements")
	}
	var e pval
	var err error
	if rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			ap.elems = append(ap.elems, pval{tag: tagNull, null: true})
			return nil
		}
	}
	e, err = classify(rv.Interface(), idx)
	if err != nil {
		return err
	}
	if e.null {
		ap.elems = append(ap.elems, e)
		return nil
	}
	if e.tag == tagArray {
		return badParam(idx, "nested array element (use a multidimensional slice)")
	}
	if e.tag == tagInt8 {
		// keep the declared width of the slice element type
		if t := elemTagForType(rv.Type()); t == tagInt2 || t == tagInt4 {
			e.tag = t
		}
	}
	if ap.elemTag == 0 {
		ap.elemTag = e.tag
	} else if ap.elemTag != e.tag {
		return badParam(idx, "array elements of mixed types")
	}
	ap.elems = append(ap.elems, e)
	return nil
}

// arrayArg is the value Array returns.
type arrayArg struct{ v any }

// Array wraps a slice for binding as an SQL array, or a pointer to a slice
// for scanning one, like lib/pq's pq.Array:
//
//	db.Exec("INSERT INTO t (tags) VALUES ($1)", particledb.Array([]string{"a", "b"}))
//	db.QueryRow("SELECT tags FROM t").Scan(particledb.Array(&tags))
//
// The native API and this package's database/sql driver also accept a plain
// slice as an array parameter; Array is needed for scanning through
// database/sql and for other drivers.
func Array(a any) interface {
	driver.Valuer
	Scan(src any) error
} {
	return arrayArg{v: a}
}

// Value renders the array as PostgreSQL's external text.
func (a arrayArg) Value() (driver.Value, error) {
	p, err := classifyArray(a.v, 0)
	if err != nil {
		return nil, err
	}
	if p.null {
		return nil, nil
	}
	return arrayExternalText(p.arr), nil
}

// Scan parses an array result into the pointed-to slice.
func (a arrayArg) Scan(src any) error {
	var text string
	switch s := src.(type) {
	case nil:
		rv := reflect.ValueOf(a.v)
		if rv.Kind() != reflect.Pointer || rv.IsNil() {
			return fmt.Errorf("particledb: Array scan needs a pointer to a slice, got %T", a.v)
		}
		rv.Elem().Set(reflect.Zero(rv.Elem().Type()))
		return nil
	case string:
		text = s
	case []byte:
		text = string(s)
	default:
		return fmt.Errorf("particledb: cannot scan %T into an array", src)
	}
	return scanArrayText(text, a.v)
}

// ---------------------------------------------------------------------------
// Binary encoding (EXECUTE)
// ---------------------------------------------------------------------------

func appendParam(b []byte, p *pval) []byte {
	switch p.tag {
	case tagNull:
		return append(b, tagNull)
	case tagInt8, tagInt2, tagInt4:
		b = append(b, tagInt8)
		return binary.LittleEndian.AppendUint64(b, uint64(p.i))
	case tagText:
		b = append(b, tagText)
		return appendLenStr(b, p.s)
	case tagBool:
		return append(b, tagBool, byte(p.i))
	case tagFloat8:
		b = append(b, tagFloat8)
		return binary.LittleEndian.AppendUint64(b, math.Float64bits(p.f))
	case tagFloat4:
		b = append(b, tagFloat4)
		return binary.LittleEndian.AppendUint32(b, math.Float32bits(float32(p.f)))
	case tagNumeric:
		return appendNumeric(append(b, tagNumeric), p.dec)
	case tagDate:
		b = append(b, tagDate)
		return binary.LittleEndian.AppendUint32(b, uint32(int32(p.i)))
	case tagTimestamp:
		b = append(b, tagTimestamp)
		return binary.LittleEndian.AppendUint64(b, uint64(p.i))
	case tagTimestampTZ:
		b = append(b, tagTimestampTZ)
		b = binary.LittleEndian.AppendUint64(b, uint64(p.i))
		return binary.LittleEndian.AppendUint32(b, uint32(p.off))
	case tagUUID:
		return append(append(b, tagUUID), p.b...)
	case tagBytea:
		b = append(b, tagBytea)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(p.b)))
		return append(b, p.b...)
	case tagJSON, tagJSONB:
		b = append(b, p.tag)
		return appendLenStr(b, p.s)
	case tagArray:
		a := p.arr
		b = append(b, tagArray, a.elemTag, byte(len(a.dims)))
		for _, d := range a.dims {
			b = binary.LittleEndian.AppendUint32(b, uint32(d))
		}
		for i := range a.elems {
			e := &a.elems[i]
			if e.null {
				b = append(b, tagNull)
				continue
			}
			b = appendElem(b, e)
		}
		return b
	}
	return append(b, tagNull)
}

// appendElem encodes an array element with its own (narrow) tag.
func appendElem(b []byte, e *pval) []byte {
	switch e.tag {
	case tagInt2:
		b = append(b, tagInt2)
		return binary.LittleEndian.AppendUint16(b, uint16(int16(e.i)))
	case tagInt4:
		b = append(b, tagInt4)
		return binary.LittleEndian.AppendUint32(b, uint32(int32(e.i)))
	}
	return appendParam(b, e)
}

func appendNumeric(b []byte, d Decimal) []byte {
	if d.special != 0 {
		// Specials travel as text the server reads as numeric.
		return b
	}
	b = binary.LittleEndian.AppendUint32(b, uint32(d.scale))
	lenAt := len(b)
	b = append(b, 0, 0, 0, 0)
	b = d.wireBytes(b)
	binary.LittleEndian.PutUint32(b[lenAt:], uint32(len(b)-lenAt-4))
	return b
}

// fixSpecials rewrites numeric specials (no binary form) to text and narrow
// integers to int8 where the server needs it.
func fixSpecials(p *pval) {
	if p.tag == tagNumeric && p.dec.special != 0 {
		p.tag = tagText
		p.s = p.dec.String()
	}
}

// checkCaps verifies the server accepts p's tag.
func checkCaps(p *pval, i int, h helloOK) error {
	if p.tag == tagArray {
		if !h.arrayParams() {
			return clientErr("0A000", "parameter $%d: the server does not accept array parameters", i+1)
		}
		for j := range p.arr.elems {
			if p.arr.elems[j].tag == tagNumeric && p.arr.elems[j].dec.special != 0 {
				return clientErr("0A000", "parameter $%d: NaN or Infinity inside a numeric array", i+1)
			}
		}
	} else if p.tag > tagBool && !h.typedParams() {
		return clientErr("0A000", "parameter $%d: the server does not accept typed parameters", i+1)
	}
	return nil
}

var payloadPool = sync.Pool{New: func() any { b := make([]byte, 0, 512); return &b }}

func getPayload() *[]byte { return payloadPool.Get().(*[]byte) }

func putPayload(b *[]byte) {
	if cap(*b) <= 64<<10 {
		*b = (*b)[:0]
		payloadPool.Put(b)
	}
}

// appendExecute appends an EXECUTE payload for statement name with args
// classified and encoded in one pass.
func appendExecute(dst []byte, name string, args []any, h helloOK) ([]byte, error) {
	if len(args) > math.MaxUint16 {
		return dst, clientErr("54000", "%d parameters exceed the protocol limit of 65535", len(args))
	}
	dst = appendLenStr(dst, name)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(args)))
	for i, a := range args {
		p, err := classify(a, i)
		if err != nil {
			return dst, err
		}
		fixSpecials(&p)
		if err := checkCaps(&p, i, h); err != nil {
			return dst, err
		}
		dst = appendParam(dst, &p)
	}
	return dst, nil
}

// classifyArgs classifies every argument, checking the server's
// capabilities for the typed and array tags.
func classifyArgs(dst []pval, args []any, h helloOK) ([]pval, error) {
	if len(args) > math.MaxUint16 {
		return nil, clientErr("54000", "%d parameters exceed the protocol limit of 65535", len(args))
	}
	for i, a := range args {
		p, err := classify(a, i)
		if err != nil {
			return nil, err
		}
		fixSpecials(&p)
		if err := checkCaps(&p, i, h); err != nil {
			return nil, err
		}
		dst = append(dst, p)
	}
	return dst, nil
}

// ---------------------------------------------------------------------------
// Literal spelling (the server's native_literal, byte for byte)
// ---------------------------------------------------------------------------

func appendSigned(b []byte, s string) []byte {
	if strings.HasPrefix(s, "-") {
		b = append(b, '(')
		b = append(b, s...)
		return append(b, ')')
	}
	return append(b, s...)
}

func appendQuoted(b []byte, s string) []byte {
	b = append(b, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b = append(b, '\'')
		}
		b = append(b, s[i])
	}
	return append(b, '\'')
}

func floatSpecial(f float64) string {
	switch {
	case math.IsNaN(f):
		return "'NaN'::float8"
	case math.IsInf(f, 1):
		return "'Infinity'::float8"
	case math.IsInf(f, -1):
		return "'-Infinity'::float8"
	}
	return ""
}

// appendLiteral appends the SQL literal the server renders for p.
func appendLiteral(b []byte, p *pval) []byte {
	switch p.tag {
	case tagNull:
		return append(b, "NULL"...)
	case tagBool:
		if p.i != 0 {
			return append(b, "TRUE"...)
		}
		return append(b, "FALSE"...)
	case tagInt8, tagInt2, tagInt4:
		return appendSigned(b, strconv.FormatInt(p.i, 10))
	case tagText, tagJSON, tagJSONB:
		return appendQuoted(b, p.s)
	case tagFloat8:
		if s := floatSpecial(p.f); s != "" {
			return append(b, s...)
		}
		return appendSigned(b, javaDoubleText(p.f))
	case tagFloat4:
		if s := floatSpecial(p.f); s != "" {
			return append(b, s...)
		}
		return appendSigned(b, javaFloatText(float32(p.f)))
	case tagNumeric:
		s := p.dec.String()
		if p.dec.special != 0 {
			return appendQuoted(b, s)
		}
		return appendSigned(b, s)
	case tagDate:
		return appendQuoted(b, dateText(p.i))
	case tagTimestamp:
		return appendQuoted(b, timestampText(p.i))
	case tagTimestampTZ:
		return appendQuoted(b, timestamptzText(p.i, p.off))
	case tagUUID:
		var u UUID
		copy(u[:], p.b)
		return appendQuoted(b, u.String())
	case tagBytea:
		return appendQuoted(b, byteaHex(p.b))
	case tagArray:
		b = appendQuoted(b, arrayExternalText(p.arr))
		b = append(b, "::"...)
		b = append(b, arrayCastName(p.arr.elemTag)...)
		return append(b, "[]"...)
	}
	return append(b, "NULL"...)
}

func byteaHex(v []byte) string {
	const hx = "0123456789abcdef"
	out := make([]byte, 2, 2+2*len(v))
	out[0], out[1] = '\\', 'x'
	for _, c := range v {
		out = append(out, hx[c>>4], hx[c&15])
	}
	return string(out)
}

func arrayCastName(tag byte) string {
	switch tag {
	case tagInt8:
		return "int8"
	case tagText:
		return "text"
	case tagBool:
		return "bool"
	case tagFloat8:
		return "float8"
	case tagFloat4:
		return "float4"
	case tagNumeric:
		return "numeric"
	case tagDate:
		return "date"
	case tagTimestamp:
		return "timestamp"
	case tagTimestampTZ:
		return "timestamptz"
	case tagUUID:
		return "uuid"
	case tagBytea:
		return "bytea"
	case tagInt2:
		return "int2"
	case tagInt4:
		return "int4"
	case tagJSON:
		return "json"
	case tagJSONB:
		return "jsonb"
	}
	return "text"
}

// arrayExternalText is PostgreSQL's external array text of a (the body of
// the literal, before quoting).
func arrayExternalText(a *arrayParam) string {
	if len(a.dims) == 0 {
		return "{}"
	}
	blocks := make([]int, len(a.dims))
	acc := 1
	for d := len(a.dims) - 1; d >= 0; d-- {
		acc *= a.dims[d]
		blocks[d] = acc
	}
	var out []byte
	for k := range a.elems {
		if k > 0 {
			out = append(out, ',')
		}
		for _, bl := range blocks {
			if k%bl == 0 {
				out = append(out, '{')
			}
		}
		out = appendArrayElem(out, &a.elems[k])
		for _, bl := range blocks {
			if (k+1)%bl == 0 {
				out = append(out, '}')
			}
		}
	}
	return string(out)
}

func appendArrayQuoted(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' || s[i] == '"' {
			b = append(b, '\\')
		}
		b = append(b, s[i])
	}
	return append(b, '"')
}

func appendArrayElem(b []byte, e *pval) []byte {
	if e.null {
		return append(b, "NULL"...)
	}
	switch e.tag {
	case tagBool:
		if e.i != 0 {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case tagInt8, tagInt2, tagInt4:
		return strconv.AppendInt(b, e.i, 10)
	case tagFloat8, tagFloat4:
		switch {
		case math.IsNaN(e.f):
			return append(b, "NaN"...)
		case math.IsInf(e.f, 1):
			return append(b, "Infinity"...)
		case math.IsInf(e.f, -1):
			return append(b, "-Infinity"...)
		}
		if e.tag == tagFloat4 {
			return append(b, javaFloatText(float32(e.f))...)
		}
		return append(b, javaDoubleText(e.f)...)
	case tagNumeric:
		return append(b, e.dec.String()...)
	case tagText, tagJSON, tagJSONB:
		return appendArrayQuoted(b, e.s)
	case tagDate:
		return appendArrayQuoted(b, dateText(e.i))
	case tagTimestamp:
		return appendArrayQuoted(b, timestampText(e.i))
	case tagTimestampTZ:
		return appendArrayQuoted(b, timestamptzText(e.i, e.off))
	case tagUUID:
		var u UUID
		copy(u[:], e.b)
		return appendArrayQuoted(b, u.String())
	case tagBytea:
		return appendArrayQuoted(b, byteaHex(e.b))
	}
	return append(b, "NULL"...)
}

// ---------------------------------------------------------------------------
// Java Double.toString layout of the shortest round-trip digits
// ---------------------------------------------------------------------------

func javaDoubleText(d float64) string {
	if d == 0 {
		if math.Signbit(d) {
			return "-0.0"
		}
		return "0.0"
	}
	a := math.Abs(d)
	digits, exp := canonicalDigits(a, 17, 64)
	return javaLayout(d < 0, digits, exp)
}

func javaFloatText(f float32) string {
	if f == 0 {
		if math.Signbit(float64(f)) {
			return "-0.0"
		}
		return "0.0"
	}
	a := float32(math.Abs(float64(f)))
	digits, exp := canonicalDigits(float64(a), 9, 32)
	return javaLayout(f < 0, digits, exp)
}

func canonicalDigits(a float64, max, bits int) (string, int) {
	shortest := strconv.FormatFloat(a, 'e', -1, bits)
	n := sigDigits(shortest)
	nearest := func(p int) string { return strconv.FormatFloat(a, 'e', p-1, bits) }
	roundTrips := func(r string) bool {
		v, err := strconv.ParseFloat(r, bits)
		return err == nil && v == a
	}
	chosenP, chosen := 0, ""
	for p := n; p <= max; p++ {
		r := nearest(p)
		if roundTrips(r) {
			chosenP, chosen = p, r
			break
		}
	}
	var r string
	switch {
	case chosenP == 1:
		if two := nearest(2); roundTrips(two) {
			r = two
		} else {
			r = chosen
		}
	case chosenP > 0:
		r = chosen
	default:
		r = nearest(max)
	}
	return splitSci(r)
}

func sigDigits(sci string) int {
	mant := sci
	if i := strings.IndexByte(sci, 'e'); i >= 0 {
		mant = sci[:i]
	}
	n := 0
	for i := 0; i < len(mant); i++ {
		if mant[i] >= '0' && mant[i] <= '9' {
			n++
		}
	}
	if n == 0 {
		n = 1
	}
	return n
}

func splitSci(sci string) (string, int) {
	mant, exp := sci, "0"
	if i := strings.IndexByte(sci, 'e'); i >= 0 {
		mant, exp = sci[:i], sci[i+1:]
	}
	var d []byte
	for i := 0; i < len(mant); i++ {
		if mant[i] >= '0' && mant[i] <= '9' {
			d = append(d, mant[i])
		}
	}
	for len(d) > 1 && d[len(d)-1] == '0' {
		d = d[:len(d)-1]
	}
	e, _ := strconv.Atoi(exp)
	return string(d), e
}

func javaLayout(neg bool, digits string, exp int) string {
	var out []byte
	if neg {
		out = append(out, '-')
	}
	if exp >= -3 && exp < 7 {
		if exp >= 0 {
			il := exp + 1
			if len(digits) <= il {
				out = append(out, digits...)
				out = append(out, strings.Repeat("0", il-len(digits))...)
				out = append(out, ".0"...)
			} else {
				out = append(out, digits[:il]...)
				out = append(out, '.')
				out = append(out, digits[il:]...)
			}
		} else {
			out = append(out, "0."...)
			out = append(out, strings.Repeat("0", -exp-1)...)
			out = append(out, digits...)
		}
	} else {
		out = append(out, digits[0], '.')
		if len(digits) > 1 {
			out = append(out, digits[1:]...)
		} else {
			out = append(out, '0')
		}
		out = append(out, 'E')
		out = strconv.AppendInt(out, int64(exp), 10)
	}
	return string(out)
}

// ---------------------------------------------------------------------------
// Date and time spellings
// ---------------------------------------------------------------------------

const usPerDay = 86_400_000_000

func civilFromDays(days int64) (int64, int, int) {
	z := days + 719468
	era := z / 146097
	if z < 0 && z%146097 != 0 {
		era = (z - 146096) / 146097
	}
	doe := z - era*146097
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d := int(doy - (153*mp+2)/5 + 1)
	m := int(mp + 3)
	if mp >= 10 {
		m = int(mp - 9)
	}
	y := yoe + era*400
	if m <= 2 {
		y++
	}
	return y, m, d
}

func datePart(days int64) (string, bool) {
	y, m, d := civilFromDays(days)
	if y >= 1 {
		return fmt.Sprintf("%04d-%02d-%02d", y, m, d), false
	}
	return fmt.Sprintf("%04d-%02d-%02d", 1-y, m, d), true
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

func dateText(days int64) string {
	s, bc := datePart(days)
	if bc {
		return s + " BC"
	}
	return s
}

func timestampText(us int64) string {
	days := floorDiv(us, usPerDay)
	tod := us - days*usPerDay
	date, bc := datePart(days)
	secs := tod / 1_000_000
	frac := tod % 1_000_000
	s := fmt.Sprintf("%s %02d:%02d:%02d.", date, secs/3600, secs/60%60, secs%60)
	if frac == 0 {
		s += "0"
	} else {
		f := strings.TrimRight(fmt.Sprintf("%09d", frac*1000), "0")
		s += f
	}
	if bc {
		s += " BC"
	}
	return s
}

func timestamptzText(utc int64, off int32) string {
	local := utc + int64(off)*1_000_000
	days := floorDiv(local, usPerDay)
	tod := local - days*usPerDay
	date, bc := datePart(days)
	secs := tod / 1_000_000
	frac := tod % 1_000_000
	s := fmt.Sprintf("%sT%02d:%02d", date, secs/3600, secs/60%60)
	if secs%60 != 0 || frac != 0 {
		s += fmt.Sprintf(":%02d", secs%60)
	}
	if frac != 0 {
		if frac%1000 == 0 {
			s += fmt.Sprintf(".%03d", frac/1000)
		} else {
			s += fmt.Sprintf(".%06d", frac)
		}
	}
	if off == 0 {
		s += "Z"
	} else {
		a := off
		sign := "+"
		if off < 0 {
			a, sign = -off, "-"
		}
		s += fmt.Sprintf("%s%02d:%02d", sign, a/3600, a/60%60)
		if a%60 != 0 {
			s += fmt.Sprintf(":%02d", a%60)
		}
	}
	if bc {
		s += " BC"
	}
	return s
}

// renderSQL substitutes $n placeholders with literals, skipping quoted text,
// quoted identifiers, dollar-quoted strings and comments.
func renderSQL(sql string, ps []pval) (string, error) {
	if len(ps) == 0 {
		return sql, nil
	}
	lits := make([][]byte, len(ps))
	for i := range ps {
		lits[i] = appendLiteral(nil, &ps[i])
	}
	var out []byte
	used := make([]bool, len(ps))
	err := scanSQL(sql, func(kind int, s string) {
		if kind != tokPlaceholder {
			out = append(out, s...)
			return
		}
		n, _ := strconv.Atoi(s[1:])
		if n >= 1 && n <= len(ps) {
			out = append(out, lits[n-1]...)
			used[n-1] = true
			return
		}
		out = append(out, s...)
	})
	if err != nil {
		return "", err
	}
	_ = used
	return string(out), nil
}
