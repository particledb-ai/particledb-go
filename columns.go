package particledb

// Column descriptions and cell conversion: from an Arrow column plus its
// declared SQL type (the pdb:decl field metadata) to Go values.

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type colKind uint8

const (
	ckNull colKind = iota
	ckBool
	ckInt
	ckFloat32
	ckFloat64
	ckDecimal     // Arrow Decimal
	ckNumericText // numeric carried as text (per-value scale, or a special)
	ckText
	ckJSON
	ckUUID
	ckBytes
	ckVector
	ckDate
	ckTimestamp
	ckTimestampTZ
	ckArray // PostgreSQL external array text
	ckTime
	ckOther
)

// FieldDescription describes one result column.
type FieldDescription struct {
	// Name is the column label.
	Name string
	// DatabaseTypeName is the PostgreSQL type name in upper case (INT4,
	// NUMERIC, TIMESTAMPTZ, _INT4 for int4[], ...).
	DatabaseTypeName string
	// Nullable is false for a column the server declares NOT NULL.
	Nullable bool
	// Length is the declared length of a varchar(n) / char(n) column, or -1.
	Length int64
	// Precision and Scale of a numeric(p,s) column, or -1.
	Precision, Scale int64

	kind     colKind
	elemKind colKind // array element kind
	field    *arrowField
}

// normalizeDecl maps a declared type spelling to the PostgreSQL type name.
func normalizeDecl(decl string) (name string, length, prec, scale int64) {
	length, prec, scale = -1, -1, -1
	d := strings.ToLower(strings.TrimSpace(decl))
	if d == "" {
		return "", length, prec, scale
	}
	if strings.HasSuffix(d, "[]") {
		elem, _, _, _ := normalizeDecl(strings.TrimSuffix(d, "[]"))
		return "_" + elem, length, prec, scale
	}
	base, args := d, ""
	if i := strings.IndexByte(d, '('); i >= 0 && strings.HasSuffix(d, ")") {
		base, args = strings.TrimSpace(d[:i]), d[i+1:len(d)-1]
	}
	parseArgs := func() []int64 {
		var out []int64
		for _, p := range strings.Split(args, ",") {
			if v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64); err == nil {
				out = append(out, v)
			}
		}
		return out
	}
	switch base {
	case "smallint", "int2":
		name = "INT2"
	case "integer", "int", "int4", "serial":
		name = "INT4"
	case "bigint", "int8", "bigserial":
		name = "INT8"
	case "real", "float4":
		name = "FLOAT4"
	case "double precision", "float8", "double", "float":
		name = "FLOAT8"
	case "numeric", "decimal":
		name = "NUMERIC"
		if a := parseArgs(); len(a) >= 1 {
			prec, scale = a[0], 0
			if len(a) >= 2 {
				scale = a[1]
			}
		}
	case "boolean", "bool":
		name = "BOOL"
	case "text", "string":
		name = "TEXT"
	case "varchar", "character varying":
		name = "VARCHAR"
		if a := parseArgs(); len(a) == 1 {
			length = a[0]
		}
	case "char", "character", "bpchar":
		name = "BPCHAR"
		if a := parseArgs(); len(a) == 1 {
			length = a[0]
		}
	case "bytea", "blob":
		name = "BYTEA"
	case "date":
		name = "DATE"
	case "timestamp", "timestamp without time zone", "datetime":
		name = "TIMESTAMP"
	case "timestamptz", "timestamp with time zone":
		name = "TIMESTAMPTZ"
	case "time", "time without time zone":
		name = "TIME"
	case "uuid":
		name = "UUID"
	case "json":
		name = "JSON"
	case "jsonb":
		name = "JSONB"
	case "vector":
		name = "VECTOR"
		if a := parseArgs(); len(a) == 1 {
			length = a[0]
		}
	case "interval":
		name = "INTERVAL"
	default:
		name = strings.ToUpper(base)
	}
	return name, length, prec, scale
}

func describeField(f *arrowField) FieldDescription {
	fd := FieldDescription{Name: f.name, Nullable: f.nullable, Length: -1, Precision: -1, Scale: -1, field: f}
	name, length, prec, scale := normalizeDecl(f.decl)
	fd.Length, fd.Precision, fd.Scale = length, prec, scale
	if strings.HasPrefix(name, "_") && f.typ == atUtf8 {
		fd.DatabaseTypeName = name
		fd.kind = ckArray
		fd.elemKind = kindForName(name[1:])
		return fd
	}
	switch f.typ {
	case atNull:
		fd.kind = ckNull
		if name == "" {
			name = "TEXT"
		}
	case atBool:
		fd.kind = ckBool
		name = "BOOL"
	case atInt:
		fd.kind = ckInt
		if name == "" || (name != "INT2" && name != "INT4" && name != "INT8") {
			name = "INT8"
		}
	case atFloatingPoint:
		if f.prec == 1 {
			fd.kind, name = ckFloat32, "FLOAT4"
		} else {
			fd.kind, name = ckFloat64, "FLOAT8"
		}
	case atDecimal:
		fd.kind, name = ckDecimal, "NUMERIC"
		if fd.Precision < 0 && f.dprec > 0 {
			fd.Precision, fd.Scale = int64(f.dprec), int64(f.dscale)
		}
	case atUtf8, atLargeUtf8:
		switch name {
		case "NUMERIC":
			fd.kind = ckNumericText
		case "JSON", "JSONB":
			fd.kind = ckJSON
		case "UUID":
			fd.kind = ckUUID
		case "TIME":
			fd.kind = ckTime
		case "", "TEXT", "VARCHAR", "BPCHAR":
			fd.kind = ckText
			if name == "" {
				name = "TEXT"
			}
		default:
			fd.kind = ckText
		}
	case atBinary, atLargeBinary, atFixedSizeBinary:
		if name == "VECTOR" {
			fd.kind = ckVector
		} else {
			fd.kind, name = ckBytes, "BYTEA"
		}
	case atDate:
		fd.kind, name = ckDate, "DATE"
	case atTimestamp:
		if name == "TIMESTAMPTZ" || f.tz != "" {
			fd.kind, name = ckTimestampTZ, "TIMESTAMPTZ"
		} else {
			fd.kind, name = ckTimestamp, "TIMESTAMP"
		}
	case atTime:
		fd.kind, name = ckTime, "TIME"
	case atFixedSizeList, atList, atLargeList:
		if name == "VECTOR" || (len(f.children) == 1 && f.children[0].typ == atFloatingPoint) {
			fd.kind = ckVector
			if name == "" {
				name = "VECTOR"
			}
		} else {
			fd.kind = ckOther
		}
	default:
		fd.kind = ckOther
		if name == "" {
			name = "UNKNOWN"
		}
	}
	fd.DatabaseTypeName = name
	return fd
}

func kindForName(n string) colKind {
	switch n {
	case "INT2", "INT4", "INT8":
		return ckInt
	case "FLOAT4":
		return ckFloat32
	case "FLOAT8":
		return ckFloat64
	case "NUMERIC":
		return ckNumericText
	case "BOOL":
		return ckBool
	case "UUID":
		return ckUUID
	case "BYTEA":
		return ckBytes
	case "DATE":
		return ckDate
	case "TIMESTAMP":
		return ckTimestamp
	case "TIMESTAMPTZ":
		return ckTimestampTZ
	case "JSON", "JSONB":
		return ckJSON
	}
	return ckText
}

// ScanType returns the Go type a column scans into naturally (for
// database/sql ColumnTypeScanType).
func (fd *FieldDescription) scanType(nativeTypes bool) reflect.Type {
	switch fd.kind {
	case ckBool:
		return reflect.TypeOf(false)
	case ckInt:
		return reflect.TypeOf(int64(0))
	case ckFloat32:
		if nativeTypes {
			return reflect.TypeOf(float32(0))
		}
		return reflect.TypeOf(float64(0))
	case ckFloat64:
		return reflect.TypeOf(float64(0))
	case ckDecimal, ckNumericText:
		if nativeTypes {
			return reflect.TypeOf(Decimal{})
		}
		return reflect.TypeOf("")
	case ckBytes:
		return reflect.TypeOf([]byte(nil))
	case ckJSON:
		if nativeTypes {
			return reflect.TypeOf(json.RawMessage(nil))
		}
		return reflect.TypeOf([]byte(nil))
	case ckUUID:
		if nativeTypes {
			return reflect.TypeOf(UUID{})
		}
		return reflect.TypeOf("")
	case ckVector:
		if nativeTypes {
			return reflect.TypeOf(Vector(nil))
		}
		return reflect.TypeOf("")
	case ckDate, ckTimestamp, ckTimestampTZ:
		return reflect.TypeOf(time.Time{})
	case ckArray:
		if nativeTypes {
			return reflect.TypeOf([]any(nil))
		}
		return reflect.TypeOf("")
	case ckNull:
		return reflect.TypeOf((*any)(nil)).Elem()
	}
	return reflect.TypeOf("")
}

// cell is one value of one column of the current batch.
type cell struct {
	c  *arrowCol
	fd *FieldDescription
	i  int
}

func (x cell) null() bool { return x.c.isNull(x.i) }

// text renders the cell in PostgreSQL's text form (and the conformance
// suite's canonical form for numbers).
func (x cell) text(loc *time.Location) string {
	c, i := x.c, x.i
	switch x.fd.kind {
	case ckBool:
		if c.boolAt(i) {
			return "true"
		}
		return "false"
	case ckInt:
		return strconv.FormatInt(c.intAt(i), 10)
	case ckFloat32:
		return formatFloat(float64(c.float32At(i)), 32)
	case ckFloat64:
		return formatFloat(c.floatAt(i), 64)
	case ckDecimal:
		return x.decimal().String()
	case ckBytes:
		return byteaHex(c.bytesAt(i))
	case ckVector:
		return x.vector().String()
	case ckDate:
		return x.time(loc).Format("2006-01-02")
	case ckTimestamp:
		return formatTimestamp(x.time(loc), false)
	case ckTimestampTZ:
		return formatTimestamp(x.time(time.UTC), true)
	case ckText, ckNumericText, ckJSON, ckUUID, ckArray, ckTime:
		return string(c.bytesAt(i))
	case ckNull:
		return ""
	}
	if c.f.typ == atUtf8 || c.f.typ == atLargeUtf8 || c.f.typ == atBinary || c.f.typ == atLargeBinary {
		return string(c.bytesAt(i))
	}
	return ""
}

func formatFloat(f float64, bits int) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	return strconv.FormatFloat(f, 'g', -1, bits)
}

func formatTimestamp(t time.Time, tz bool) string {
	s := t.Format("2006-01-02 15:04:05.999999")
	if t.Year() <= 0 {
		// PostgreSQL's BC spelling
		y := 1 - t.Year()
		s = strconv.Itoa(y) + t.Format("-01-02 15:04:05.999999") + " BC"
		if tz {
			return strings.Replace(s, " BC", "+00 BC", 1)
		}
		return s
	}
	if tz {
		s += "+00"
	}
	return s
}

func (x cell) decimal() Decimal {
	c := x.c
	switch x.fd.kind {
	case ckDecimal:
		w := c.f.bitWidth / 8
		return decimalFromArrow(c.data[w*x.i:w*(x.i+1)], c.f.dscale)
	case ckInt:
		return NewDecimal(c.intAt(x.i), 0)
	}
	d, _ := ParseDecimal(string(c.bytesAt(x.i)))
	return d
}

func (x cell) vector() Vector {
	c := x.c
	if c.child != nil {
		s, e := c.listRange(x.i)
		v := make(Vector, e-s)
		for k := s; k < e; k++ {
			v[k-s] = c.child.float32At(k)
		}
		return v
	}
	return vectorFromBytes(c.bytesAt(x.i))
}

func (x cell) time(loc *time.Location) time.Time {
	c, i := x.c, x.i
	switch c.f.typ {
	case atDate:
		if c.f.unit == 0 {
			days := int64(int32(leU32(c.data[4*i:])))
			return time.Unix(days*86400, 0).UTC()
		}
		return time.UnixMilli(int64(leU64(c.data[8*i:]))).UTC()
	case atTimestamp:
		v := int64(leU64(c.data[8*i:]))
		var t time.Time
		switch c.f.unit {
		case 0:
			t = time.Unix(v, 0)
		case 1:
			t = time.UnixMilli(v)
		case 2:
			t = time.UnixMicro(v)
		default:
			t = time.Unix(0, v)
		}
		if x.fd.kind == ckTimestampTZ {
			if loc == nil {
				loc = time.Local
			}
			return t.In(loc)
		}
		return t.UTC()
	}
	return time.Time{}
}

func leU32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
func leU64(b []byte) uint64 {
	return uint64(leU32(b)) | uint64(leU32(b[4:]))<<32
}

// value is the cell's natural Go value. native selects the richer types of
// the native API (Decimal, UUID, Vector, json.RawMessage, []any arrays,
// float32); otherwise the database/sql driver.Value forms.
func (x cell) value(native bool, loc *time.Location) any {
	if x.null() {
		return nil
	}
	c, i := x.c, x.i
	switch x.fd.kind {
	case ckBool:
		return c.boolAt(i)
	case ckInt:
		return c.intAt(i)
	case ckFloat32:
		if native {
			return c.float32At(i)
		}
		return float64(c.float32At(i))
	case ckFloat64:
		return c.floatAt(i)
	case ckDecimal, ckNumericText:
		if native {
			return x.decimal()
		}
		if x.fd.kind == ckDecimal {
			return x.decimal().String()
		}
		return string(c.bytesAt(i))
	case ckText, ckTime:
		return string(c.bytesAt(i))
	case ckJSON:
		b := c.bytesAt(i)
		out := make([]byte, len(b))
		copy(out, b)
		if native {
			return json.RawMessage(out)
		}
		return out
	case ckUUID:
		if native {
			u, err := ParseUUID(string(c.bytesAt(i)))
			if err == nil {
				return u
			}
		}
		return string(c.bytesAt(i))
	case ckBytes:
		b := c.bytesAt(i)
		out := make([]byte, len(b))
		copy(out, b)
		return out
	case ckVector:
		if native {
			return x.vector()
		}
		return x.vector().String()
	case ckDate, ckTimestamp, ckTimestampTZ:
		return x.time(loc)
	case ckArray:
		if native {
			v, err := parseArrayAny(string(c.bytesAt(i)), x.fd.elemKind)
			if err == nil {
				return v
			}
		}
		return string(c.bytesAt(i))
	case ckNull:
		return nil
	}
	return x.text(loc)
}
