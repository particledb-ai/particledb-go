package particledb

import (
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// Decimal
// ---------------------------------------------------------------------------

// Decimal is an exact SQL numeric that keeps its scale: 1.50 and 1.5 are
// different values of Decimal (equal in value, different in scale), exactly
// as the server stores them. It also represents NaN and the infinities.
//
// Decimal implements sql.Scanner and driver.Valuer, so it works with
// database/sql and sql.Null[Decimal]. A parameter of type Decimal is sent as
// a binary numeric (no text parsing on the server).
type Decimal struct {
	small   int64    // the unscaled value when big is nil
	big     *big.Int // the unscaled value when it does not fit int64
	scale   int32
	special int8 // 0 finite, 1 NaN, 2 +Infinity, 3 -Infinity
}

// NewDecimal returns unscaled * 10^-scale.
func NewDecimal(unscaled int64, scale int32) Decimal { return Decimal{small: unscaled, scale: scale} }

// NewDecimalBig returns unscaled * 10^-scale. unscaled is copied.
func NewDecimalBig(unscaled *big.Int, scale int32) Decimal {
	if unscaled.IsInt64() {
		return Decimal{small: unscaled.Int64(), scale: scale}
	}
	return Decimal{big: new(big.Int).Set(unscaled), scale: scale}
}

// DecimalNaN, DecimalInf return the numeric specials.
func DecimalNaN() Decimal { return Decimal{special: 1} }
func DecimalInf(sign int) Decimal {
	if sign < 0 {
		return Decimal{special: 3}
	}
	return Decimal{special: 2}
}

// ParseDecimal parses a plain or exponent decimal ("1.50", "-0.0001",
// "1e3", "NaN", "Infinity"). The scale is the number of digits after the
// point (an exponent shifts it; a negative result scale is kept as zeros).
func ParseDecimal(s string) (Decimal, error) {
	t := strings.TrimSpace(s)
	switch strings.ToLower(t) {
	case "nan":
		return DecimalNaN(), nil
	case "infinity", "+infinity", "inf", "+inf":
		return DecimalInf(1), nil
	case "-infinity", "-inf":
		return DecimalInf(-1), nil
	}
	if t == "" {
		return Decimal{}, fmt.Errorf("particledb: invalid decimal %q", s)
	}
	exp := 0
	if i := strings.IndexAny(t, "eE"); i >= 0 {
		e, err := strconv.Atoi(t[i+1:])
		if err != nil {
			return Decimal{}, fmt.Errorf("particledb: invalid decimal %q", s)
		}
		exp = e
		t = t[:i]
	}
	neg := false
	if t != "" && (t[0] == '-' || t[0] == '+') {
		neg = t[0] == '-'
		t = t[1:]
	}
	intPart, frac := t, ""
	if i := strings.IndexByte(t, '.'); i >= 0 {
		intPart, frac = t[:i], t[i+1:]
	}
	if intPart == "" && frac == "" {
		return Decimal{}, fmt.Errorf("particledb: invalid decimal %q", s)
	}
	digits := intPart + frac
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return Decimal{}, fmt.Errorf("particledb: invalid decimal %q", s)
		}
	}
	scale := len(frac) - exp
	if scale < 0 {
		digits += strings.Repeat("0", -scale)
		scale = 0
	}
	if scale > math.MaxInt32 {
		return Decimal{}, fmt.Errorf("particledb: decimal %q out of range", s)
	}
	d := Decimal{scale: int32(scale)}
	if len(digits) <= 18 {
		v, _ := strconv.ParseInt(digits, 10, 64)
		if neg {
			v = -v
		}
		d.small = v
		return d, nil
	}
	b, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return Decimal{}, fmt.Errorf("particledb: invalid decimal %q", s)
	}
	if neg {
		b.Neg(b)
	}
	if b.IsInt64() {
		d.small = b.Int64()
	} else {
		d.big = b
	}
	return d, nil
}

// MustParseDecimal is ParseDecimal that panics on error.
func MustParseDecimal(s string) Decimal {
	d, err := ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

// Scale is the number of digits after the point.
func (d Decimal) Scale() int32 { return d.scale }

// Unscaled returns the unscaled integer (a new big.Int).
func (d Decimal) Unscaled() *big.Int {
	if d.big != nil {
		return new(big.Int).Set(d.big)
	}
	return big.NewInt(d.small)
}

// IsNaN reports whether d is NaN; IsInf whether it is an infinity.
func (d Decimal) IsNaN() bool { return d.special == 1 }
func (d Decimal) IsInf() bool { return d.special >= 2 }

// String renders d in plain notation with exactly Scale() fractional digits.
func (d Decimal) String() string {
	switch d.special {
	case 1:
		return "NaN"
	case 2:
		return "Infinity"
	case 3:
		return "-Infinity"
	}
	var digits string
	neg := false
	if d.big != nil {
		digits = d.big.String()
	} else {
		digits = strconv.FormatInt(d.small, 10)
	}
	if digits[0] == '-' {
		neg = true
		digits = digits[1:]
	}
	s := plainDecimal(digits, int(d.scale))
	if neg {
		return "-" + s
	}
	return s
}

func plainDecimal(mag string, scale int) string {
	if scale <= 0 {
		return mag
	}
	if len(mag) > scale {
		p := len(mag) - scale
		return mag[:p] + "." + mag[p:]
	}
	return "0." + strings.Repeat("0", scale-len(mag)) + mag
}

// Float64 converts d (inexactly).
func (d Decimal) Float64() float64 {
	switch d.special {
	case 1:
		return math.NaN()
	case 2:
		return math.Inf(1)
	case 3:
		return math.Inf(-1)
	}
	f, _ := strconv.ParseFloat(d.String(), 64)
	return f
}

// Rat converts d exactly (nil for the specials).
func (d Decimal) Rat() *big.Rat {
	if d.special != 0 {
		return nil
	}
	r := new(big.Rat).SetInt(d.Unscaled())
	if d.scale > 0 {
		r.Quo(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.scale)), nil)))
	}
	return r
}

// Equal reports whether d and o have the same value and scale.
func (d Decimal) Equal(o Decimal) bool { return d.String() == o.String() }

// Scan implements sql.Scanner.
func (d *Decimal) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		return errors.New("particledb: cannot scan NULL into Decimal (use sql.Null[Decimal] or *Decimal)")
	case string:
		x, err := ParseDecimal(v)
		*d = x
		return err
	case []byte:
		x, err := ParseDecimal(string(v))
		*d = x
		return err
	case int64:
		*d = NewDecimal(v, 0)
		return nil
	case float64:
		x, err := ParseDecimal(strconv.FormatFloat(v, 'f', -1, 64))
		*d = x
		return err
	case Decimal:
		*d = v
		return nil
	}
	return fmt.Errorf("particledb: cannot scan %T into Decimal", src)
}

// Value implements driver.Valuer (the plain decimal text).
func (d Decimal) Value() (driver.Value, error) { return d.String(), nil }

// wireBytes returns the scale and the big-endian two's-complement unscaled
// bytes of a finite d.
func (d Decimal) wireBytes(dst []byte) []byte {
	if d.big == nil {
		v := d.small
		n := 8
		for n > 1 {
			b := byte(v >> (8 * (n - 1)))
			nb := byte(v >> (8 * (n - 2)))
			if (b == 0 && nb&0x80 == 0) || (b == 0xFF && nb&0x80 != 0) {
				n--
				continue
			}
			break
		}
		for i := n - 1; i >= 0; i-- {
			dst = append(dst, byte(v>>(8*i)))
		}
		return dst
	}
	return append(dst, twosComplementBE(d.big)...)
}

func twosComplementBE(x *big.Int) []byte {
	if x.Sign() >= 0 {
		b := x.Bytes()
		if len(b) == 0 || b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return b
	}
	// -x = ^(x-1) over enough bytes.
	n := (x.BitLen() + 8) / 8
	m := new(big.Int).Add(x, new(big.Int).Lsh(big.NewInt(1), uint(8*n)))
	b := m.Bytes()
	for len(b) < n {
		b = append([]byte{0}, b...)
	}
	if b[0]&0x80 == 0 {
		b = append([]byte{0xFF}, b...)
	}
	return b
}

// decimalFromArrow builds a Decimal from a little-endian two's-complement
// Arrow decimal cell.
func decimalFromArrow(le []byte, scale int) Decimal {
	// fast path: fits int64
	neg := le[len(le)-1]&0x80 != 0
	fits := true
	for i := 8; i < len(le); i++ {
		if (neg && le[i] != 0xFF) || (!neg && le[i] != 0) {
			fits = false
			break
		}
	}
	if fits && (le[7]&0x80 != 0) == neg {
		var v uint64
		for i := 7; i >= 0; i-- {
			v = v<<8 | uint64(le[i])
		}
		return Decimal{small: int64(v), scale: int32(scale)}
	}
	be := make([]byte, len(le))
	for i := range le {
		be[len(le)-1-i] = le[i]
	}
	v := new(big.Int).SetBytes(be)
	if neg {
		v.Sub(v, new(big.Int).Lsh(big.NewInt(1), uint(8*len(le))))
	}
	if v.IsInt64() {
		return Decimal{small: v.Int64(), scale: int32(scale)}
	}
	return Decimal{big: v, scale: int32(scale)}
}

// bigDecimal is the method set of github.com/shopspring/decimal.Decimal: a
// value of that type is sent as a binary numeric without importing it.
type bigDecimal interface {
	Coefficient() *big.Int
	Exponent() int32
}

// ---------------------------------------------------------------------------
// UUID
// ---------------------------------------------------------------------------

// UUID is a 16-byte RFC 4122 UUID. A [16]byte-based type from another
// package (google/uuid.UUID, gofrs/uuid.UUID) is also bound as a uuid.
type UUID [16]byte

// ParseUUID parses the canonical form (hyphens optional, any case, braces
// allowed).
func ParseUUID(s string) (UUID, error) {
	var u UUID
	t := strings.Trim(s, "{}")
	t = strings.ReplaceAll(t, "-", "")
	if len(t) != 32 {
		return u, fmt.Errorf("particledb: invalid UUID %q", s)
	}
	if _, err := hex.Decode(u[:], []byte(t)); err != nil {
		return u, fmt.Errorf("particledb: invalid UUID %q", s)
	}
	return u, nil
}

// String is the lower-case hyphenated form.
func (u UUID) String() string {
	var b [36]byte
	hex.Encode(b[0:8], u[0:4])
	b[8] = '-'
	hex.Encode(b[9:13], u[4:6])
	b[13] = '-'
	hex.Encode(b[14:18], u[6:8])
	b[18] = '-'
	hex.Encode(b[19:23], u[8:10])
	b[23] = '-'
	hex.Encode(b[24:], u[10:])
	return string(b[:])
}

// Scan implements sql.Scanner.
func (u *UUID) Scan(src any) error {
	switch v := src.(type) {
	case string:
		x, err := ParseUUID(v)
		*u = x
		return err
	case []byte:
		if len(v) == 16 {
			copy(u[:], v)
			return nil
		}
		x, err := ParseUUID(string(v))
		*u = x
		return err
	case UUID:
		*u = v
		return nil
	case nil:
		return errors.New("particledb: cannot scan NULL into UUID")
	}
	return fmt.Errorf("particledb: cannot scan %T into UUID", src)
}

// Value implements driver.Valuer.
func (u UUID) Value() (driver.Value, error) { return u.String(), nil }

// ---------------------------------------------------------------------------
// Vector
// ---------------------------------------------------------------------------

// Vector is a vector(n) value: float32 components.
type Vector []float32

// String is the pgvector text form "[1,2,3]".
func (v Vector) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// ParseVector parses "[1,2,3]".
func ParseVector(s string) (Vector, error) {
	t := strings.TrimSpace(s)
	if len(t) < 2 || t[0] != '[' || t[len(t)-1] != ']' {
		return nil, fmt.Errorf("particledb: invalid vector %q", s)
	}
	t = strings.TrimSpace(t[1 : len(t)-1])
	if t == "" {
		return Vector{}, nil
	}
	parts := strings.Split(t, ",")
	v := make(Vector, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("particledb: invalid vector %q", s)
		}
		v[i] = float32(f)
	}
	return v, nil
}

// vectorFromBytes decodes the native wire's vector cell (little-endian
// float32 components).
func vectorFromBytes(b []byte) Vector {
	v := make(Vector, len(b)/4)
	for i := range v {
		v[i] = math.Float32frombits(uint32(b[4*i]) | uint32(b[4*i+1])<<8 | uint32(b[4*i+2])<<16 | uint32(b[4*i+3])<<24)
	}
	return v
}

// Scan implements sql.Scanner.
func (v *Vector) Scan(src any) error {
	switch s := src.(type) {
	case string:
		x, err := ParseVector(s)
		*v = x
		return err
	case []byte:
		x, err := ParseVector(string(s))
		*v = x
		return err
	case Vector:
		*v = append((*v)[:0], s...)
		return nil
	case nil:
		*v = nil
		return nil
	}
	return fmt.Errorf("particledb: cannot scan %T into Vector", src)
}

// Value implements driver.Valuer.
func (v Vector) Value() (driver.Value, error) {
	if v == nil {
		return nil, nil
	}
	return v.String(), nil
}

// ---------------------------------------------------------------------------
// Date and Timestamp (no zone)
// ---------------------------------------------------------------------------

// Date binds the calendar date of Time (in its own location) as an SQL date.
type Date struct{ time.Time }

// Timestamp binds the wall clock of Time (in its own location) as an SQL
// timestamp without time zone. A plain time.Time parameter is sent as a
// timestamptz (an instant with its offset).
type Timestamp struct{ time.Time }

// Scan implements sql.Scanner.
func (d *Date) Scan(src any) error {
	if t, ok := src.(time.Time); ok {
		d.Time = t
		return nil
	}
	return fmt.Errorf("particledb: cannot scan %T into Date", src)
}

// Scan implements sql.Scanner.
func (t *Timestamp) Scan(src any) error {
	if v, ok := src.(time.Time); ok {
		t.Time = v
		return nil
	}
	return fmt.Errorf("particledb: cannot scan %T into Timestamp", src)
}

// Value implements driver.Valuer.
func (d Date) Value() (driver.Value, error) { return d.Format("2006-01-02"), nil }

// Value implements driver.Valuer.
func (t Timestamp) Value() (driver.Value, error) {
	return t.Format("2006-01-02 15:04:05.999999"), nil
}

// daysFromCivil is the number of days from 1970-01-01 to y-m-d (proleptic
// Gregorian, astronomical years).
func daysFromCivil(y int64, m, d int) int64 {
	if m <= 2 {
		y--
	}
	era := y / 400
	if y < 0 && y%400 != 0 {
		era = (y - 399) / 400
	}
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := int64((153*mp+2)/5 + d - 1)
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}

// wallMicros is the wall clock of t as microseconds since 1970-01-01 00:00.
func wallMicros(t time.Time) int64 {
	y, mo, d := t.Date()
	h, mi, s := t.Clock()
	days := daysFromCivil(int64(y), int(mo), d)
	return days*86_400_000_000 + int64(h)*3_600_000_000 + int64(mi)*60_000_000 + int64(s)*1_000_000 + int64(t.Nanosecond()/1000)
}

// unixMicros is the instant of t in microseconds since the epoch (truncated).
func unixMicros(t time.Time) int64 {
	return t.Unix()*1_000_000 + int64(t.Nanosecond()/1000)
}
