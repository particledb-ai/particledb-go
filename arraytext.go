package particledb

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// arrayNode is a parsed external array: either a list of sub-arrays or of
// elements (nil for NULL).
type arrayNode struct {
	elems []*string
	subs  []*arrayNode
	leaf  bool
}

// parseArrayText parses PostgreSQL's external array text ({1,NULL,"a,b"},
// {{1,2},{3,4}}, {}), optionally with a [lo:hi]= dimension prefix.
func parseArrayText(s string) (*arrayNode, error) {
	t := strings.TrimSpace(s)
	if strings.HasPrefix(t, "[") {
		if i := strings.Index(t, "="); i >= 0 {
			t = t[i+1:]
		}
	}
	p := &arrayParser{s: t}
	n, err := p.parse()
	if err != nil {
		return nil, err
	}
	p.skipSpace()
	if p.i != len(p.s) {
		return nil, fmt.Errorf("particledb: trailing text after array %q", s)
	}
	return n, nil
}

type arrayParser struct {
	s string
	i int
}

func (p *arrayParser) skipSpace() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t' || p.s[p.i] == '\n') {
		p.i++
	}
}

func (p *arrayParser) parse() (*arrayNode, error) {
	p.skipSpace()
	if p.i >= len(p.s) || p.s[p.i] != '{' {
		return nil, fmt.Errorf("particledb: array text %q does not start with {", p.s)
	}
	p.i++
	n := &arrayNode{}
	p.skipSpace()
	if p.i < len(p.s) && p.s[p.i] == '}' {
		p.i++
		n.leaf = true
		return n, nil
	}
	for {
		p.skipSpace()
		if p.i >= len(p.s) {
			return nil, fmt.Errorf("particledb: unterminated array text")
		}
		if p.s[p.i] == '{' {
			sub, err := p.parse()
			if err != nil {
				return nil, err
			}
			n.subs = append(n.subs, sub)
		} else {
			n.leaf = true
			e, err := p.elem()
			if err != nil {
				return nil, err
			}
			n.elems = append(n.elems, e)
		}
		p.skipSpace()
		if p.i >= len(p.s) {
			return nil, fmt.Errorf("particledb: unterminated array text")
		}
		switch p.s[p.i] {
		case ',':
			p.i++
		case '}':
			p.i++
			return n, nil
		default:
			return nil, fmt.Errorf("particledb: unexpected %q in array text", p.s[p.i])
		}
	}
}

func (p *arrayParser) elem() (*string, error) {
	if p.s[p.i] == '"' {
		p.i++
		var b strings.Builder
		for p.i < len(p.s) {
			c := p.s[p.i]
			if c == '\\' && p.i+1 < len(p.s) {
				b.WriteByte(p.s[p.i+1])
				p.i += 2
				continue
			}
			if c == '"' {
				p.i++
				v := b.String()
				return &v, nil
			}
			b.WriteByte(c)
			p.i++
		}
		return nil, fmt.Errorf("particledb: unterminated quoted array element")
	}
	start := p.i
	for p.i < len(p.s) && p.s[p.i] != ',' && p.s[p.i] != '}' {
		p.i++
	}
	raw := strings.TrimSpace(p.s[start:p.i])
	if strings.EqualFold(raw, "NULL") {
		return nil, nil
	}
	return &raw, nil
}

// parseArrayAny converts array text to nested []any with typed elements.
func parseArrayAny(s string, k colKind) ([]any, error) {
	n, err := parseArrayText(s)
	if err != nil {
		return nil, err
	}
	return n.toAny(k)
}

func (n *arrayNode) toAny(k colKind) ([]any, error) {
	if !n.leaf {
		out := make([]any, len(n.subs))
		for i, s := range n.subs {
			v, err := s.toAny(k)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	out := make([]any, len(n.elems))
	for i, e := range n.elems {
		if e == nil {
			continue
		}
		v, err := convertElemText(*e, k)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func convertElemText(s string, k colKind) (any, error) {
	switch k {
	case ckInt:
		return strconv.ParseInt(s, 10, 64)
	case ckFloat32:
		f, err := strconv.ParseFloat(s, 32)
		return float32(f), err
	case ckFloat64:
		return strconv.ParseFloat(s, 64)
	case ckBool:
		return s == "t" || s == "true", nil
	case ckNumericText, ckDecimal:
		return ParseDecimal(s)
	case ckUUID:
		return ParseUUID(s)
	case ckBytes:
		return decodeByteaText(s)
	case ckDate, ckTimestamp, ckTimestampTZ:
		return parseTimeText(s)
	}
	return s, nil
}

func decodeByteaText(s string) ([]byte, error) {
	if strings.HasPrefix(s, `\x`) {
		b := make([]byte, (len(s)-2)/2)
		for i := range b {
			v, err := strconv.ParseUint(s[2+2*i:4+2*i], 16, 8)
			if err != nil {
				return nil, err
			}
			b[i] = byte(v)
		}
		return b, nil
	}
	return []byte(s), nil
}

var timeLayouts = []string{
	"2006-01-02 15:04:05.999999999Z07:00:00",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07",
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02",
}

func parseTimeText(s string) (time.Time, error) {
	for _, l := range timeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("particledb: cannot parse time %q", s)
}

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// scanArrayText parses array text into dest, a pointer to a (nested) slice.
func scanArrayText(text string, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("particledb: Array scan needs a pointer to a slice, got %T", dest)
	}
	n, err := parseArrayText(text)
	if err != nil {
		return err
	}
	return assignArray(n, rv.Elem())
}

func assignArray(n *arrayNode, dst reflect.Value) error {
	if dst.Kind() != reflect.Slice {
		return fmt.Errorf("particledb: cannot scan an array into %s", dst.Type())
	}
	et := dst.Type().Elem()
	if !n.leaf {
		out := reflect.MakeSlice(dst.Type(), len(n.subs), len(n.subs))
		for i, s := range n.subs {
			if err := assignArray(s, out.Index(i)); err != nil {
				return err
			}
		}
		dst.Set(out)
		return nil
	}
	out := reflect.MakeSlice(dst.Type(), len(n.elems), len(n.elems))
	for i, e := range n.elems {
		if err := assignElem(e, out.Index(i), et); err != nil {
			return err
		}
	}
	dst.Set(out)
	return nil
}

func assignElem(e *string, dst reflect.Value, et reflect.Type) error {
	if reflect.PointerTo(et).Implements(scannerType) {
		sc := dst.Addr().Interface().(sql.Scanner)
		if e == nil {
			return sc.Scan(nil)
		}
		return sc.Scan(*e)
	}
	if et.Kind() == reflect.Pointer {
		if e == nil {
			dst.Set(reflect.Zero(et))
			return nil
		}
		nv := reflect.New(et.Elem())
		if err := assignElem(e, nv.Elem(), et.Elem()); err != nil {
			return err
		}
		dst.Set(nv)
		return nil
	}
	if e == nil {
		if et.Kind() == reflect.Interface {
			return nil
		}
		return fmt.Errorf("particledb: array has a NULL element; scan into a slice of pointers or sql.Null types")
	}
	s := *e
	switch et {
	case reflect.TypeOf(time.Time{}):
		t, err := parseTimeText(s)
		if err != nil {
			return err
		}
		dst.Set(reflect.ValueOf(t))
		return nil
	}
	switch et.Kind() {
	case reflect.String:
		dst.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v, err := strconv.ParseInt(s, 10, et.Bits())
		if err != nil {
			return fmt.Errorf("particledb: array element %q: %w", s, err)
		}
		dst.SetInt(v)
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v, err := strconv.ParseUint(s, 10, et.Bits())
		if err != nil {
			return fmt.Errorf("particledb: array element %q: %w", s, err)
		}
		dst.SetUint(v)
	case reflect.Float32, reflect.Float64:
		v, err := strconv.ParseFloat(s, et.Bits())
		if err != nil {
			return fmt.Errorf("particledb: array element %q: %w", s, err)
		}
		dst.SetFloat(v)
	case reflect.Bool:
		dst.SetBool(s == "t" || s == "true")
	case reflect.Slice:
		if et.Elem().Kind() == reflect.Uint8 {
			b, err := decodeByteaText(s)
			if err != nil {
				return err
			}
			dst.SetBytes(b)
			return nil
		}
		return fmt.Errorf("particledb: cannot scan array element into %s", et)
	case reflect.Interface:
		dst.Set(reflect.ValueOf(s))
	default:
		return fmt.Errorf("particledb: cannot scan array element into %s", et)
	}
	return nil
}
