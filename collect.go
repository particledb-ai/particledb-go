package particledb

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

// CollectableRow is the row a RowToFunc reads.
type CollectableRow interface {
	FieldDescriptions() []FieldDescription
	Scan(dest ...any) error
	Values() ([]any, error)
}

// RowToFunc converts one row to a T.
type RowToFunc[T any] func(row CollectableRow) (T, error)

// CollectRows reads every row with fn and closes rows.
//
//	users, err := particledb.CollectRows(rows, particledb.RowToStructByName[User])
func CollectRows[T any](rows *Rows, fn RowToFunc[T]) ([]T, error) {
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := fn(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// AppendRows appends every row to s.
func AppendRows[T any, S ~[]T](s S, rows *Rows, fn RowToFunc[T]) (S, error) {
	defer rows.Close()
	for rows.Next() {
		v, err := fn(rows)
		if err != nil {
			return nil, err
		}
		s = append(s, v)
	}
	return s, rows.Err()
}

// CollectOneRow reads the first row (ErrNoRows when there is none) and
// closes rows.
func CollectOneRow[T any](rows *Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()
	var zero T
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return zero, err
		}
		return zero, ErrNoRows
	}
	v, err := fn(rows)
	if err != nil {
		return zero, err
	}
	rows.Close()
	return v, rows.Err()
}

// CollectExactlyOneRow is CollectOneRow that also fails when there is more
// than one row.
func CollectExactlyOneRow[T any](rows *Rows, fn RowToFunc[T]) (T, error) {
	defer rows.Close()
	var zero T
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return zero, err
		}
		return zero, ErrNoRows
	}
	v, err := fn(rows)
	if err != nil {
		return zero, err
	}
	if rows.Next() {
		return zero, ErrTooManyRows
	}
	return v, rows.Err()
}

// ErrTooManyRows is returned by CollectExactlyOneRow.
var ErrTooManyRows = fmt.Errorf("particledb: too many rows in result set")

// ForEachRow scans each row into scans and calls fn.
func ForEachRow(rows *Rows, scans []any, fn func() error) (CommandTag, error) {
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return CommandTag{}, err
		}
		if err := fn(); err != nil {
			return CommandTag{}, err
		}
	}
	rows.Close()
	return rows.CommandTag(), rows.Err()
}

// RowTo scans a one-column row into a T.
func RowTo[T any](row CollectableRow) (T, error) {
	var v T
	err := row.Scan(&v)
	return v, err
}

// RowToAddrOf scans a one-column row into a new *T.
func RowToAddrOf[T any](row CollectableRow) (*T, error) {
	v := new(T)
	err := row.Scan(v)
	return v, err
}

// RowToMap returns the row as a column name to value map.
func RowToMap(row CollectableRow) (map[string]any, error) {
	vals, err := row.Values()
	if err != nil {
		return nil, err
	}
	fds := row.FieldDescriptions()
	m := make(map[string]any, len(vals))
	for i, v := range vals {
		m[fds[i].Name] = v
	}
	return m, nil
}

// RowToStructByName scans a row into a struct by column name: a field's
// `db:"name"` tag, else its name matched case-insensitively (also in
// snake_case). Every column must have a field and every field (except
// `db:"-"`) a column.
func RowToStructByName[T any](row CollectableRow) (T, error) {
	var v T
	err := scanStruct(row, &v, true)
	return v, err
}

// RowToStructByNameLax is RowToStructByName that leaves fields without a
// column at their zero value.
func RowToStructByNameLax[T any](row CollectableRow) (T, error) {
	var v T
	err := scanStruct(row, &v, false)
	return v, err
}

// RowToAddrOfStructByName is RowToStructByName returning a pointer.
func RowToAddrOfStructByName[T any](row CollectableRow) (*T, error) {
	v := new(T)
	err := scanStruct(row, v, true)
	return v, err
}

// RowToStructByPos scans a row into a struct's exported fields in order.
func RowToStructByPos[T any](row CollectableRow) (T, error) {
	var v T
	rv := reflect.ValueOf(&v).Elem()
	if rv.Kind() != reflect.Struct {
		return v, fmt.Errorf("particledb: RowToStructByPos needs a struct, got %T", v)
	}
	var dest []any
	for i := 0; i < rv.NumField(); i++ {
		f := rv.Type().Field(i)
		if f.PkgPath != "" || f.Tag.Get("db") == "-" {
			continue
		}
		dest = append(dest, rv.Field(i).Addr().Interface())
	}
	err := row.Scan(dest...)
	return v, err
}

type structPlan struct {
	index [][]int // per column: field index path, nil to skip
	err   error
}

var structPlans sync.Map // key: planKey

type planKey struct {
	t      reflect.Type
	cols   string
	strict bool
}

func scanStruct(row CollectableRow, dst any, strict bool) error {
	rv := reflect.ValueOf(dst).Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("particledb: struct scan needs a struct, got %s", rv.Type())
	}
	fds := row.FieldDescriptions()
	var sb strings.Builder
	for _, fd := range fds {
		sb.WriteString(fd.Name)
		sb.WriteByte(0)
	}
	key := planKey{rv.Type(), sb.String(), strict}
	var plan *structPlan
	if p, ok := structPlans.Load(key); ok {
		plan = p.(*structPlan)
	} else {
		plan = buildPlan(rv.Type(), fds, strict)
		structPlans.Store(key, plan)
	}
	if plan.err != nil {
		return plan.err
	}
	dest := make([]any, len(fds))
	for i, idx := range plan.index {
		if idx != nil {
			dest[i] = fieldByIndexAlloc(rv, idx).Addr().Interface()
		}
	}
	return row.Scan(dest...)
}

func fieldByIndexAlloc(v reflect.Value, idx []int) reflect.Value {
	for i, x := range idx {
		if i > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(x)
	}
	return v
}

func snake(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func collectFields(t reflect.Type, prefix []int, out map[string][]int, order *[]string) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("db")
		if tag == "-" {
			continue
		}
		path := append(append([]int(nil), prefix...), i)
		if f.Anonymous && tag == "" {
			ft := f.Type
			if ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct {
				collectFields(ft, path, out, order)
				continue
			}
		}
		if f.PkgPath != "" {
			continue
		}
		names := []string{strings.ToLower(f.Name), snake(f.Name)}
		if tag != "" {
			names = []string{strings.ToLower(strings.Split(tag, ",")[0])}
		}
		for _, n := range names {
			if _, dup := out[n]; !dup {
				out[n] = path
			}
		}
		*order = append(*order, names[0])
	}
}

func buildPlan(t reflect.Type, fds []FieldDescription, strict bool) *structPlan {
	byName := map[string][]int{}
	var order []string
	collectFields(t, nil, byName, &order)
	p := &structPlan{index: make([][]int, len(fds))}
	used := map[string]bool{}
	for i, fd := range fds {
		n := strings.ToLower(fd.Name)
		idx, ok := byName[n]
		if !ok {
			if strict {
				p.err = fmt.Errorf("particledb: struct %s has no field for column %q", t, fd.Name)
				return p
			}
			continue
		}
		p.index[i] = idx
		used[fmt.Sprint(idx)] = true
	}
	if strict {
		for _, n := range order {
			if !used[fmt.Sprint(byName[n])] {
				p.err = fmt.Errorf("particledb: struct %s field %q has no column", t, n)
				return p
			}
		}
	}
	return p
}
