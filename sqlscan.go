package particledb

import (
	"strings"
)

const (
	tokText = iota
	tokPlaceholder
	tokSemicolon
)

// scanSQL walks sql and reports runs of text, $n placeholders and top-level
// semicolons. Quoted strings ('...' with ” escapes, E'...' with backslash
// escapes), quoted identifiers, dollar-quoted strings and comments (-- and
// nested /* */) are text.
func scanSQL(sql string, emit func(kind int, s string)) error {
	start := 0
	flush := func(i int) {
		if i > start {
			emit(tokText, sql[start:i])
		}
		start = i
	}
	i := 0
	n := len(sql)
	for i < n {
		c := sql[i]
		switch {
		case c == '\'':
			escapes := i > 0 && (sql[i-1] == 'E' || sql[i-1] == 'e') && (i < 2 || !isIdentChar(sql[i-2]))
			i++
			for i < n {
				if escapes && sql[i] == '\\' {
					i += 2
					continue
				}
				if sql[i] == '\'' {
					if i+1 < n && sql[i+1] == '\'' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			i++
		case c == '"':
			i++
			for i < n {
				if sql[i] == '"' {
					if i+1 < n && sql[i+1] == '"' {
						i += 2
						continue
					}
					break
				}
				i++
			}
			i++
		case c == '-' && i+1 < n && sql[i+1] == '-':
			for i < n && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && sql[i+1] == '*':
			depth := 1
			i += 2
			for i < n && depth > 0 {
				if sql[i] == '/' && i+1 < n && sql[i+1] == '*' {
					depth++
					i += 2
				} else if sql[i] == '*' && i+1 < n && sql[i+1] == '/' {
					depth--
					i += 2
				} else {
					i++
				}
			}
		case c == '$':
			// $n placeholder, or a dollar-quote tag $tag$ / $$.
			j := i + 1
			if j < n && sql[j] >= '0' && sql[j] <= '9' && (i == 0 || !isIdentChar(sql[i-1])) {
				for j < n && sql[j] >= '0' && sql[j] <= '9' {
					j++
				}
				flush(i)
				emit(tokPlaceholder, sql[i:j])
				start = j
				i = j
				continue
			}
			for j < n && isIdentChar(sql[j]) && !(sql[j] >= '0' && sql[j] <= '9' && j == i+1) {
				j++
			}
			if j < n && sql[j] == '$' && (i == 0 || !isIdentChar(sql[i-1])) {
				tag := sql[i : j+1]
				end := strings.Index(sql[j+1:], tag)
				if end < 0 {
					i = n
				} else {
					i = j + 1 + end + len(tag)
				}
				continue
			}
			i++
		case c == ';':
			flush(i)
			emit(tokSemicolon, ";")
			start = i + 1
			i++
		default:
			i++
		}
	}
	if i > n {
		i = n
	}
	flush(n)
	return nil
}

func isIdentChar(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c >= 0x80
}

// splitStatements splits a query string at top-level semicolons, dropping
// empty statements (and comment-only ones).
func splitStatements(sql string) []string {
	var out []string
	var cur strings.Builder
	push := func() {
		s := strings.TrimSpace(cur.String())
		cur.Reset()
		if s != "" && !onlyComments(s) {
			out = append(out, s)
		}
	}
	_ = scanSQL(sql, func(kind int, s string) {
		if kind == tokSemicolon {
			push()
			return
		}
		cur.WriteString(s)
	})
	push()
	return out
}

func onlyComments(s string) bool {
	only := true
	_ = scanSQL(s, func(kind int, t string) {})
	// strip comments manually
	i := 0
	for i < len(s) {
		switch {
		case s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r':
			i++
		case strings.HasPrefix(s[i:], "--"):
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case strings.HasPrefix(s[i:], "/*"):
			depth := 1
			i += 2
			for i < len(s) && depth > 0 {
				if strings.HasPrefix(s[i:], "/*") {
					depth++
					i += 2
				} else if strings.HasPrefix(s[i:], "*/") {
					depth--
					i += 2
				} else {
					i++
				}
			}
		default:
			only = false
			i = len(s)
		}
	}
	return only
}

// firstKeyword returns the upper-cased first word of a statement (at most
// 16 bytes), skipping leading white space, comments and parentheses. It does
// not allocate for the common case of an already upper-case keyword.
func firstKeyword(s string) string {
	i := 0
	for i < len(s) {
		switch {
		case s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r' || s[i] == '(':
			i++
		case strings.HasPrefix(s[i:], "--"):
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case strings.HasPrefix(s[i:], "/*"):
			if e := strings.Index(s[i+2:], "*/"); e >= 0 {
				i += e + 4
			} else {
				return ""
			}
		default:
			j := i
			upper := true
			for j < len(s) && isIdentChar(s[j]) && j-i < 16 {
				if s[j] >= 'a' && s[j] <= 'z' {
					upper = false
				}
				j++
			}
			if upper {
				return s[i:j]
			}
			var b [16]byte
			for k := i; k < j; k++ {
				c := s[k]
				if c >= 'a' && c <= 'z' {
					c -= 'a' - 'A'
				}
				b[k-i] = c
			}
			return string(b[:j-i])
		}
	}
	return ""
}

// isTxnControl reports whether a statement is transaction control.
func isTxnControl(stmt string) bool {
	switch firstKeyword(stmt) {
	case "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE":
		return true
	case "PREPARE":
		return strings.Contains(strings.ToUpper(stmt), "TRANSACTION")
	case "SET":
		u := strings.ToUpper(stmt)
		return strings.Contains(u, "SET TRANSACTION") || strings.Contains(u, "SESSION CHARACTERISTICS")
	}
	return false
}
