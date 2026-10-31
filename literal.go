package particledb

import "strings"

// quoteIdent double-quotes an SQL identifier.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteLiteral single-quotes an SQL string literal, doubling quotes.
func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
