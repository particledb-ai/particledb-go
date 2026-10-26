package particledb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Error is every error the server reports, and every connection failure.
// SQLState is always set: the server's SQLSTATE for a statement failure,
// class 08 for a lost or refused connection, 57014 for a cancelled or timed
// out statement.
//
// Match classes with errors.Is against the sentinels (ErrUniqueViolation,
// ErrSerializationFailure, ...) or inspect the fields with errors.As:
//
//	var pe *particledb.Error
//	if errors.As(err, &pe) && pe.SQLState == "23503" { ... }
type Error struct {
	// SQLState is the five-character SQLSTATE.
	SQLState string
	// Severity is "ERROR" for statement failures and "FATAL" for connection
	// failures.
	Severity string
	// Message is the server's message without the SQLSTATE marker.
	Message string
	// Code is the native wire's numeric error code (0 for client-side errors).
	Code uint16
	// BatchIndex is the position of the failed statement in a batch, or -1.
	BatchIndex int

	cause error
}

func (e *Error) Error() string {
	var b strings.Builder
	b.WriteString("particledb: ")
	if e.Severity != "" {
		b.WriteString(e.Severity)
		b.WriteString(": ")
	}
	b.WriteString(e.Message)
	if e.cause != nil && !strings.Contains(e.Message, e.cause.Error()) {
		b.WriteString(": ")
		b.WriteString(e.cause.Error())
	}
	b.WriteString(" (SQLSTATE ")
	b.WriteString(e.SQLState)
	b.WriteString(")")
	return b.String()
}

// Unwrap returns the underlying cause: context.Canceled or
// context.DeadlineExceeded for a cancelled statement, the network error for
// a lost connection.
func (e *Error) Unwrap() error { return e.cause }

// Is matches the SQLSTATE sentinels of this package.
func (e *Error) Is(target error) bool {
	s, ok := target.(*stateClass)
	if !ok {
		return false
	}
	return s.matches(e.SQLState)
}

// Retryable reports whether the failed transaction can be retried as a whole
// (serialization failure or deadlock).
func (e *Error) Retryable() bool {
	return e.SQLState == "40001" || e.SQLState == "40P01"
}

// stateClass is a sentinel matching one SQLSTATE or a whole class.
type stateClass struct {
	name   string
	states []string // exact states, or two-character classes
}

func (s *stateClass) Error() string { return "particledb: " + s.name }

func (s *stateClass) matches(state string) bool {
	for _, x := range s.states {
		if len(x) == 2 && strings.HasPrefix(state, x) {
			return true
		}
		if x == state {
			return true
		}
	}
	return false
}

// Sentinels for errors.Is. Each matches *Error values by SQLSTATE.
var (
	// ErrSerializationFailure is 40001: retry the whole transaction.
	ErrSerializationFailure error = &stateClass{"serialization failure", []string{"40001"}}
	// ErrDeadlock is 40P01: retry the whole transaction.
	ErrDeadlock error = &stateClass{"deadlock detected", []string{"40P01"}}
	// ErrRetryable matches 40001 and 40P01.
	ErrRetryable error = &stateClass{"retryable transaction failure", []string{"40001", "40P01"}}
	// ErrUniqueViolation is 23505.
	ErrUniqueViolation error = &stateClass{"unique violation", []string{"23505"}}
	// ErrForeignKeyViolation is 23503.
	ErrForeignKeyViolation error = &stateClass{"foreign key violation", []string{"23503"}}
	// ErrNotNullViolation is 23502.
	ErrNotNullViolation error = &stateClass{"not null violation", []string{"23502"}}
	// ErrIntegrityViolation matches class 23.
	ErrIntegrityViolation error = &stateClass{"integrity constraint violation", []string{"23"}}
	// ErrQueryCanceled is 57014: a cancelled context, an expired deadline or a
	// server statement_timeout.
	ErrQueryCanceled error = &stateClass{"query canceled", []string{"57014"}}
	// ErrConnection matches class 08: the connection is lost or was refused.
	ErrConnection error = &stateClass{"connection failure", []string{"08"}}
	// ErrInFailedTransaction is 25P02.
	ErrInFailedTransaction error = &stateClass{"in failed transaction", []string{"25P02"}}
	// ErrUndefinedTable is 42P01.
	ErrUndefinedTable error = &stateClass{"undefined table", []string{"42P01"}}
	// ErrSyntax matches 42601.
	ErrSyntax error = &stateClass{"syntax error", []string{"42601"}}
	// ErrFeatureNotSupported is 0A000.
	ErrFeatureNotSupported error = &stateClass{"feature not supported", []string{"0A000"}}
)

// IsRetryable reports whether err is a serialization failure or a deadlock.
func IsRetryable(err error) bool { return errors.Is(err, ErrRetryable) }

// SQLState returns the SQLSTATE of err, or "" when err is not an *Error.
func SQLState(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.SQLState
	}
	return ""
}

var wireBand = map[uint16]string{
	60000: "XX000", 60001: "42P01", 60002: "42P02", 60003: "25P02", 60004: "25P01",
	60005: "40P01", 60006: "55P03", 60007: "0A000", 60008: "22P02", 60009: "3B001",
	60010: "28P01", 60011: "22P04", 60012: "2201B", 60013: "2201E", 60014: "2201F",
	60015: "2201G", 60016: "2202E", 60017: "2200H", 60018: "22P05", 60019: "2BP01",
	60020: "3D000", 60021: "3F000", 60022: "42P05", 60023: "42P06", 60024: "42P07",
	60025: "42P10", 60026: "42P16", 60027: "42P17", 60028: "42P18", 60029: "42P19",
	60030: "42P20", 60031: "42P21", 60032: "428C9", 60033: "55P02", 60034: "57P01",
	60035: "57P03", 60036: "08P01", 60037: "P0001",
}

var wireProtocolCodes = map[uint16]string{
	0x00: "08006", 0x01: "08P01", 0x02: "08P01", 0x03: "08P01", 0x04: "08P01",
	0x05: "08P01", 0x06: "08P01", 0x07: "57014", 0x08: "08006", 0x10: "08004",
	0x11: "53300", 0x12: "08004", 0x13: "28000", 0x14: "28P01", 26: "26000",
	1064: "XX000",
}

// sqlStateForCode maps a native wire error code to its SQLSTATE.
func sqlStateForCode(code uint16) string {
	if code >= 60000 {
		if s, ok := wireBand[code]; ok {
			return s
		}
		return "XX000"
	}
	if s, ok := wireProtocolCodes[code]; ok {
		return s
	}
	if code >= 10000 {
		return strconv.Itoa(int(code))
	}
	if code >= 1000 {
		return "0" + strconv.Itoa(int(code))
	}
	return "XX000"
}

var engineFraming = []string{"SQL execution error: ", "SQL plan error: ", "SQL parse error: "}

func isStateChar(c byte) bool { return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') }

// serverError builds the *Error for an ERROR frame.
func serverError(code uint16, msg string) *Error {
	text := strings.TrimSpace(msg)
	state := ""
	if n := len(text); n >= 7 && text[n-1] == ']' && text[n-7] == '[' {
		cand := text[n-6 : n-1]
		ok := true
		for i := 0; i < 5; i++ {
			ok = ok && isStateChar(cand[i])
		}
		if ok {
			state = cand
			text = strings.TrimSpace(text[:n-7])
		}
	}
	if state == "" {
		state = sqlStateForCode(code)
		// "<SQLSTATE>: <message>" (a refused startup parameter, a session
		// statement refusal) carries the state when the code does not.
		if state == "XX000" && len(text) > 7 && text[5:7] == ": " {
			ok := true
			for i := 0; i < 5; i++ {
				ok = ok && isStateChar(text[i])
			}
			if ok {
				state = text[:5]
			}
		}
	}
	for _, f := range engineFraming {
		if strings.HasPrefix(text, f) {
			text = text[len(f):]
			break
		}
	}
	if len(text) > 7 && strings.HasPrefix(text, state) && text[5:7] == ": " {
		text = text[7:]
	}
	text = strings.TrimPrefix(text, "FATAL: ")
	sev := "ERROR"
	if state[:2] == "08" || state[:2] == "28" {
		sev = "FATAL"
	}
	return &Error{SQLState: state, Severity: sev, Message: text, Code: code, BatchIndex: -1}
}

func protoErr(format string, args ...any) *Error {
	return &Error{SQLState: "08P01", Severity: "FATAL", Message: "protocol error: " + fmt.Sprintf(format, args...), BatchIndex: -1}
}

func connErr(msg string, cause error) *Error {
	return &Error{SQLState: "08006", Severity: "FATAL", Message: msg, cause: cause, BatchIndex: -1}
}

func clientErr(state, format string, args ...any) *Error {
	return &Error{SQLState: state, Severity: "ERROR", Message: fmt.Sprintf(format, args...), BatchIndex: -1}
}

// canceledErr is the error of a statement ended by its context.
func canceledErr(ctxErr error, serverMsg string) *Error {
	msg := "canceling statement due to user request"
	if errors.Is(ctxErr, context.DeadlineExceeded) {
		msg = "canceling statement due to context deadline"
	}
	if serverMsg != "" {
		msg = serverMsg
	}
	return &Error{SQLState: "57014", Severity: "ERROR", Message: msg, cause: ctxErr, BatchIndex: -1}
}

// errClosed is returned by every call on a closed connection.
var errClosed = &Error{SQLState: "08003", Severity: "FATAL", Message: "connection is closed", BatchIndex: -1}
