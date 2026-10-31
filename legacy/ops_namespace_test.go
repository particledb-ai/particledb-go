package legacy

import (
	"errors"
	"strings"
	"testing"
)

// The three namespace Ops methods cannot succeed, and now say so.
//
// CreateNamespace / SetNamespace / DropNamespace used to emit
// CREATE NAMESPACE / SET NAMESPACE / DROP NAMESPACE IF EXISTS. No parser in the
// engine accepts any of those verbs on any wire, so every call was refused,
// while the doc comments described a row count on success - an outcome that
// could not occur.
//
// These tests pin the corrected behaviour:
//
//   - the call returns an error wrapping ErrNamespaceUnsupported, so
//     errors.Is-based handling keeps working;
//   - it returns WITHOUT sending SQL - the probe is a nil *Client, on which any
//     attempt to execute panics (Client.Execute dereferences c.db). A returned
//     error therefore proves nothing was sent, which is what distinguishes
//     "fails fast with an explanation" from "sends dead SQL and relays a parse
//     error";
//   - the message names the verb and the cause, so the caller learns why.
//
// Reverting the fix turns these red: the methods would panic on the nil client
// instead of returning ErrNamespaceUnsupported.

// TestNilClientProbeDetectsASend is the positive control for the probe used
// below. A zero-send claim is worthless unless the instrument can register a
// send, so this asserts that a method which DOES contact the server panics on
// the same nil client the namespace tests use.
func TestNilClientProbeDetectsASend(t *testing.T) {
	ops := &ExtendedOps{client: nil}

	defer func() {
		if recover() == nil {
			t.Fatal("nil-client probe did not panic on a method that sends SQL — " +
				"the probe cannot distinguish 'sent nothing' from 'sent something'")
		}
	}()

	//nolint:errcheck // the panic is the assertion
	_, _ = ops.RefreshMaterializedView("some_view")
}

func TestNamespaceMethodsFailWithoutSendingSQL(t *testing.T) {
	cases := []struct {
		name string
		verb string
		call func(o *ExtendedOps) (int64, error)
	}{
		{
			name: "CreateNamespace",
			verb: "CREATE NAMESPACE",
			call: func(o *ExtendedOps) (int64, error) {
				return o.CreateNamespace("tenant_acme", nil)
			},
		},
		{
			name: "CreateNamespace_with_quotas",
			verb: "CREATE NAMESPACE",
			call: func(o *ExtendedOps) (int64, error) {
				return o.CreateNamespace("tenant_acme", map[string]interface{}{"max_tables": 100})
			},
		},
		{
			name: "SetNamespace",
			verb: "SET NAMESPACE",
			call: func(o *ExtendedOps) (int64, error) {
				return o.SetNamespace("tenant_acme")
			},
		},
		{
			name: "DropNamespace",
			verb: "DROP NAMESPACE",
			call: func(o *ExtendedOps) (int64, error) {
				return o.DropNamespace("tenant_acme")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A nil client: reaching the server would panic, not return.
			ops := &ExtendedOps{client: nil}

			n, err := tc.call(ops)
			if err == nil {
				t.Fatalf("%s returned nil error — the server refuses this verb on every wire", tc.verb)
			}
			if n != 0 {
				t.Errorf("expected 0 rows affected, got %d", n)
			}
			if !errors.Is(err, ErrNamespaceUnsupported) {
				t.Errorf("errors.Is(err, ErrNamespaceUnsupported) = false; err = %v", err)
			}

			msg := err.Error()
			for _, want := range []string{
				tc.verb,
				"not implemented by any ParticleDB server",
				"can never succeed",
				"separate server per tenant",
				"databases-and-schemas",
			} {
				if !strings.Contains(msg, want) {
					t.Errorf("error message missing %q; got: %s", want, msg)
				}
			}
		})
	}
}
