package legacy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// DefaultGRPCPort is the default gRPC port for ParticleDB.
const DefaultGRPCPort = 26257

// GRPCClient provides access to ParticleDB over gRPC. It wraps connections to
// both the ParticleDB service (SQL execution, batch, health) and the
// QueryService (query, streaming query, DML execute).
type GRPCClient struct {
	conn   *grpc.ClientConn
	host   string
	port   int
	pdb    particleDBServiceClient
	query  queryServiceClient
}

// GRPCOption configures a GRPCClient.
type GRPCOption func(*grpcOptions)

type grpcOptions struct {
	dialOpts []grpc.DialOption
	timeout  time.Duration
}

// WithDialOptions appends additional grpc.DialOptions to the connection.
func WithDialOptions(opts ...grpc.DialOption) GRPCOption {
	return func(o *grpcOptions) {
		o.dialOpts = append(o.dialOpts, opts...)
	}
}

// WithConnectTimeout sets the timeout for establishing the gRPC connection.
func WithConnectTimeout(d time.Duration) GRPCOption {
	return func(o *grpcOptions) {
		o.timeout = d
	}
}

// NewGRPCClient creates a new gRPC client connected to the ParticleDB server
// at host:port. The connection uses insecure credentials by default; pass
// WithDialOptions to override.
func NewGRPCClient(host string, port int, opts ...GRPCOption) (*GRPCClient, error) {
	o := &grpcOptions{
		timeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	dialOpts = append(dialOpts, o.dialOpts...)

	target := fmt.Sprintf("%s:%d", host, port)

	ctx, cancel := context.WithTimeout(context.Background(), o.timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("particledb: grpc dial %s failed: %w", target, err)
	}

	return &GRPCClient{
		conn:  conn,
		host:  host,
		port:  port,
		pdb:   newParticleDBServiceClient(conn),
		query: newQueryServiceClient(conn),
	}, nil
}

// Close closes the underlying gRPC connection.
func (c *GRPCClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// ParticleDB service RPCs
// ---------------------------------------------------------------------------

// ExecuteSQL executes a single SQL statement via the ParticleDB.ExecuteSQL RPC
// and returns the response. This is the primary entry point for both queries
// (SELECT) and DML statements (INSERT/UPDATE/DELETE) when using the
// ParticleDB service.
func (c *GRPCClient) ExecuteSQL(ctx context.Context, sql string, params ...*PBValue) (*PBExecuteSqlResponse, error) {
	req := &PBExecuteSqlRequest{
		Sql:    sql,
		Params: params,
	}
	return c.pdb.ExecuteSQL(ctx, req)
}

// BatchExecute executes multiple SQL statements in sequence via the
// ParticleDB.BatchExecute RPC.
func (c *GRPCClient) BatchExecute(ctx context.Context, sqls []string) (*PBBatchExecuteResponse, error) {
	req := &PBBatchExecuteRequest{
		Statements: sqls,
	}
	return c.pdb.BatchExecute(ctx, req)
}

// Health calls the ParticleDB.Health RPC.
func (c *GRPCClient) Health(ctx context.Context) (*PBHealthResponse, error) {
	return c.pdb.Health(ctx, &PBHealthRequest{})
}

// ---------------------------------------------------------------------------
// QueryService RPCs
// ---------------------------------------------------------------------------

// Query executes a SELECT query via the QueryService.Query RPC.
func (c *GRPCClient) Query(ctx context.Context, sql string) (*PBQueryResponse, error) {
	req := &PBQueryRequest{
		Sql: sql,
	}
	return c.query.Query(ctx, req)
}

// QueryWithOptions executes a query with full control over the request.
func (c *GRPCClient) QueryWithOptions(ctx context.Context, req *PBQueryRequest) (*PBQueryResponse, error) {
	return c.query.Query(ctx, req)
}

// StreamQuery calls the QueryService.QueryStream server-streaming RPC and
// returns a channel that delivers QueryResponse messages as they arrive. The
// channel is closed when the stream ends or an error occurs. Errors are
// delivered on the returned error channel (buffered, size 1).
func (c *GRPCClient) StreamQuery(ctx context.Context, sql string) (<-chan *PBQueryResponse, <-chan error) {
	ch := make(chan *PBQueryResponse, 64)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errCh)

		req := &PBQueryRequest{Sql: sql}
		stream, err := c.query.QueryStream(ctx, req)
		if err != nil {
			errCh <- fmt.Errorf("particledb: QueryStream RPC failed: %w", err)
			return
		}

		for {
			resp, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				errCh <- fmt.Errorf("particledb: QueryStream recv failed: %w", err)
				return
			}
			select {
			case ch <- resp:
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			}
		}
	}()

	return ch, errCh
}

// Execute runs a DML statement (INSERT, UPDATE, DELETE) via the
// QueryService.Execute RPC.
func (c *GRPCClient) Execute(ctx context.Context, sql string) (*PBExecuteResponse, error) {
	req := &PBExecuteRequest{
		Sql: sql,
	}
	return c.query.Execute(ctx, req)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// IntValue creates a PBValue holding an int64.
func IntValue(v int64) *PBValue {
	return &PBValue{Kind: &PBValue_IntValue{IntValue: v}}
}

// FloatValue creates a PBValue holding a float64.
func FloatValue(v float64) *PBValue {
	return &PBValue{Kind: &PBValue_FloatValue{FloatValue: v}}
}

// StringValue creates a PBValue holding a string.
func StringValue(v string) *PBValue {
	return &PBValue{Kind: &PBValue_StringValue{StringValue: v}}
}

// BoolValue creates a PBValue holding a bool.
func BoolValue(v bool) *PBValue {
	return &PBValue{Kind: &PBValue_BoolValue{BoolValue: v}}
}

// NullValue creates a PBValue representing NULL.
func NullValue() *PBValue {
	return &PBValue{Kind: &PBValue_NullValue{NullValue: true}}
}

// BytesValue creates a PBValue holding a byte slice.
func BytesValue(v []byte) *PBValue {
	return &PBValue{Kind: &PBValue_BytesValue{BytesValue: v}}
}

// ResultToMaps is a convenience helper that converts a PBExecuteSqlResponse
// into the same []map[string]interface{} format used by the PG-wire Client.
func ResultToMaps(resp *PBExecuteSqlResponse) []map[string]interface{} {
	if resp == nil {
		return nil
	}
	colNames := make([]string, len(resp.Columns))
	for i, col := range resp.Columns {
		colNames[i] = col.Name
	}
	rows := make([]map[string]interface{}, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		m := make(map[string]interface{}, len(colNames))
		for i, val := range row.Values {
			if i < len(colNames) {
				m[colNames[i]] = pbValueToInterface(val)
			}
		}
		rows = append(rows, m)
	}
	return rows
}

func pbValueToInterface(v *PBValue) interface{} {
	if v == nil {
		return nil
	}
	switch k := v.Kind.(type) {
	case *PBValue_IntValue:
		return k.IntValue
	case *PBValue_FloatValue:
		return k.FloatValue
	case *PBValue_StringValue:
		return k.StringValue
	case *PBValue_BoolValue:
		return k.BoolValue
	case *PBValue_NullValue:
		return nil
	case *PBValue_BytesValue:
		return k.BytesValue
	default:
		return nil
	}
}

// Ensure proto import is used (the generated message types implement proto.Message).
var _ = proto.Marshal
