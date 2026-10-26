package legacy

import (
	"errors"
	"fmt"
	"strings"
)

// ErrNamespaceUnsupported is the sentinel every namespace method wraps.
// Test for it with errors.Is.
//
// ParticleDB has no server-side namespace surface: CREATE NAMESPACE,
// SET NAMESPACE and DROP NAMESPACE are refused with a parse error on every
// wire, so those calls can never succeed.
var ErrNamespaceUnsupported = errors.New(
	"not implemented by any ParticleDB server: there is no namespace surface " +
		"on any wire, so this call can never succeed. Use a table-name prefix, a " +
		"tenant column filtered on every query, or a separate server per tenant. " +
		"See https://particledb.ai/getting-started/databases-and-schemas/")

// namespaceUnsupported names the verb the caller reached for and wraps the
// sentinel so errors.Is(err, ErrNamespaceUnsupported) holds.
func namespaceUnsupported(verb string) error {
	return fmt.Errorf("%s is %w", verb, ErrNamespaceUnsupported)
}

// quoteIdent quotes a SQL identifier by wrapping in double quotes
// with internal double-quote escaping.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// escapeLiteral escapes a SQL string literal by doubling single quotes.
func escapeLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

// parseBool coerces a database value (bool, int, string) to a Go bool.
func parseBool(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case int64:
		return t != 0
	case float64:
		return t != 0
	case string:
		return t == "1" || t == "true" || t == "t" || t == "yes"
	}
	return false
}

// ExtendedOps provides extended database operations for ParticleDB including
// time travel, materialized views, changefeeds/CDC, geospatial functions,
// time-series operations, multi-tenant namespace management, and lakehouse
// file access.
type ExtendedOps struct {
	client *Client
}

// --- Time Travel ---

// QueryAsOf executes a query as of a historical timestamp.
// The timestamp should be in ISO-8601 format (e.g. "2024-01-01T00:00:00Z").
func (o *ExtendedOps) QueryAsOf(sql string, timestamp string, args ...interface{}) (*QueryResult, error) {
	asOfSQL := fmt.Sprintf("SET TIMESTAMP AS OF '%s'; %s", escapeLiteral(timestamp), sql)
	return o.client.Query(asOfSQL, args...)
}

// BuildQueryAsOfSQL returns the SQL for a time-travel query.
func (o *ExtendedOps) BuildQueryAsOfSQL(sql string, timestamp string) string {
	return fmt.Sprintf("SET TIMESTAMP AS OF '%s'; %s", escapeLiteral(timestamp), sql)
}

// --- Materialized Views ---

// CreateMaterializedView creates a materialized view with the given query.
// The refresh parameter controls the refresh strategy: "manual" (default) or
// an interval like "1h", "5m".
func (o *ExtendedOps) CreateMaterializedView(name string, query string, refresh string) (int64, error) {
	sql := o.BuildCreateMaterializedViewSQL(name, query, refresh)
	return o.client.Execute(sql)
}

// BuildCreateMaterializedViewSQL returns the DDL for creating a materialized view.
func (o *ExtendedOps) BuildCreateMaterializedViewSQL(name string, query string, refresh string) string {
	if refresh == "" || refresh == "manual" {
		return "CREATE MATERIALIZED VIEW " + quoteIdent(name) + " AS " + query
	}
	return "CREATE MATERIALIZED VIEW " + quoteIdent(name) + " WITH (refresh_interval = '" + escapeLiteral(refresh) + "') AS " + query
}

// RefreshMaterializedView triggers a refresh of a materialized view.
func (o *ExtendedOps) RefreshMaterializedView(name string) (int64, error) {
	return o.client.Execute(o.BuildRefreshMaterializedViewSQL(name))
}

// BuildRefreshMaterializedViewSQL returns the SQL to refresh a materialized view.
func (o *ExtendedOps) BuildRefreshMaterializedViewSQL(name string) string {
	return "REFRESH MATERIALIZED VIEW " + quoteIdent(name)
}

// DropMaterializedView drops a materialized view.
func (o *ExtendedOps) DropMaterializedView(name string) (int64, error) {
	return o.client.Execute(o.BuildDropMaterializedViewSQL(name))
}

// BuildDropMaterializedViewSQL returns the DDL to drop a materialized view.
func (o *ExtendedOps) BuildDropMaterializedViewSQL(name string) string {
	return "DROP MATERIALIZED VIEW IF EXISTS " + quoteIdent(name)
}

// --- Changefeeds / CDC ---

// CreateChangefeed creates a changefeed for change data capture on the given table.
func (o *ExtendedOps) CreateChangefeed(name string, table string, sinkType string, sinkURL string) (int64, error) {
	return o.client.Execute(o.BuildCreateChangefeedSQL(name, table, sinkType, sinkURL))
}

// BuildCreateChangefeedSQL returns the DDL for creating a changefeed.
func (o *ExtendedOps) BuildCreateChangefeedSQL(name string, table string, sinkType string, sinkURL string) string {
	return "CREATE CHANGEFEED " + quoteIdent(name) + " ON " + quoteIdent(table) + " INTO '" + escapeLiteral(sinkType) + "' WITH (sink_url = '" + escapeLiteral(sinkURL) + "')"
}

// PauseChangefeed pauses a running changefeed.
func (o *ExtendedOps) PauseChangefeed(name string) (int64, error) {
	return o.client.Execute(o.BuildPauseChangefeedSQL(name))
}

// BuildPauseChangefeedSQL returns the SQL to pause a changefeed.
func (o *ExtendedOps) BuildPauseChangefeedSQL(name string) string {
	return "ALTER CHANGEFEED " + quoteIdent(name) + " PAUSE"
}

// ResumeChangefeed resumes a paused changefeed.
func (o *ExtendedOps) ResumeChangefeed(name string) (int64, error) {
	return o.client.Execute(o.BuildResumeChangefeedSQL(name))
}

// BuildResumeChangefeedSQL returns the SQL to resume a changefeed.
func (o *ExtendedOps) BuildResumeChangefeedSQL(name string) string {
	return "ALTER CHANGEFEED " + quoteIdent(name) + " RESUME"
}

// DropChangefeed drops a changefeed.
func (o *ExtendedOps) DropChangefeed(name string) (int64, error) {
	return o.client.Execute(o.BuildDropChangefeedSQL(name))
}

// BuildDropChangefeedSQL returns the DDL to drop a changefeed.
func (o *ExtendedOps) BuildDropChangefeedSQL(name string) string {
	return "DROP CHANGEFEED IF EXISTS " + quoteIdent(name)
}

// ListChangefeeds returns all changefeeds.
func (o *ExtendedOps) ListChangefeeds() (*QueryResult, error) {
	return o.client.Query(o.BuildListChangefeedsSQL())
}

// BuildListChangefeedsSQL returns the SQL to list all changefeeds.
func (o *ExtendedOps) BuildListChangefeedsSQL() string {
	return "SHOW CHANGEFEEDS"
}

// --- Geospatial ---

// STDistance computes the Haversine distance between two geographic points.
// Returns distance in meters.
func (o *ExtendedOps) STDistance(p1Lon, p1Lat, p2Lon, p2Lat float64) (float64, error) {
	sql := o.BuildSTDistanceSQL(p1Lon, p1Lat, p2Lon, p2Lat)
	row, err := o.client.QueryOne(sql)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, nil
	}
	if val, ok := row["distance"]; ok && val != nil {
		if f, ok := val.(float64); ok {
			return f, nil
		}
	}
	return 0, nil
}

// BuildSTDistanceSQL returns the SQL for computing distance between two points.
func (o *ExtendedOps) BuildSTDistanceSQL(p1Lon, p1Lat, p2Lon, p2Lat float64) string {
	return fmt.Sprintf(
		"SELECT ST_DISTANCE(ST_POINT(%g, %g), ST_POINT(%g, %g)) AS distance",
		p1Lon, p1Lat, p2Lon, p2Lat,
	)
}

// STWithin tests whether a point falls within a polygon.
func (o *ExtendedOps) STWithin(pointLon, pointLat float64, polygonWKT string) (bool, error) {
	sql := o.BuildSTWithinSQL(pointLon, pointLat, polygonWKT)
	row, err := o.client.QueryOne(sql)
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, nil
	}
	if val, ok := row["result"]; ok && val != nil {
		return parseBool(val), nil
	}
	return false, nil
}

// BuildSTWithinSQL returns the SQL for a point-in-polygon test.
func (o *ExtendedOps) BuildSTWithinSQL(pointLon, pointLat float64, polygonWKT string) string {
	return fmt.Sprintf(
		"SELECT ST_WITHIN(ST_POINT(%g, %g), ST_GEOMFROMTEXT('%s')) AS result",
		pointLon, pointLat, escapeLiteral(polygonWKT),
	)
}

// --- Time-Series ---

// CreateRetentionPolicy creates a data retention policy on a time-series table.
func (o *ExtendedOps) CreateRetentionPolicy(name string, table string, column string, retention string) (int64, error) {
	return o.client.Execute(o.BuildCreateRetentionPolicySQL(name, table, column, retention))
}

// BuildCreateRetentionPolicySQL returns the DDL for a retention policy.
func (o *ExtendedOps) BuildCreateRetentionPolicySQL(name string, table string, column string, retention string) string {
	return "CREATE RETENTION POLICY " + quoteIdent(name) + " ON " + quoteIdent(table) +
		" USING (" + quoteIdent(column) + ") RETAIN FOR '" + escapeLiteral(retention) + "'"
}

// CreateContinuousAggregate creates a continuous aggregate for time-series data.
func (o *ExtendedOps) CreateContinuousAggregate(name string, query string, interval string) (int64, error) {
	return o.client.Execute(o.BuildCreateContinuousAggregateSQL(name, query, interval))
}

// BuildCreateContinuousAggregateSQL returns the DDL for a continuous aggregate.
func (o *ExtendedOps) BuildCreateContinuousAggregateSQL(name string, query string, interval string) string {
	return "CREATE CONTINUOUS AGGREGATE " + quoteIdent(name) +
		" WITH (refresh_interval = '" + escapeLiteral(interval) + "') AS " + query
}

// --- Multi-Tenant / Namespaces --- NOT IMPLEMENTED SERVER-SIDE ---
//
// DEPRECATED 2026-09-03. ParticleDB has no server-side namespace surface.
// CREATE NAMESPACE, SET NAMESPACE and DROP NAMESPACE are refused with a parse
// error on every wire (PG, native and MySQL), and the tenancy model that would
// give them a meaning is still an open decision -- all three candidate models
// leave these verbs refused. The methods below used to emit that SQL and
// document a row count on success, an outcome that could not occur. They now
// return ErrNamespaceUnsupported without contacting the server, and are kept
// only so existing code keeps compiling; they will be removed in the next
// major version.
//
// For tenant isolation today: a table-name prefix, a tenant column filtered on
// every query, or a separate server per tenant (its own --data-dir and ports).
// See https://particledb.ai/getting-started/databases-and-schemas/

// CreateNamespace always fails: CREATE NAMESPACE is not implemented by any
// ParticleDB server. It returns an error wrapping ErrNamespaceUnsupported
// without contacting the server.
//
// Deprecated: this never worked. See the section note above.
func (o *ExtendedOps) CreateNamespace(name string, quotas map[string]interface{}) (int64, error) {
	return 0, namespaceUnsupported("CREATE NAMESPACE")
}

// BuildCreateNamespaceSQL returns the DDL for creating a namespace.
//
// Deprecated: the DDL this returns is refused by every ParticleDB server.
// Retained as a pure function so existing code keeps compiling.
func (o *ExtendedOps) BuildCreateNamespaceSQL(name string, quotas map[string]interface{}) string {
	if len(quotas) == 0 {
		return "CREATE NAMESPACE " + quoteIdent(name)
	}
	parts := make([]string, 0, len(quotas))
	for k, v := range quotas {
		parts = append(parts, fmt.Sprintf("%s = %v", k, v))
	}
	return "CREATE NAMESPACE " + quoteIdent(name) + " WITH (" + strings.Join(parts, ", ") + ")"
}

// SetNamespace always fails: SET NAMESPACE is not implemented by any
// ParticleDB server. It returns an error wrapping ErrNamespaceUnsupported
// without contacting the server.
//
// Deprecated: this never worked. See the section note above.
func (o *ExtendedOps) SetNamespace(name string) (int64, error) {
	return 0, namespaceUnsupported("SET NAMESPACE")
}

// BuildSetNamespaceSQL returns the SQL to set the active namespace.
//
// Deprecated: the SQL this returns is refused by every ParticleDB server.
// Retained as a pure function so existing code keeps compiling.
func (o *ExtendedOps) BuildSetNamespaceSQL(name string) string {
	return "SET NAMESPACE " + quoteIdent(name)
}

// DropNamespace always fails: DROP NAMESPACE is not implemented by any
// ParticleDB server. It returns an error wrapping ErrNamespaceUnsupported
// without contacting the server.
//
// Deprecated: this never worked. See the section note above.
func (o *ExtendedOps) DropNamespace(name string) (int64, error) {
	return 0, namespaceUnsupported("DROP NAMESPACE")
}

// BuildDropNamespaceSQL returns the DDL to drop a namespace.
//
// Deprecated: the DDL this returns is refused by every ParticleDB server.
// Retained as a pure function so existing code keeps compiling.
func (o *ExtendedOps) BuildDropNamespaceSQL(name string) string {
	return "DROP NAMESPACE IF EXISTS " + quoteIdent(name)
}

// --- Lakehouse ---

// ReadParquet queries a Parquet file directly, returning the result rows.
func (o *ExtendedOps) ReadParquet(path string) (*QueryResult, error) {
	return o.client.Query(o.BuildReadParquetSQL(path))
}

// BuildReadParquetSQL returns the SQL to query a Parquet file.
func (o *ExtendedOps) BuildReadParquetSQL(path string) string {
	return "SELECT * FROM read_parquet('" + escapeLiteral(path) + "')"
}

// ReadCSV queries a CSV file directly, returning the result rows.
func (o *ExtendedOps) ReadCSV(path string) (*QueryResult, error) {
	return o.client.Query(o.BuildReadCSVSQL(path))
}

// BuildReadCSVSQL returns the SQL to query a CSV file.
func (o *ExtendedOps) BuildReadCSVSQL(path string) string {
	return "SELECT * FROM read_csv('" + escapeLiteral(path) + "')"
}
