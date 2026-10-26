// Package particledb provides a Go SDK for ParticleDB: OLTP, OLAP, vector
// search, KV operations, RAG, and AI functions. Client speaks the PDB native
// wire by default (PdbWireClient); the PostgreSQL wire through database/sql
// is an explicit opt-in (Config.Protocol = ProtocolPG, or ConnectPG).
package legacy

// Protocol selects the transport a Client speaks.
type Protocol string

const (
	// ProtocolNative is the PDB native wire (default port 5440): binary
	// framing, Arrow results, native prepared statements and EXECUTE_BATCH.
	// It is the default.
	ProtocolNative Protocol = "native"
	// ProtocolPG is the PostgreSQL wire through database/sql (default port
	// 5432), for legacy setups. It requires a registered "postgres"
	// database/sql driver: the SDK does not import one, so the application
	// must, e.g. import _ "github.com/lib/pq".
	ProtocolPG Protocol = "pg"
)

// DefaultPGPort is the default ParticleDB PostgreSQL-wire port.
const DefaultPGPort = 5432

// Config holds connection parameters for a ParticleDB instance.
type Config struct {
	// Host is the hostname or IP address. Defaults to "localhost".
	Host string
	// Port is the TCP port number. 0 means the protocol default:
	// 5440 (native) or 5432 (PG).
	Port int
	// User is the database user name. Defaults to "particledb".
	User string
	// Password is the user's password. Defaults to empty. On the native
	// wire a non-empty password is sent with MSG_AUTH on every connection.
	Password string
	// Database is the database name (PG wire only). Defaults to "particledb".
	Database string
	// PoolSize is the maximum number of open connections. Defaults to 10.
	PoolSize int
	// SSLMode controls whether SSL is used. Defaults to "disable". The native
	// wire client has no TLS: "require", "verify-ca" and "verify-full" are
	// refused by New rather than silently sent in plaintext.
	SSLMode string
	// Protocol selects the transport: ProtocolNative (default when empty)
	// or ProtocolPG (explicit opt-in).
	Protocol Protocol
	// DisableUnixSocket (native wire): never use the server's Unix socket;
	// by default a local Host (localhost, 127.x, ::1) connects over
	// <UnixSocketDir>/.s.PDB.<Port> when it exists, else TCP.
	// URL / DSN: unix_socket=off.
	DisableUnixSocket bool
	// UnixSocketDir (native wire) is the server's --unix-socket-dir.
	// Default "/tmp". URL / DSN: unix_socket_dir=<dir>.
	UnixSocketDir string
}

// DefaultConfig returns a Config for a local server over the native wire
// (localhost:5440).
func DefaultConfig() Config {
	return Config{
		Host:     "localhost",
		Port:     WireDefaultPort,
		User:     "particledb",
		Password: "",
		Database: "particledb",
		PoolSize: 10,
		SSLMode:  "disable",
		Protocol: ProtocolNative,
	}
}

// DefaultPGConfig returns a Config for a local server over the PostgreSQL
// wire (localhost:5432). The application must import a "postgres"
// database/sql driver.
func DefaultPGConfig() Config {
	c := DefaultConfig()
	c.Port = DefaultPGPort
	c.Protocol = ProtocolPG
	return c
}

// DSN returns a PostgreSQL-compatible connection string built from this
// Config (used only with ProtocolPG). A zero Port renders as 5432.
func (c Config) DSN() string {
	port := c.Port
	if port == 0 {
		port = DefaultPGPort
	}
	dsn := "host=" + c.Host
	dsn += " port=" + itoa(port)
	dsn += " user=" + c.User
	if c.Password != "" {
		dsn += " password=" + c.Password
	}
	dsn += " dbname=" + c.Database
	dsn += " sslmode=" + c.SSLMode
	return dsn
}

// QueryResult holds the result of a query execution.
type QueryResult struct {
	// Columns contains the column names in the result set.
	Columns []string
	// Rows contains each row as a map from column name to value.
	Rows []map[string]interface{}
	// RowCount is the number of rows returned.
	RowCount int
}

// VectorSearchResult holds a single result from a vector similarity search.
type VectorSearchResult struct {
	// Row contains the full row data as a map from column name to value.
	Row map[string]interface{}
	// Distance is the computed distance between the query vector and this row's vector.
	Distance float64
}

// VectorSearchOptions configures a vector similarity search.
type VectorSearchOptions struct {
	// Filter is an optional SQL WHERE clause (without the WHERE keyword).
	Filter string
	// K is the number of nearest neighbors to return. Defaults to 10 if zero.
	K int
	// Columns lists the column names to include in the SELECT clause.
	// If empty, defaults to "id".
	Columns []string
}

// PipelineConfig describes a named RAG pipeline configuration.
type PipelineConfig struct {
	// Name is the pipeline name.
	Name string
	// Table is the source table name.
	Table string
	// VectorCol is the column containing vectors.
	VectorCol string
	// TextCol is the column containing text.
	TextCol string
	// K is the number of results to retrieve. Defaults to 5.
	K int
	// Mode is the search mode: "hybrid", "vector", or "text".
	Mode string
}

// itoa converts an integer to its decimal string representation without
// importing strconv (keeps dependencies minimal for this internal helper).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	negative := false
	if n < 0 {
		negative = true
		n = -n
	}
	// max int digits is 20
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

// i64toa converts an int64 to its decimal string representation.
func i64toa(n int64) string {
	if n == 0 {
		return "0"
	}
	negative := false
	if n < 0 {
		negative = true
		n = -n
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if negative {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
