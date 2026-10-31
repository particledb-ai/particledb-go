package particledb

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DefaultPort is the server's default database port (native wire, HTTP, gRPC
// and the PostgreSQL wire share it).
const DefaultPort = 7173

// DefaultUnixSocketDir is the server's default --unix-socket-dir.
const DefaultUnixSocketDir = "/tmp"

// Config describes how to open a connection. Build one with ParseConfig and
// adjust fields before passing it to ConnectConfig, NewPoolConfig or
// NewConnector. The zero value is not usable; ParseConfig("") gives the
// defaults (localhost:7173, no TLS).
type Config struct {
	Host     string
	Port     int
	User     string
	Password string `json:"-"`
	// Database is recorded for the application; the server serves one
	// database per instance and does not switch on it.
	Database string

	// TLS, when non-nil, encrypts the connection. ParseConfig sets it from
	// sslmode (require, verify-ca, verify-full); nil means plaintext.
	TLS *tls.Config
	// TLSUpgrade selects the in-band upgrade (sslnegotiation=upgrade) for a
	// dedicated native-wire listener; the default is direct TLS with ALPN
	// "pdb" on the database port.
	TLSUpgrade bool

	// ConnectTimeout bounds dialing, TLS and the handshake. 0 = 10 s.
	ConnectTimeout time.Duration

	// PreferUnixSocket (default true) connects to a loopback host through the
	// server's Unix socket <UnixSocketDir>/.s.PDB.<Port> when it exists.
	PreferUnixSocket bool
	UnixSocketDir    string
	// UnixSocketPath, when set, is used and must work.
	UnixSocketPath string

	// RuntimeParams are session settings applied at connect (for example
	// "TimeZone"). They travel in the HELLO's session parameter block; a
	// server that does not apply them gets a SET after connect.
	RuntimeParams map[string]string

	// StatementCacheCapacity is the number of server-side prepared statements
	// each connection keeps for parameterized queries. 0 = 256, -1 disables
	// the cache (every parameterized query prepares and closes its handle).
	StatementCacheCapacity int

	// CancelGrace is how long a cancelled statement may take to answer its
	// stream after the client reset it. Past it the connection is closed,
	// because the statement's outcome is unknown. 0 = 5 s.
	CancelGrace time.Duration

	// OnNotice, when set, receives every advisory the server attaches to a
	// statement (PostgreSQL's NoticeResponse; for example the notice that a
	// CREATE INDEX ... USING gin is kept as a btree).
	OnNotice func(*Conn, Notice)

	// DialFunc replaces net.Dialer.DialContext.
	DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	// Pool settings (NewPool / NewPoolConfig only).
	MaxConns          int           // 0 = max(4, GOMAXPROCS)
	MinConns          int           // connections kept open
	MaxConnLifetime   time.Duration // 0 = 1 h
	MaxConnIdleTime   time.Duration // 0 = 30 min
	HealthCheckPeriod time.Duration // 0 = 1 min
	// SharedStreamsPerConn is how many concurrent statements the pool puts
	// on one shared connection before it opens another (up to MaxConns).
	// A multiplexed connection serves several statements at once, and a few
	// busy sockets outperform many idle ones. 0 = 8.
	SharedStreamsPerConn int
}

func (c *Config) sharedStreams() int {
	if c.SharedStreamsPerConn > 0 {
		return c.SharedStreamsPerConn
	}
	return 8
}

// ParseConfig parses a connection string. Two forms are accepted:
//
//	particledb://user:password@host:7173/db?sslmode=verify-full&sslrootcert=ca.pem
//	host=localhost port=7173 user=app sslmode=disable
//
// The scheme pdb:// is a synonym. Recognised parameters: sslmode
// (disable | require | verify-ca | verify-full), sslrootcert, sslcert, sslkey,
// sslnegotiation (direct | upgrade), connect_timeout (seconds),
// prefer_unix_socket, unix_socket_dir, unix_socket_path, timezone,
// statement_cache_capacity, cancel_grace (Go duration), pool_max_conns,
// pool_min_conns, pool_max_conn_lifetime, pool_max_conn_idle_time,
// pool_health_check_period. Any other key is applied as a session setting.
func ParseConfig(dsn string) (*Config, error) {
	c := &Config{Host: "localhost", Port: DefaultPort, PreferUnixSocket: true, UnixSocketDir: DefaultUnixSocketDir}
	params := map[string]string{}
	dsn = strings.TrimSpace(dsn)
	switch {
	case strings.HasPrefix(dsn, "particledb://"), strings.HasPrefix(dsn, "pdb://"):
		u, err := url.Parse(dsn)
		if err != nil {
			return nil, errors.New("particledb: invalid connection URL")
		}
		if h := u.Hostname(); h != "" {
			c.Host = h
		}
		if p := u.Port(); p != "" {
			n, err := strconv.Atoi(p)
			if err != nil || n <= 0 || n > 65535 {
				return nil, errors.New("particledb: invalid port (must be an integer between 1 and 65535)")
			}
			c.Port = n
		}
		if u.User != nil {
			c.User = u.User.Username()
			c.Password, _ = u.User.Password()
		}
		c.Database = strings.TrimPrefix(u.Path, "/")
		for k, v := range u.Query() {
			if len(v) > 0 {
				params[k] = v[len(v)-1]
			}
		}
	case dsn == "" || strings.Contains(dsn, "="):
		kv, err := parseKeyValues(dsn)
		if err != nil {
			return nil, err
		}
		for k, v := range kv {
			switch k {
			case "host":
				c.Host = v
			case "port":
				n, err := strconv.Atoi(v)
				if err != nil || n <= 0 || n > 65535 {
					return nil, errors.New("particledb: invalid port (must be an integer between 1 and 65535)")
				}
				c.Port = n
			case "user":
				c.User = v
			case "password":
				c.Password = v
			case "dbname", "database":
				c.Database = v
			default:
				params[k] = v
			}
		}
	default:
		return nil, fmt.Errorf("particledb: unrecognised connection string (want particledb://... or key=value pairs)")
	}
	if err := c.applyParams(params); err != nil {
		return nil, err
	}
	return c, nil
}

func parseKeyValues(s string) (map[string]string, error) {
	out := map[string]string{}
	for len(s) > 0 {
		s = strings.TrimLeft(s, " \t\n")
		if s == "" {
			break
		}
		eq := strings.IndexByte(s, '=')
		if eq <= 0 {
			return nil, errors.New("particledb: invalid key=value connection string")
		}
		key := strings.TrimSpace(s[:eq])
		s = strings.TrimLeft(s[eq+1:], " \t")
		var val strings.Builder
		if strings.HasPrefix(s, "'") {
			s = s[1:]
			closed := false
			for i := 0; i < len(s); i++ {
				if s[i] == '\\' && i+1 < len(s) {
					val.WriteByte(s[i+1])
					i++
					continue
				}
				if s[i] == '\'' {
					s = s[i+1:]
					closed = true
					break
				}
				val.WriteByte(s[i])
			}
			if !closed {
				return nil, errors.New("particledb: unterminated quoted connection value")
			}
		} else {
			end := strings.IndexAny(s, " \t\n")
			if end < 0 {
				end = len(s)
			}
			val.WriteString(s[:end])
			s = s[end:]
		}
		out[key] = val.String()
	}
	return out, nil
}

func (c *Config) applyParams(p map[string]string) error {
	sslmode := ""
	var rootCert, cert, key string
	for k, v := range p {
		var err error
		switch strings.ToLower(k) {
		case "user":
			c.User = v
		case "password":
			c.Password = v
		case "dbname", "database":
			c.Database = v
		case "sslmode":
			sslmode = strings.ToLower(v)
		case "sslrootcert":
			rootCert = v
		case "sslcert":
			cert = v
		case "sslkey":
			key = v
		case "sslnegotiation":
			switch strings.ToLower(v) {
			case "direct", "":
			case "upgrade":
				c.TLSUpgrade = true
			default:
				return fmt.Errorf("particledb: unknown sslnegotiation %q (direct | upgrade)", v)
			}
		case "connect_timeout":
			var n int
			n, err = strconv.Atoi(v)
			c.ConnectTimeout = time.Duration(n) * time.Second
		case "prefer_unix_socket", "preferunixsocket":
			c.PreferUnixSocket, err = strconv.ParseBool(v)
		case "unix_socket_dir":
			c.UnixSocketDir = v
		case "unix_socket_path":
			c.UnixSocketPath = v
		case "timezone":
			c.setRuntime("TimeZone", v)
		case "statement_cache_capacity":
			c.StatementCacheCapacity, err = strconv.Atoi(v)
		case "cancel_grace":
			c.CancelGrace, err = time.ParseDuration(v)
		case "pool_max_conns":
			c.MaxConns, err = strconv.Atoi(v)
		case "pool_min_conns":
			c.MinConns, err = strconv.Atoi(v)
		case "pool_max_conn_lifetime":
			c.MaxConnLifetime, err = time.ParseDuration(v)
		case "pool_max_conn_idle_time":
			c.MaxConnIdleTime, err = time.ParseDuration(v)
		case "pool_health_check_period":
			c.HealthCheckPeriod, err = time.ParseDuration(v)
		case "pool_shared_streams_per_conn":
			c.SharedStreamsPerConn, err = strconv.Atoi(v)
		default:
			c.setRuntime(k, v)
		}
		if err != nil {
			return fmt.Errorf("particledb: bad value %q for %s: %w", v, k, err)
		}
	}
	t, err := buildTLS(sslmode, c.Host, rootCert, cert, key)
	if err != nil {
		return err
	}
	c.TLS = t
	return nil
}

func (c *Config) setRuntime(k, v string) {
	if c.RuntimeParams == nil {
		c.RuntimeParams = map[string]string{}
	}
	c.RuntimeParams[k] = v
}

// buildTLS turns libpq-style sslmode settings into a *tls.Config (nil for
// disable). require with a root certificate behaves as verify-ca, as in
// libpq.
func buildTLS(mode, host, rootCert, certFile, keyFile string) (*tls.Config, error) {
	switch mode {
	case "", "disable":
		return nil, nil
	case "require", "verify-ca", "verify-full":
	case "allow", "prefer":
		return nil, fmt.Errorf("particledb: sslmode=%s is not supported: it could silently fall back to plaintext; use disable or require", mode)
	default:
		return nil, fmt.Errorf("particledb: unknown sslmode %q (disable | require | verify-ca | verify-full)", mode)
	}
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("particledb: sslcert and sslkey must be set together")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	var roots *x509.CertPool
	if rootCert != "" {
		pem, err := os.ReadFile(rootCert)
		if err != nil {
			return nil, fmt.Errorf("particledb: sslrootcert: %w", err)
		}
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("particledb: sslrootcert %s holds no PEM certificate", rootCert)
		}
	}
	if certFile != "" {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("particledb: client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	if mode == "require" && roots != nil {
		mode = "verify-ca"
	}
	switch mode {
	case "require":
		cfg.InsecureSkipVerify = true
	case "verify-ca":
		if roots == nil {
			return nil, errors.New("particledb: sslmode=verify-ca needs sslrootcert (the CA that signed the server certificate)")
		}
		// Chain verification without the host name check.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server sent no certificate")
			}
			opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, ic := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(ic)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	case "verify-full":
		cfg.RootCAs = roots // nil = the system pool
		cfg.ServerName = host
	}
	return cfg, nil
}

// Copy returns a deep copy of c.
func (c *Config) Copy() *Config {
	n := *c
	if c.TLS != nil {
		n.TLS = c.TLS.Clone()
	}
	if c.RuntimeParams != nil {
		n.RuntimeParams = make(map[string]string, len(c.RuntimeParams))
		for k, v := range c.RuntimeParams {
			n.RuntimeParams[k] = v
		}
	}
	return &n
}

func (c *Config) connectTimeout() time.Duration {
	if c.ConnectTimeout > 0 {
		return c.ConnectTimeout
	}
	return 10 * time.Second
}

func (c *Config) cancelGrace() time.Duration {
	if c.CancelGrace > 0 {
		return c.CancelGrace
	}
	return 5 * time.Second
}

func (c *Config) stmtCacheCap() int {
	switch {
	case c.StatementCacheCapacity < 0:
		return 0
	case c.StatementCacheCapacity == 0:
		return 256
	}
	return c.StatementCacheCapacity
}

// isLoopback reports whether host is localhost or a loopback literal.
func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	return ip != nil && ip.IsLoopback()
}

// unixSocket returns the Unix socket path to try first, or "".
func (c *Config) unixSocket() (path string, explicit bool) {
	if c.TLS != nil {
		return "", false
	}
	if c.UnixSocketPath != "" {
		return c.UnixSocketPath, true
	}
	if !c.PreferUnixSocket || !isLoopback(c.Host) {
		return "", false
	}
	dir := c.UnixSocketDir
	if dir == "" {
		dir = DefaultUnixSocketDir
	}
	return filepath.Join(dir, fmt.Sprintf(".s.PDB.%d", c.Port)), false
}

// String omits credentials when a configuration is logged.
func (c Config) String() string {
	return fmt.Sprintf("ParticleDB{host:%q port:%d user:%q database:%q password:[redacted]}", c.Host, c.Port, c.User, c.Database)
}

// GoString also redacts credentials in Go-syntax formatting.
func (c Config) GoString() string { return c.String() }

// MarshalJSON emits connection identity without credentials, keys or callbacks.
func (c Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Host     string `json:"host"`
		Port     int    `json:"port"`
		User     string `json:"user"`
		Database string `json:"database"`
		TLS      bool   `json:"tls"`
	}{c.Host, c.Port, c.User, c.Database, c.TLS != nil})
}
