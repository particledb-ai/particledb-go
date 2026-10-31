package legacy

import (
	"fmt"
	"strings"
)

// KVOps provides Redis-compatible key-value operations backed by ParticleDB's
// KV SQL functions (KV_SET, KV_GET, KV_DEL, KV_MSET, KV_MGET, etc.).
type KVOps struct {
	client *Client
}

// Set stores a key-value pair.
func (k *KVOps) Set(key, value string) error {
	sql := k.BuildSetSQL()
	_, err := k.client.QueryOne(sql, key, value)
	return err
}

// SetWithTTL stores a key-value pair with a time-to-live in seconds.
func (k *KVOps) SetWithTTL(key, value string, ttlSeconds int) error {
	if err := k.Set(key, value); err != nil {
		return err
	}
	expireSQL := k.BuildExpireSQL()
	_, err := k.client.QueryOne(expireSQL, key, ttlSeconds)
	return err
}

// Get retrieves the value for a key. Returns empty string and no error if the
// key does not exist.
func (k *KVOps) Get(key string) (string, error) {
	sql := k.BuildGetSQL()
	row, err := k.client.QueryOne(sql, key)
	if err != nil {
		return "", err
	}
	if row == nil {
		return "", nil
	}
	val, ok := row["value"]
	if !ok || val == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", val), nil
}

// Delete removes a key. KV_DEL is a void function that returns no rows,
// so this method simply executes the command and returns any error.
func (k *KVOps) Delete(key string) error {
	sql := k.BuildDeleteSQL()
	_, err := k.client.QueryOne(sql, key)
	// KV_DEL returns no rows - a nil-row result is expected, not an error.
	return err
}

// MSet sets multiple key-value pairs at once.
func (k *KVOps) MSet(mapping map[string]string) error {
	if len(mapping) == 0 {
		return nil
	}
	sql, args := k.BuildMSetSQL(mapping)
	_, err := k.client.QueryOne(sql, args...)
	return err
}

// MGet retrieves multiple values at once. Keys that do not exist will have a
// nil pointer in the returned map.
func (k *KVOps) MGet(keys []string) (map[string]*string, error) {
	if len(keys) == 0 {
		return make(map[string]*string), nil
	}
	sql, args := k.BuildMGetSQL(keys)
	row, err := k.client.QueryOne(sql, args...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*string, len(keys))
	for _, key := range keys {
		result[key] = nil
	}
	if row != nil {
		for i, key := range keys {
			if val, ok := row[fmt.Sprintf("v%d", i)]; ok && val != nil {
				s := fmt.Sprintf("%v", val)
				result[key] = &s
			}
		}
	}
	return result, nil
}

// Incr increments a key's integer value by the given amount and returns the
// new value.
func (k *KVOps) Incr(key string, amount int64) (int64, error) {
	sql := k.BuildIncrSQL()
	row, err := k.client.QueryOne(sql, key, amount)
	if err != nil {
		return 0, err
	}
	if row == nil {
		return 0, nil
	}
	val, ok := row["value"]
	if !ok || val == nil {
		return 0, nil
	}
	switch v := val.(type) {
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	case string:
		return parseStringToInt64(v), nil
	case []byte:
		return parseStringToInt64(string(v)), nil
	}
	return 0, nil
}

// Exists checks whether a key exists by attempting a KV_GET and checking
// whether a non-empty value is returned. KV_EXISTS is a void function that
// returns no rows, so we use KV_GET as the verification mechanism.
func (k *KVOps) Exists(key string) (bool, error) {
	val, err := k.Get(key)
	if err != nil {
		return false, err
	}
	return val != "", nil
}

// TTL returns the remaining time-to-live of a key in seconds. Returns -1 if
// the key has no TTL or does not exist.
func (k *KVOps) TTL(key string) (int, error) {
	sql := k.BuildTTLSQL()
	row, err := k.client.QueryOne(sql, key)
	if err != nil {
		return -1, err
	}
	if row == nil {
		return -1, nil
	}
	val, ok := row["ttl"]
	if !ok || val == nil {
		return -1, nil
	}
	if v, ok := val.(int64); ok {
		return int(v), nil
	}
	return -1, nil
}

// Keys returns all keys matching the given glob-style pattern.
// KV_KEYS returns a single row with comma-separated keys as a string.
func (k *KVOps) Keys(pattern string) ([]string, error) {
	sql := k.BuildKeysSQL()
	row, err := k.client.QueryOne(sql, pattern)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	val, ok := row["key"]
	if !ok || val == nil {
		return nil, nil
	}
	raw := fmt.Sprintf("%v", val)
	if raw == "" {
		return nil, nil
	}
	parts := strings.Split(raw, ",")
	var keys []string
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			keys = append(keys, trimmed)
		}
	}
	return keys, nil
}

// --- SQL builders (exported for testing) ---

// BuildSetSQL returns the SQL for KV_SET.
func (k *KVOps) BuildSetSQL() string {
	return "SELECT KV_SET($1, $2)"
}

// BuildExpireSQL returns the SQL for KV_EXPIRE.
func (k *KVOps) BuildExpireSQL() string {
	return "SELECT KV_EXPIRE($1, $2)"
}

// BuildGetSQL returns the SQL for KV_GET.
func (k *KVOps) BuildGetSQL() string {
	return "SELECT KV_GET($1) AS value"
}

// BuildDeleteSQL returns the SQL for KV_DEL.
func (k *KVOps) BuildDeleteSQL() string {
	return "SELECT KV_DEL($1) AS result"
}

// BuildMSetSQL returns the SQL and args for KV_MSET with the given mapping.
func (k *KVOps) BuildMSetSQL(mapping map[string]string) (string, []interface{}) {
	// Sort keys for deterministic output.
	keys := make([]string, 0, len(mapping))
	for key := range mapping {
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}

	args := make([]interface{}, 0, len(mapping)*2)
	placeholders := make([]string, 0, len(mapping)*2)
	idx := 1
	for _, key := range keys {
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, key)
		idx++
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx))
		args = append(args, mapping[key])
		idx++
	}
	sql := "SELECT KV_MSET(" + strings.Join(placeholders, ", ") + ")"
	return sql, args
}

// BuildMGetSQL returns the SQL and args for a multi-key get: one KV_GET
// column per key (v0, v1, ...). KV_MGET is not used because the server
// answers it with one comma-joined string ("a,(nil),b"), which cannot be
// split back into values that themselves contain a comma.
func (k *KVOps) BuildMGetSQL(keys []string) (string, []interface{}) {
	cols := make([]string, len(keys))
	args := make([]interface{}, len(keys))
	for i, key := range keys {
		cols[i] = fmt.Sprintf("KV_GET($%d) AS v%d", i+1, i)
		args[i] = key
	}
	sql := "SELECT " + strings.Join(cols, ", ")
	return sql, args
}

// BuildIncrSQL returns the SQL for KV_INCRBY.
func (k *KVOps) BuildIncrSQL() string {
	return "SELECT KV_INCRBY($1, $2) AS value"
}

// BuildExistsSQL returns the SQL for KV_EXISTS.
func (k *KVOps) BuildExistsSQL() string {
	return "SELECT KV_EXISTS($1) AS result"
}

// BuildTTLSQL returns the SQL for KV_TTL.
func (k *KVOps) BuildTTLSQL() string {
	return "SELECT KV_TTL($1) AS ttl"
}

// BuildKeysSQL returns the SQL for KV_KEYS.
func (k *KVOps) BuildKeysSQL() string {
	return "SELECT KV_KEYS($1) AS key"
}

// parseStringToInt64 parses a decimal string into int64 without importing strconv.
func parseStringToInt64(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	negative := false
	start := 0
	if s[0] == '-' {
		negative = true
		start = 1
	} else if s[0] == '+' {
		start = 1
	}
	var n int64
	for i := start; i < len(s); i++ {
		ch := s[i]
		if ch < '0' || ch > '9' {
			break
		}
		n = n*10 + int64(ch-'0')
	}
	if negative {
		return -n
	}
	return n
}
