## Project: High-Performance In-Memory Cache in the Redis Style Written in Go

### Goals
- Concurrent in-memory key-value store with Redis-like semantics (`GET`, `SET`, `DEL`, `EXPIRE`, `INCR`).
- Access via a console REPL and via an HTTP/JSON API.
- TTL support on keys, atomic operations, lazy and active cleanup.
- Horizontal scalability through consistent-hash sharding (planned).

### Architecture
```
cmd/
  cachectl/        # CLI client
  cached/          # HTTP + TCP server
internal/
  store/           # storage core (data structures, TTL, eviction)
  protocol/        # codecs for text (Redis RESP-lite) and JSON protocols
pkg/
  api/             # HTTP handlers, DTOs, validation
```

### Storage (`internal/store`)
- The primary `Store` structure contains:
  - `shards []*bucket` - an array of segments, each with its own `sync.RWMutex` to reduce lock contention.
  - `clock Clock` - time abstraction (for tests).
  - `evictCh chan struct{}` - signal for the background cleaner worker.
- Keys are stored in `map[string]*entry`, where `entry` = {value []byte, expireAt time.Time, flags EntryFlags}.
- TTL:
  - `GET` performs lazy expiration (if `expireAt` < now - delete and return miss).
  - Background `cleaner` scans N random keys in each shard every `cleanInterval`.
- Additional operations:
  - `IncrBy(key, delta)` - uses `big.Int` or `int64`, stored as a byte slice and parsed on the fly.
  - `CompareAndSwap(key, expect, update)` for future atomic structures.

### Protocols and Interfaces
#### Console REPL (`cmd/cachectl`)
- Connects to `cached` via TCP and speaks RESP-lite:
  - `SET key value [EX seconds]`
  - `GET key`
  - `DEL key1 key2 ...`
  - `INCR key`
  - `PING`
- Client capabilities: script mode (`cachectl < script.txt`), interactive mode with syntax highlighting (via `github.com/peterh/liner`).

#### HTTP API (`cmd/cached/http`)
- REST-like over JSON.
- Endpoints:
  - `PUT /kv/{key}` body: `{ "value":"base64", "ttl_ms":1234? }`
  - `GET /kv/{key}` -> `{ "value":"base64", "ttl_ms":500 }` (ttl = remaining time, `null` if perpetual)
  - `DELETE /kv/{key}`
  - `POST /atomic/incr` body: `{ "key":"foo", "delta":1 }`
  - `POST /ops/mset` body: `{ "pairs":[{"key":"k","value":"base64","ttl_ms":0}] }`
- Validation via `github.com/go-chi/chi` + `net/http`. JSON -> base64 for binary values.

### Server (`cmd/cached`)
- Consists of:
  - `tcpServer` (RESP-lite) - uses `bufio.Reader/Writer`, supports pipelining, enforces command size limits.
  - `httpServer` - `chi.Router`, Prometheus metrics (`/metrics`), profiler `/debug/pprof`.
  - `adminServer` - optional gRPC for shard metrics.
- Configuration via `yaml` or flags:
  - `--shards=64`, `--max-memory=2GB`, `--clean-interval=250ms`, `--tcp-addr=:6380`, `--http-addr=:8080`.
- Memory limits: approximate `maxMemory` controller counts total value + metadata size; exceeding the limit triggers LRU/TTL eviction.

### Sharding and Scaling
- Locally `Store` shards by `fnv64(key) % shardCount`.
- For distributed mode:
  - External `cluster.Router` uses a consistent ring (e.g., `memberlist` + `hashicorp/serf`).
  - CLI and HTTP proxy/redirect to the correct node.

### Testing
- Unit tests: `store` covered via `testing` + `t.Parallel`, mock clock.
- Integration: launch the server (tcp+http) via `testcontainers-go`, run scenarios (SET/GET/TTL/INCR).
- Benchmarks: `BenchmarkStoreSet`, `BenchmarkStoreGetParallel`, `BenchmarkRESPRoundTrip`.

### Implementation Steps
1. `internal/store`: shard structure, basic ops, TTL.
2. RESP-lite server.
3. CLI client.
4. HTTP API and observability (logging, metrics).
5. LRU eviction + max-memory.
6. Distributed mode (if needed).

### CLI Example
```bash
$ cached --tcp-addr=:6380 --http-addr=:8080 --shards=32 --max-memory=512MB
$ cachectl -a 127.0.0.1:6380
cache> SET foo bar EX 5
OK
cache> GET foo
"bar"
cache> INCR counter
(integer) 1
```

### HTTP Example
```bash
curl -X PUT localhost:8080/kv/foo -d '{"value":"YmFy","ttl_ms":5000}'
curl localhost:8080/kv/foo
```

### Run and Test
- Build/run server: `GOEXE=~/local/go/bin/go` (or system Go)
```
  go run ./cmd/cached --http-addr=:8080 --tcp-addr=:6380 --shards=64
```
- Console client:
```
go run ./cmd/cachectl -a 127.0.0.1:6380
```
- Tests (core + HTTP):
```
go test ./...
```

