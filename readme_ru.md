## Проект: быстродействующий in-memory кеш в стиле Redis на Go

### Цели
- Конкурентный in-memory KV store с семантикой Redis-подобных команд (`GET`, `SET`, `DEL`, `EXPIRE`, `INCR`).
- Доступ через консольный REPL и через HTTP/JSON API.
- Поддержка TTL на ключах, атомарных операций, ленивой и активной очистки.
- Горизонтальное масштабирование через шардирование по консистентному хешу (в будущем).

### Архитектура
```
cmd/
  cachectl/        # CLI клиент
  cached/          # HTTP + TCP сервер
internal/
  store/           # ядро хранилища (структуры, TTL, eviction)
  protocol/        # кодеки текстового (Redis RESP-lite) и JSON протоколов
pkg/
  api/             # HTTP-handlers, DTO, валидация
```

### Хранилище (`internal/store`)
- Основная структура `Store` содержит:
  - `shards []*bucket` — массив сегментов, каждый со своим `sync.RWMutex` для уменьшения lock contention.
  - `clock Clock` — абстракция времени (для тестов).
  - `evictCh chan struct{}` — сигнал для фонового воркера очистки.
- Ключи хранятся в `map[string]*entry`, где `entry` = {value []byte, expireAt time.Time, flags EntryFlags}.
- TTL:
  - При `GET` выполняется ленивое истечение (если `expireAt` < now — удаляем и возвращаем miss).
  - Фоновый `cleaner` каждые `cleanInterval` сканирует N случайных ключей в каждом сегменте.
- Дополнительные операции:
  - `IncrBy(key, delta)` — использует `big.Int` или `int64`, хранится как байтовый слайс, парсится на лету.
  - `CompareAndSwap(key, expect, update)` для будущих атомарных структур.

### Протоколы и интерфейсы
#### Консольный REPL (`cmd/cachectl`)
- Подключается по TCP к `cached`, говорит RESP-lite:
  - `SET key value [EX seconds]`
  - `GET key`
  - `DEL key1 key2 ...`
  - `INCR key`
  - `PING`
- Клиент умеет: режим скриптов (`cachectl < script.txt`), интерактивный режим с подсветкой (использовать `github.com/peterh/liner`).

#### HTTP API (`cmd/cached/http`)
- REST-like поверх JSON.
- Эндпоинты:
  - `PUT /kv/{key}` body: `{ "value":"base64", "ttl_ms":1234? }`
  - `GET /kv/{key}` → `{ "value":"base64", "ttl_ms":500 }` (ttl = оставшееся время, `null` если бессрочно)
  - `DELETE /kv/{key}`
  - `POST /atomic/incr` body: `{ "key":"foo", "delta":1 }`
  - `POST /ops/mset` body: `{ "pairs":[{"key":"k","value":"base64","ttl_ms":0}] }`
- Валидация через `github.com/go-chi/chi` + `net/http`. JSON → base64 для бинарных значений.

### Сервер (`cmd/cached`)
- Состоит из:
  - `tcpServer` (RESP-lite) — использует `bufio.Reader/Writer`, поддерживает пайплайнинг, ограничение размера команд.
  - `httpServer` — `chi.Router`, метрики Prometheus (`/metrics`), профилировщик `/debug/pprof`.
  - `adminServer` — опциональный gRPC для метрики шардов.
- Конфиг через `yaml` или флаги:
  - `--shards=64`, `--max-memory=2GB`, `--clean-interval=250ms`, `--tcp-addr=:6380`, `--http-addr=:8080`.
- Ограничение памяти: приблизительный `maxMemory` контроллер, подсчитываем суммарный размер значений и метаданных; при превышении запускаем LRU/TTL eviction.

### Шардирование и масштабирование
- Локально `Store` шардирован по `fnv64(key) % shardCount`.
- Для распределенного режима:
  - Внешний слой `cluster.Router` использует консистентное кольцо (например, `memberlist` + `hashicorp/serf`).
  - CLI и HTTP проксируют на правильный узел (редирект или проксирование).

### Тестирование
- Юнит-тесты: `store` покрывается через `testing` + `t.Parallel`, mock clock.
- Интеграционные: поднять сервер (tcp+http) через `testcontainers-go`, прогнать сценарии (SET/GET/TTL/INCR).
- Бенчмарки: `BenchmarkStoreSet`, `BenchmarkStoreGetParallel`, `BenchmarkRESPRoundTrip`.

### Пошаговая реализация
1. `internal/store`: структура сегментов, базовые операции, TTL.
2. RESP-lite сервер.
3. CLI клиент.
4. HTTP API и observability (логирование, метрики).
5. LRU eviction + max-memory.
6. Распределенный режим (при необходимости).

### Пример использования CLI
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

### Пример HTTP
```bash
curl -X PUT localhost:8080/kv/foo -d '{"value":"YmFy","ttl_ms":5000}'
curl localhost:8080/kv/foo
```

### Запуск и тесты
- Сборка/запуск сервера: `GOEXE=~/local/go/bin/go` (или системный Go)  
```
  go run ./cmd/cached --http-addr=:8080 --tcp-addr=:6380 --shards=64
```
- Консольный клиент: 
```
go run ./cmd/cachectl -a 127.0.0.1:6380
```
- Тесты (ядро + HTTP): 
```
go test ./...
```
