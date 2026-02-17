package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Clock abstracts time to simplify TTL tests.
type Clock interface {
	Now() time.Time
}

// RealClock implements Clock via time.Now.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

var (
	errNotInteger = errors.New("value is not an integer")
)

// StoreOptions configures Store behaviour.
type StoreOptions struct {
	Shards        int
	CleanInterval time.Duration
	Clock         Clock
}

// Store is a sharded in-memory key-value database with TTL support.
type Store struct {
	buckets       []*bucket
	cleanInterval time.Duration
	clock         Clock
	stopOnce      sync.Once
	stopCh        chan struct{}
	items         atomic.Int64
}

type bucket struct {
	sync.RWMutex
	entries map[string]*entry
}

type entry struct {
	value    []byte
	expireAt time.Time
}

// NewStore constructs a Store with the provided options.
func NewStore(opts StoreOptions) *Store {
	shards := opts.Shards
	if shards <= 0 {
		shards = 64
	}
	interval := opts.CleanInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	clk := opts.Clock
	if clk == nil {
		clk = RealClock{}
	}
	buckets := make([]*bucket, shards)
	for i := range buckets {
		buckets[i] = &bucket{entries: make(map[string]*entry)}
	}
	s := &Store{
		buckets:       buckets,
		cleanInterval: interval,
		clock:         clk,
		stopCh:        make(chan struct{}),
	}
	go s.cleaner()
	return s
}

// Close stops the background cleaner.
func (s *Store) Close() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *Store) bucketFor(key string) *bucket {
	h := fnv64(key)
	return s.buckets[int(h%uint64(len(s.buckets)))]
}

// Set assigns value to key with optional ttl. ttl <= 0 means no expiration.
func (s *Store) Set(key string, value []byte, ttl time.Duration) {
	b := s.bucketFor(key)
	b.Lock()
	defer b.Unlock()
	expires := time.Time{}
	if ttl > 0 {
		expires = s.clock.Now().Add(ttl)
	}
	if _, exists := b.entries[key]; !exists {
		s.items.Add(1)
	}
	b.entries[key] = &entry{value: cloneBytes(value), expireAt: expires}
}

// Get retrieves key. ok=false when missing or expired.
func (s *Store) Get(key string) ([]byte, bool) {
	b := s.bucketFor(key)
	b.RLock()
	e, ok := b.entries[key]
	if ok && e.expireAt.IsZero() {
		val := cloneBytes(e.value)
		b.RUnlock()
		return val, true
	}
	var expired bool
	if ok {
		expired = !e.expireAt.IsZero() && s.clock.Now().After(e.expireAt)
	}
	if expired {
		b.RUnlock()
		b.Lock()
		if cur, ok2 := b.entries[key]; ok2 {
			if !cur.expireAt.IsZero() && s.clock.Now().After(cur.expireAt) {
				delete(b.entries, key)
				s.items.Add(-1)
			}
		}
		b.Unlock()
		return nil, false
	}
	if !ok {
		b.RUnlock()
		return nil, false
	}
	val := cloneBytes(e.value)
	b.RUnlock()
	return val, true
}

// TTL returns remaining lifetime. second bool indicates key existence.
func (s *Store) TTL(key string) (time.Duration, bool) {
	b := s.bucketFor(key)
	b.RLock()
	e, ok := b.entries[key]
	if !ok {
		b.RUnlock()
		return 0, false
	}
	if e.expireAt.IsZero() {
		b.RUnlock()
		return -1, true
	}
	remaining := e.expireAt.Sub(s.clock.Now())
	if remaining < 0 {
		remaining = 0
	}
	b.RUnlock()
	return remaining, true
}

// Del removes keys, returning count.
func (s *Store) Del(keys ...string) int {
	var removed int
	for _, key := range keys {
		b := s.bucketFor(key)
		b.Lock()
		if _, ok := b.entries[key]; ok {
			delete(b.entries, key)
			s.items.Add(-1)
			removed++
		}
		b.Unlock()
	}
	return removed
}

// Incr increments integer stored at key.
func (s *Store) Incr(key string, delta int64) (int64, error) {
	b := s.bucketFor(key)
	b.Lock()
	defer b.Unlock()
	e, ok := b.entries[key]
	var current int64
	if ok {
		if expired := !e.expireAt.IsZero() && s.clock.Now().After(e.expireAt); expired {
			delete(b.entries, key)
			s.items.Add(-1)
			ok = false
		} else {
			var err error
			current, err = parseInt(e.value)
			if err != nil {
				return 0, err
			}
		}
	}
	current += delta
	if !ok {
		s.items.Add(1)
	}
	b.entries[key] = &entry{value: intToBytes(current)}
	return current, nil
}

// Len returns approximation of key count.
func (s *Store) Len() int64 {
	return s.items.Load()
}

func (s *Store) cleaner() {
	ticker := time.NewTicker(s.cleanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

func (s *Store) sweep() {
	now := s.clock.Now()
	for _, b := range s.buckets {
		b.Lock()
		for k, e := range b.entries {
			if !e.expireAt.IsZero() && now.After(e.expireAt) {
				delete(b.entries, k)
				s.items.Add(-1)
			}
		}
		b.Unlock()
	}
}

func cloneBytes(in []byte) []byte {
	if in == nil {
		return nil
	}
	cp := make([]byte, len(in))
	copy(cp, in)
	return cp
}

func parseInt(b []byte) (int64, error) {
	if len(b) == 0 {
		return 0, errNotInteger
	}
	var sign int64 = 1
	var i int
	if b[0] == '-' {
		sign = -1
		i = 1
	}
	var val int64
	for ; i < len(b); i++ {
		d := b[i]
		if d < '0' || d > '9' {
			return 0, errNotInteger
		}
		val = val*10 + int64(d-'0')
	}
	return val * sign, nil
}

func intToBytes(v int64) []byte {
	if v == 0 {
		return []byte{'0'}
	}
	sign := v < 0
	if sign {
		v = -v
	}
	buf := make([]byte, 0, 20)
	for v > 0 {
		buf = append(buf, byte('0'+v%10))
		v /= 10
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	if sign {
		buf = append([]byte{'-'}, buf...)
	}
	return buf
}

func fnv64(key string) uint64 {
	var hash uint64 = 1469598103934665603
	const prime uint64 = 1099511628211
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime
	}
	return hash
}
