package store

import (
	"testing"
	"time"
)

type fakeClock struct {
	now time.Time
}

func (f *fakeClock) Now() time.Time { return f.now }

func (f *fakeClock) advance(d time.Duration) {
	f.now = f.now.Add(d)
}

func newTestStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clk := &fakeClock{now: time.Unix(1, 0)}
	s := NewStore(StoreOptions{Shards: 4, CleanInterval: 10 * time.Millisecond, Clock: clk})
	t.Cleanup(func() { s.Close() })
	return s, clk
}

func TestSetGet(t *testing.T) {
	s, _ := newTestStore(t)
	s.Set("foo", []byte("bar"), 0)
	if got, ok := s.Get("foo"); !ok || string(got) != "bar" {
		t.Fatalf("expected bar, got %q ok=%v", got, ok)
	}
}

func TestTTLAndExpiration(t *testing.T) {
	s, clk := newTestStore(t)
	s.Set("k", []byte("v"), time.Second)
	if ttl, ok := s.TTL("k"); !ok || ttl <= 0 {
		t.Fatalf("ttl should be >0, got %v ok=%v", ttl, ok)
	}
	clk.advance(2 * time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("expected key to expire")
	}
}

func TestDel(t *testing.T) {
	s, _ := newTestStore(t)
	s.Set("k1", []byte("v"), 0)
	s.Set("k2", []byte("v"), 0)
	if removed := s.Del("k1", "missing"); removed != 1 {
		t.Fatalf("expected 1 removed got %d", removed)
	}
	if _, ok := s.Get("k1"); ok {
		t.Fatal("k1 should be gone")
	}
	if s.Len() != 1 {
		t.Fatalf("expected len 1 got %d", s.Len())
	}
}

func TestIncr(t *testing.T) {
	s, _ := newTestStore(t)
	v, err := s.Incr("counter", 1)
	if err != nil || v != 1 {
		t.Fatalf("expected 1 got %d err=%v", v, err)
	}
	v, err = s.Incr("counter", 5)
	if err != nil || v != 6 {
		t.Fatalf("expected 6 got %d err=%v", v, err)
	}
}

func TestIncrNotInteger(t *testing.T) {
	s, _ := newTestStore(t)
	s.Set("key", []byte("abc"), 0)
	if _, err := s.Incr("key", 1); err == nil {
		t.Fatal("expected error")
	}
}
