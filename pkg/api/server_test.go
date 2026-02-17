package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/komalk/cashe/internal/store"
)

func TestPutGet(t *testing.T) {
	st := store.NewStore(store.StoreOptions{Shards: 2})
	t.Cleanup(st.Close)
	srv := NewServer(st)

	reqBody := map[string]any{"value": base64.StdEncoding.EncodeToString([]byte("hello"))}
	bodyBytes, _ := json.Marshal(reqBody)
	r := httptest.NewRequest(http.MethodPut, "/kv/foo", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()
	srv.Router().ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected 204 got %d", w.Code)
	}

	r = httptest.NewRequest(http.MethodGet, "/kv/foo", nil)
	w = httptest.NewRecorder()
	srv.Router().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 got %d", w.Code)
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	val, ok := resp["value"].(string)
	if !ok {
		t.Fatalf("missing value field")
	}
	if val != base64.StdEncoding.EncodeToString([]byte("hello")) {
		t.Fatalf("unexpected value %v", resp)
	}
}
