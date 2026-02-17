package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/komalk/cashe/internal/store"
)

// Server exposes HTTP handlers bridging to the in-memory store.
type Server struct {
	store *store.Store
}

// NewServer builds a new HTTP server wrapper.
func NewServer(st *store.Store) *Server {
	return &Server{store: st}
}

// Router returns a configured chi.Router.
func (s *Server) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Put("/kv/{key}", s.handlePut)
	r.Get("/kv/{key}", s.handleGet)
	r.Delete("/kv/{key}", s.handleDelete)
	r.Post("/atomic/incr", s.handleIncr)
	r.Post("/ops/mset", s.handleMSet)
	return r
}

type putRequest struct {
	Value string `json:"value"`
	TTLms int64  `json:"ttl_ms"`
}

type valueResponse struct {
	Value string `json:"value"`
	TTLms *int64 `json:"ttl_ms,omitempty"`
}

type incrRequest struct {
	Key   string `json:"key"`
	Delta int64  `json:"delta"`
}

type incrResponse struct {
	Value int64 `json:"value"`
}

type msetRequest struct {
	Pairs []putPair `json:"pairs"`
}

type putPair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTLms int64  `json:"ttl_ms"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	var req putRequest
	if err := decodeJSON(r, &req); err != nil {
		respondErr(w, http.StatusBadRequest, err)
		return
	}
	data, err := base64.StdEncoding.DecodeString(req.Value)
	if err != nil {
		respondErr(w, http.StatusBadRequest, errors.New("value must be base64"))
		return
	}
	var ttl time.Duration
	if req.TTLms > 0 {
		ttl = time.Duration(req.TTLms) * time.Millisecond
	}
	s.store.Set(key, data, ttl)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	val, ok := s.store.Get(key)
	if !ok {
		respondErr(w, http.StatusNotFound, errors.New("key not found"))
		return
	}
	resp := valueResponse{Value: base64.StdEncoding.EncodeToString(val)}
	if ttl, ok := s.store.TTL(key); ok && ttl >= 0 {
		ms := ttl.Milliseconds()
		resp.TTLms = &ms
	}
	respondJSON(w, http.StatusOK, resp)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")
	removed := s.store.Del(key)
	if removed == 0 {
		respondErr(w, http.StatusNotFound, errors.New("key not found"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleIncr(w http.ResponseWriter, r *http.Request) {
	var req incrRequest
	if err := decodeJSON(r, &req); err != nil {
		respondErr(w, http.StatusBadRequest, err)
		return
	}
	if req.Key == "" {
		respondErr(w, http.StatusBadRequest, errors.New("key required"))
		return
	}
	v, err := s.store.Incr(req.Key, req.Delta)
	if err != nil {
		respondErr(w, http.StatusBadRequest, err)
		return
	}
	respondJSON(w, http.StatusOK, incrResponse{Value: v})
}

func (s *Server) handleMSet(w http.ResponseWriter, r *http.Request) {
	var req msetRequest
	if err := decodeJSON(r, &req); err != nil {
		respondErr(w, http.StatusBadRequest, err)
		return
	}
	for _, pair := range req.Pairs {
		if pair.Key == "" {
			respondErr(w, http.StatusBadRequest, errors.New("key required"))
			return
		}
		data, err := base64.StdEncoding.DecodeString(pair.Value)
		if err != nil {
			respondErr(w, http.StatusBadRequest, errors.New("value must be base64"))
			return
		}
		var ttl time.Duration
		if pair.TTLms > 0 {
			ttl = time.Duration(pair.TTLms) * time.Millisecond
		}
		s.store.Set(pair.Key, data, ttl)
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := jsonNewDecoder(r.Body)
	return dec.Decode(v)
}

func respondErr(w http.ResponseWriter, status int, err error) {
	respondJSON(w, status, map[string]string{"error": err.Error()})
}

func respondJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := jsonNewEncoder(w)
	_ = enc.Encode(payload)
}

func jsonNewDecoder(r io.Reader) *json.Decoder {
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	return dec
}

func jsonNewEncoder(w io.Writer) *json.Encoder {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc
}
