// Package server exposes a queue.Broker over a small JSON HTTP API.
//
//	POST /v1/queues/{queue}/jobs            enqueue
//	POST /v1/queues/{queue}/lease           lease up to n jobs (long poll with wait_ms)
//	POST /v1/jobs/{id}/ack                  {"token": ..., "result": ...}
//	POST /v1/jobs/{id}/nack                 {"token": ..., "error": "...", "retry_after_ms": 0}
//	POST /v1/jobs/{id}/heartbeat            {"token": ..., "extend_ms": 30000}
//	POST /v1/jobs/{id}/requeue              move a dead job back to pending
//	GET  /v1/jobs/{id}
//	GET  /v1/queues/{queue}/dead?limit=100
//	GET  /v1/stats
//	GET  /metrics                           Prometheus text format
//	GET  /healthz
package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/queue"
)

const maxBody = 1 << 20

func New(b *queue.Broker) http.Handler {
	s := &srv{b: b}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/queues/{queue}/jobs", s.enqueue)
	mux.HandleFunc("POST /v1/queues/{queue}/lease", s.lease)
	mux.HandleFunc("GET /v1/queues/{queue}/dead", s.dead)
	mux.HandleFunc("POST /v1/jobs/{id}/ack", s.ack)
	mux.HandleFunc("POST /v1/jobs/{id}/nack", s.nack)
	mux.HandleFunc("POST /v1/jobs/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("POST /v1/jobs/{id}/requeue", s.requeue)
	mux.HandleFunc("GET /v1/jobs/{id}", s.get)
	mux.HandleFunc("GET /v1/stats", s.stats)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	return mux
}

type srv struct{ b *queue.Broker }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, queue.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, queue.ErrLeaseLost), errors.Is(err, queue.ErrNotDead):
		code = http.StatusConflict
	case errors.Is(err, queue.ErrClosed):
		code = http.StatusServiceUnavailable
	case errors.Is(err, errBadRequest):
		code = http.StatusBadRequest
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

var errBadRequest = errors.New("bad request")

func decode(r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return errors.Join(errBadRequest, err)
	}
	return nil
}

func (s *srv) enqueue(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Payload        json.RawMessage `json:"payload"`
		Priority       int             `json:"priority"`
		DelayMS        int64           `json:"delay_ms"`
		MaxAttempts    int             `json:"max_attempts"`
		IdempotencyKey string          `json:"idempotency_key"`
	}
	if err := decode(r, &body); err != nil {
		writeErr(w, err)
		return
	}
	key := body.IdempotencyKey
	if key == "" {
		key = r.Header.Get("Idempotency-Key")
	}
	j, created, err := s.b.Enqueue(queue.EnqueueRequest{
		Queue: r.PathValue("queue"), Payload: body.Payload, Priority: body.Priority,
		Delay: time.Duration(body.DelayMS) * time.Millisecond, MaxAttempts: body.MaxAttempts, IdempotencyKey: key,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, j)
}

func (s *srv) lease(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Worker  string `json:"worker"`
		N       int    `json:"n"`
		LeaseMS int64  `json:"lease_ms"`
		WaitMS  int64  `json:"wait_ms"`
	}
	if err := decode(r, &body); err != nil {
		writeErr(w, err)
		return
	}
	wait := min(time.Duration(body.WaitMS)*time.Millisecond, 60*time.Second)
	leases, err := s.b.Lease(r.Context(), r.PathValue("queue"), body.Worker, min(body.N, 1000),
		time.Duration(body.LeaseMS)*time.Millisecond, wait)
	if err != nil {
		writeErr(w, err)
		return
	}
	if leases == nil {
		leases = []queue.Lease{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"leases": leases})
}

func (s *srv) ack(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token  uint64          `json:"token"`
		Result json.RawMessage `json:"result"`
	}
	if err := decode(r, &body); err != nil {
		writeErr(w, err)
		return
	}
	if err := s.b.Ack(r.PathValue("id"), body.Token, body.Result); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "succeeded"})
}

func (s *srv) nack(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token        uint64 `json:"token"`
		Error        string `json:"error"`
		RetryAfterMS int64  `json:"retry_after_ms"`
	}
	if err := decode(r, &body); err != nil {
		writeErr(w, err)
		return
	}
	j, err := s.b.Nack(r.PathValue("id"), body.Token, body.Error, time.Duration(body.RetryAfterMS)*time.Millisecond)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *srv) heartbeat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token    uint64 `json:"token"`
		ExtendMS int64  `json:"extend_ms"`
	}
	if err := decode(r, &body); err != nil {
		writeErr(w, err)
		return
	}
	until, err := s.b.Heartbeat(r.PathValue("id"), body.Token, time.Duration(body.ExtendMS)*time.Millisecond)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_until": until})
}

func (s *srv) requeue(w http.ResponseWriter, r *http.Request) {
	j, err := s.b.Requeue(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *srv) get(w http.ResponseWriter, r *http.Request) {
	j, err := s.b.Get(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (s *srv) dead(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	jobs, err := s.b.Dead(r.PathValue("queue"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	if jobs == nil {
		jobs = []*queue.Job{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs})
}

func (s *srv) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"queues": s.b.Stats()})
}

func (s *srv) metrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.b.Metrics().WritePrometheus(w, s.b.Stats())
}
