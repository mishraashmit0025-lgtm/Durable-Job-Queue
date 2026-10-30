package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/client"
	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/queue"
	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/server"
)

func start(t *testing.T) (*client.Client, *queue.Broker, string) {
	t.Helper()
	b, err := queue.Open(queue.Config{
		Path:    filepath.Join(t.TempDir(), "q.db"),
		Backoff: queue.Backoff{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(server.New(b))
	t.Cleanup(func() {
		ts.Close()
		b.Close()
	})
	return client.New(ts.URL), b, ts.URL
}

// Workers with flaky handlers (some fail, one panics) still finish every job,
// and a job that always fails ends up in the dead letter queue.
func TestWorkersEndToEnd(t *testing.T) {
	c, _, _ := start(t)
	ctx := context.Background()
	const n = 200
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		j, err := c.Enqueue(ctx, "emails", map[string]int{"i": i}, client.EnqueueOptions{MaxAttempts: 5})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = j.ID
	}
	poison, _ := c.Enqueue(ctx, "emails", map[string]int{"i": -1}, client.EnqueueOptions{MaxAttempts: 3})

	var attempts sync.Map
	var runs atomic.Int64
	handler := func(_ context.Context, j *queue.Job) (any, error) {
		runs.Add(1)
		var p struct{ I int }
		json.Unmarshal(j.Payload, &p)
		k, _ := attempts.LoadOrStore(p.I, new(atomic.Int32))
		a := k.(*atomic.Int32).Add(1)
		switch {
		case p.I == -1:
			return nil, errors.New("poison message")
		case p.I == 7 && a == 1:
			panic("handler bug")
		case p.I%10 == 0 && a == 1:
			return nil, client.RetryAfter{Err: errors.New("rate limited"), After: 5 * time.Millisecond}
		}
		return map[string]int{"sent": p.I}, nil
	}
	wctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c.Work(wctx, "emails", handler, client.WorkerOptions{Name: fmt.Sprint("w", w), Concurrency: 8, Lease: 5 * time.Second})
		}(w)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		done := 0
		for _, id := range ids {
			if j, err := c.Get(ctx, id); err == nil && j.State == queue.StateSucceeded {
				done++
			}
		}
		p, _ := c.Get(ctx, poison.ID)
		if done == n && p.State == queue.StateDead {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d jobs succeeded, poison state %s", done, n, p.State)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if want := int64(n + n/10 + 1 + 3); runs.Load() != want {
		t.Fatalf("handler ran %d times, want %d", runs.Load(), want)
	}
	p, _ := c.Get(ctx, poison.ID)
	if p.Attempts != 3 || p.LastError != "poison message" {
		t.Fatalf("poison job %+v", p)
	}
}

func TestHTTPErrorsAndMetrics(t *testing.T) {
	c, _, url := start(t)
	ctx := context.Background()
	if _, err := c.Get(ctx, "nope"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("missing job: %v", err)
	}
	j, _ := c.Enqueue(ctx, "q", "hi", client.EnqueueOptions{})
	ls, _ := c.Lease(ctx, "q", "w", 1, time.Minute, 0)
	if err := c.Ack(ctx, j.ID, ls[0].Token+999, nil); !errors.Is(err, client.ErrLeaseLost) {
		t.Fatalf("wrong token should be 409, got %v", err)
	}
	if err := c.Ack(ctx, j.ID, ls[0].Token, "ok"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url+"/v1/queues/q/jobs", "application/json", strings.NewReader(`{"bogus":1}`))
	if err != nil || resp.StatusCode != 400 {
		t.Fatalf("unknown field should be 400, got %v %v", resp.StatusCode, err)
	}
	resp, _ = http.Get(url + "/metrics")
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	for _, want := range []string{`jobq_jobs_succeeded_total{queue="q"} 1`, `jobq_job_duration_seconds_count{queue="q"} 1`} {
		if !strings.Contains(sb.String(), want) {
			t.Fatalf("metrics missing %q:\n%s", want, sb.String())
		}
	}
}
