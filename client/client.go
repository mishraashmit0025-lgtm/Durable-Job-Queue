// Package client is a Go SDK for the job queue HTTP API, including a worker
// loop that leases jobs, keeps their leases alive while a handler runs, and
// acks or nacks the result.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/mishraashmit0025-lgtm/Durable-Job-Queue/queue"
)

// ErrLeaseLost mirrors the server's 409: another worker owns the job now.
var ErrLeaseLost = errors.New("lease lost")

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

func New(baseURL string) *Client {
	return &Client{BaseURL: baseURL, HTTP: &http.Client{Timeout: 90 * time.Second}}
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return ErrLeaseLost
	}
	if resp.StatusCode >= 300 {
		var e struct{ Error string }
		json.NewDecoder(resp.Body).Decode(&e)
		return fmt.Errorf("%s %s: %d %s", method, path, resp.StatusCode, e.Error)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// EnqueueOptions are optional enqueue settings.
type EnqueueOptions struct {
	Priority       int
	Delay          time.Duration
	MaxAttempts    int
	IdempotencyKey string
}

func (c *Client) Enqueue(ctx context.Context, q string, payload any, o EnqueueOptions) (*queue.Job, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var j queue.Job
	err = c.do(ctx, "POST", "/v1/queues/"+q+"/jobs", map[string]any{
		"payload": json.RawMessage(raw), "priority": o.Priority, "delay_ms": o.Delay.Milliseconds(),
		"max_attempts": o.MaxAttempts, "idempotency_key": o.IdempotencyKey,
	}, &j)
	return &j, err
}

func (c *Client) Lease(ctx context.Context, q, worker string, n int, lease, wait time.Duration) ([]queue.Lease, error) {
	var out struct{ Leases []queue.Lease }
	err := c.do(ctx, "POST", "/v1/queues/"+q+"/lease", map[string]any{
		"worker": worker, "n": n, "lease_ms": lease.Milliseconds(), "wait_ms": wait.Milliseconds(),
	}, &out)
	return out.Leases, err
}

func (c *Client) Ack(ctx context.Context, id string, token uint64, result any) error {
	raw, err := json.Marshal(result)
	if err != nil {
		return err
	}
	return c.do(ctx, "POST", "/v1/jobs/"+id+"/ack", map[string]any{"token": token, "result": json.RawMessage(raw)}, nil)
}

func (c *Client) Nack(ctx context.Context, id string, token uint64, reason string, retryAfter time.Duration) error {
	return c.do(ctx, "POST", "/v1/jobs/"+id+"/nack", map[string]any{
		"token": token, "error": reason, "retry_after_ms": retryAfter.Milliseconds(),
	}, nil)
}

func (c *Client) Heartbeat(ctx context.Context, id string, token uint64, extend time.Duration) error {
	return c.do(ctx, "POST", "/v1/jobs/"+id+"/heartbeat", map[string]any{"token": token, "extend_ms": extend.Milliseconds()}, nil)
}

func (c *Client) Get(ctx context.Context, id string) (*queue.Job, error) {
	var j queue.Job
	return &j, c.do(ctx, "GET", "/v1/jobs/"+id, nil, &j)
}

// Handler processes one job. Returning an error nacks it (it will be retried);
// a RetryAfter error picks the delay.
type Handler func(ctx context.Context, job *queue.Job) (result any, err error)

// RetryAfter wraps an error with an explicit retry delay, e.g. from a 429.
type RetryAfter struct {
	Err   error
	After time.Duration
}

func (r RetryAfter) Error() string { return r.Err.Error() }
func (r RetryAfter) Unwrap() error { return r.Err }

// WorkerOptions configure Work.
type WorkerOptions struct {
	Name        string
	Concurrency int           // parallel handlers [4]
	Lease       time.Duration // lease length; renewed at a third of it [30s]
	Logger      *log.Logger
}

// Work runs handlers for jobs in q until ctx is cancelled, then waits for
// in-flight jobs to finish.
func (c *Client) Work(ctx context.Context, q string, h Handler, o WorkerOptions) error {
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if o.Lease <= 0 {
		o.Lease = 30 * time.Second
	}
	if o.Name == "" {
		o.Name = "worker"
	}
	logf := func(format string, args ...any) {
		if o.Logger != nil {
			o.Logger.Printf(format, args...)
		}
	}
	slots := make(chan struct{}, o.Concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	for ctx.Err() == nil {
		slots <- struct{}{}
		free := 1
		for free < o.Concurrency && len(slots) < cap(slots) {
			slots <- struct{}{}
			free++
		}
		leases, err := c.Lease(ctx, q, o.Name, free, o.Lease, 20*time.Second)
		if err != nil {
			for i := 0; i < free; i++ {
				<-slots
			}
			if ctx.Err() != nil {
				break
			}
			logf("lease: %v", err)
			time.Sleep(time.Second)
			continue
		}
		for i := len(leases); i < free; i++ {
			<-slots
		}
		for _, l := range leases {
			wg.Add(1)
			go func(l queue.Lease) {
				defer wg.Done()
				defer func() { <-slots }()
				c.runOne(l, h, o, logf)
			}(l)
		}
	}
	return nil
}

func (c *Client) runOne(l queue.Lease, h Handler, o WorkerOptions, logf func(string, ...any)) {
	// Handlers keep running on shutdown; jobs are finished, not abandoned.
	jobCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		t := time.NewTicker(o.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
				if err := c.Heartbeat(jobCtx, l.Job.ID, l.Token, o.Lease); errors.Is(err, ErrLeaseLost) {
					logf("job %s: lease lost, abandoning", l.Job.ID)
					cancel()
					return
				}
			}
		}
	}()
	result, err := safeCall(jobCtx, h, l.Job)
	cancel()
	<-hbDone
	bg := context.Background()
	if err != nil {
		var ra RetryAfter
		after := time.Duration(0)
		if errors.As(err, &ra) {
			after = ra.After
		}
		if nerr := c.Nack(bg, l.Job.ID, l.Token, err.Error(), after); nerr != nil {
			logf("job %s: nack: %v", l.Job.ID, nerr)
		}
		return
	}
	if aerr := c.Ack(bg, l.Job.ID, l.Token, result); aerr != nil {
		logf("job %s: ack: %v", l.Job.ID, aerr)
	}
}

func safeCall(ctx context.Context, h Handler, j *queue.Job) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return h(ctx, j)
}
