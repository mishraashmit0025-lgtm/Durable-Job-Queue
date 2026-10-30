package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func open(t *testing.T, path string, clk *fakeClock) *Broker {
	t.Helper()
	cfg := Config{Path: path, Backoff: Backoff{Base: time.Second, Max: time.Second}, TickInterval: time.Millisecond}
	if clk != nil {
		cfg.Now = clk.Now
	}
	b, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newBroker(t *testing.T) (*Broker, *fakeClock, string) {
	clk := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "q.db")
	b := open(t, path, clk)
	t.Cleanup(func() { b.Close() })
	return b, clk, path
}

func enqueue(t *testing.T, b *Broker, req EnqueueRequest) *Job {
	t.Helper()
	j, _, err := b.Enqueue(req)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func leaseOne(t *testing.T, b *Broker, q string, lease time.Duration) *Lease {
	t.Helper()
	ls, err := b.Lease(context.Background(), q, "w", 1, lease, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) == 0 {
		return nil
	}
	return &ls[0]
}

func TestPriorityThenFIFO(t *testing.T) {
	b, _, _ := newBroker(t)
	for i, p := range []int{0, 5, 0, 5, 1} {
		enqueue(t, b, EnqueueRequest{Queue: "q", Priority: p, Payload: json.RawMessage(fmt.Sprint(i))})
	}
	var got []string
	for l := leaseOne(t, b, "q", time.Minute); l != nil; l = leaseOne(t, b, "q", time.Minute) {
		got = append(got, string(l.Job.Payload))
	}
	want := "[1 3 4 0 2]"
	if fmt.Sprint(got) != want {
		t.Fatalf("lease order %v, want %s", got, want)
	}
}

func TestAckCompletesJob(t *testing.T) {
	b, _, _ := newBroker(t)
	j := enqueue(t, b, EnqueueRequest{Queue: "q", Payload: json.RawMessage(`{"x":1}`)})
	l := leaseOne(t, b, "q", time.Minute)
	if err := b.Ack(j.ID, l.Token, []byte(`"done"`)); err != nil {
		t.Fatal(err)
	}
	got, err := b.Get(j.ID)
	if err != nil || got.State != StateSucceeded || string(got.Result) != `"done"` {
		t.Fatalf("after ack: %+v %v", got, err)
	}
	if err := b.Ack(j.ID, l.Token, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("double ack: %v", err)
	}
}

// A worker that stalls past its lease must not be able to ack after the job was
// handed to someone else. This is the fencing-token property.
func TestExpiredLeaseIsRedeliveredAndFenced(t *testing.T) {
	b, clk, _ := newBroker(t)
	j := enqueue(t, b, EnqueueRequest{Queue: "q"})
	first := leaseOne(t, b, "q", 10*time.Second)
	clk.Advance(11 * time.Second) // lease runs out: back to scheduled with backoff
	time.Sleep(20 * time.Millisecond)
	clk.Advance(2 * time.Second) // backoff (max 1s) elapses
	second := leaseOne(t, b, "q", 10*time.Second)
	if second == nil || second.Job.ID != j.ID {
		t.Fatalf("expected redelivery of %s, got %+v", j.ID, second)
	}
	if second.Job.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", second.Job.Attempts)
	}
	if err := b.Ack(j.ID, first.Token, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale worker ack should be rejected, got %v", err)
	}
	if err := b.Ack(j.ID, second.Token, nil); err != nil {
		t.Fatalf("current worker ack: %v", err)
	}
	st := b.Stats()[0]
	if st.Expired != 1 || st.Succeeded != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestHeartbeatKeepsLease(t *testing.T) {
	b, clk, _ := newBroker(t)
	j := enqueue(t, b, EnqueueRequest{Queue: "q"})
	l := leaseOne(t, b, "q", 10*time.Second)
	for i := 0; i < 5; i++ {
		clk.Advance(8 * time.Second)
		if _, err := b.Heartbeat(j.ID, l.Token, 10*time.Second); err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
	}
	time.Sleep(10 * time.Millisecond) // let the scheduler tick
	if err := b.Ack(j.ID, l.Token, nil); err != nil {
		t.Fatalf("ack after 40s of heartbeats: %v", err)
	}
}

func TestRetriesThenDeadLetterThenRequeue(t *testing.T) {
	b, clk, _ := newBroker(t)
	j := enqueue(t, b, EnqueueRequest{Queue: "q", MaxAttempts: 3})
	for attempt := 1; attempt <= 3; attempt++ {
		l := leaseOne(t, b, "q", time.Minute)
		if l == nil {
			t.Fatalf("attempt %d: nothing to lease", attempt)
		}
		after, err := b.Nack(j.ID, l.Token, fmt.Sprintf("boom %d", attempt), 0)
		if err != nil {
			t.Fatal(err)
		}
		if attempt < 3 && after.State != StateScheduled {
			t.Fatalf("attempt %d: state %s", attempt, after.State)
		}
		if leaseOne(t, b, "q", time.Minute) != nil {
			t.Fatalf("attempt %d: job available before its backoff elapsed", attempt)
		}
		clk.Advance(2 * time.Second)
	}
	got, _ := b.Get(j.ID)
	if got.State != StateDead || got.LastError != "boom 3" {
		t.Fatalf("expected dead job, got %+v", got)
	}
	dead, _ := b.Dead("q", 10)
	if len(dead) != 1 || dead[0].ID != j.ID {
		t.Fatalf("dead letter list: %+v", dead)
	}
	if _, err := b.Requeue(j.ID); err != nil {
		t.Fatal(err)
	}
	l := leaseOne(t, b, "q", time.Minute)
	if l == nil || l.Job.Attempts != 1 {
		t.Fatalf("requeued job: %+v", l)
	}
}

func TestDelayedJob(t *testing.T) {
	b, clk, _ := newBroker(t)
	enqueue(t, b, EnqueueRequest{Queue: "q", Delay: time.Hour})
	if leaseOne(t, b, "q", time.Minute) != nil {
		t.Fatal("delayed job delivered early")
	}
	clk.Advance(time.Hour)
	if leaseOne(t, b, "q", time.Minute) == nil {
		t.Fatal("delayed job not delivered when due")
	}
}

func TestIdempotencyKey(t *testing.T) {
	b, _, _ := newBroker(t)
	a, created1, _ := b.Enqueue(EnqueueRequest{Queue: "q", IdempotencyKey: "order-42"})
	c, created2, _ := b.Enqueue(EnqueueRequest{Queue: "q", IdempotencyKey: "order-42"})
	d, created3, _ := b.Enqueue(EnqueueRequest{Queue: "other", IdempotencyKey: "order-42"})
	if !created1 || created2 || !created3 || a.ID != c.ID || a.ID == d.ID {
		t.Fatalf("idempotency: %v %v %v / %s %s %s", created1, created2, created3, a.ID, c.ID, d.ID)
	}
}

// Everything acknowledged before a restart must still be there after it, and
// a lease held across the restart must expire and be redelivered.
func TestRecoveryAfterRestart(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "q.db")
	b := open(t, path, clk)
	done := enqueue(t, b, EnqueueRequest{Queue: "q", Priority: 9})
	inflight := enqueue(t, b, EnqueueRequest{Queue: "q", Priority: 8})
	waiting := enqueue(t, b, EnqueueRequest{Queue: "q"})
	delayed := enqueue(t, b, EnqueueRequest{Queue: "q", Delay: time.Minute})
	l := leaseOne(t, b, "q", time.Minute)
	b.Ack(done.ID, l.Token, nil)
	leaseOne(t, b, "q", 30*time.Second) // inflight, never acked: the "worker" crashes
	b.Close()

	b = open(t, path, clk)
	defer b.Close()
	for id, want := range map[string]State{done.ID: StateSucceeded, inflight.ID: StateLeased, waiting.ID: StatePending, delayed.ID: StateScheduled} {
		got, err := b.Get(id)
		if err != nil || got.State != want {
			t.Fatalf("job %s after restart: %+v %v, want %s", id, got, err, want)
		}
	}
	clk.Advance(2 * time.Minute)
	time.Sleep(20 * time.Millisecond) // scheduler notices the expired lease
	clk.Advance(2 * time.Second)      // and its retry backoff elapses
	seen := map[string]bool{}
	for l := leaseOne(t, b, "q", time.Minute); l != nil; l = leaseOne(t, b, "q", time.Minute) {
		seen[l.Job.ID] = true
	}
	if !seen[inflight.ID] || !seen[waiting.ID] || !seen[delayed.ID] || len(seen) != 3 {
		t.Fatalf("after restart and lease expiry leased %v", seen)
	}
}

func TestLongPollWakesOnEnqueue(t *testing.T) {
	b, _, _ := newBroker(t)
	got := make(chan []Lease, 1)
	go func() {
		ls, _ := b.Lease(context.Background(), "q", "w", 1, time.Minute, 5*time.Second)
		got <- ls
	}()
	time.Sleep(50 * time.Millisecond)
	t0 := time.Now()
	enqueue(t, b, EnqueueRequest{Queue: "q"})
	select {
	case ls := <-got:
		if len(ls) != 1 {
			t.Fatalf("long poll returned %d leases", len(ls))
		}
		if time.Since(t0) > time.Second {
			t.Fatalf("long poll took %v to wake", time.Since(t0))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long poll never woke")
	}
}

// Many concurrent workers must never hold the same job at the same time, and
// every job must be acked exactly once.
func TestConcurrentWorkersNoDoubleLease(t *testing.T) {
	b, _, _ := newBroker(t)
	const jobs = 2000
	for i := 0; i < jobs; i++ {
		enqueue(t, b, EnqueueRequest{Queue: "q", Payload: json.RawMessage(fmt.Sprint(i))})
	}
	var mu sync.Mutex
	acked := map[string]int{}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				ls, err := b.Lease(context.Background(), "q", fmt.Sprint("w", w), 8, time.Minute, 0)
				if err != nil {
					t.Error(err)
					return
				}
				if len(ls) == 0 {
					return
				}
				for _, l := range ls {
					if err := b.Ack(l.Job.ID, l.Token, nil); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					acked[l.Job.ID]++
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()
	if len(acked) != jobs {
		t.Fatalf("acked %d distinct jobs, want %d", len(acked), jobs)
	}
	for id, n := range acked {
		if n != 1 {
			t.Fatalf("job %s acked %d times", id, n)
		}
	}
}

func TestBackoffBounds(t *testing.T) {
	bo := Backoff{Base: 100 * time.Millisecond, Max: 2 * time.Second}
	for attempt := 1; attempt < 20; attempt++ {
		ceil := min(bo.Base<<uint(attempt-1), bo.Max)
		for i := 0; i < 200; i++ {
			if d := bo.Delay(attempt); d < 0 || d >= ceil {
				t.Fatalf("attempt %d: delay %v outside [0, %v)", attempt, d, ceil)
			}
		}
	}
}
