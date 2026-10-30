package queue

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotFound  = errors.New("job not found")
	ErrLeaseLost = errors.New("lease lost: the job was re-delivered or already finished")
	ErrNotDead   = errors.New("job is not in the dead letter queue")
	ErrClosed    = errors.New("broker closed")
)

// Config tunes a Broker. Zero values pick the defaults in brackets.
type Config struct {
	Path               string        // bbolt file
	DefaultMaxAttempts int           // [5]
	DefaultLease       time.Duration // [30s]
	MaxLease           time.Duration // [1h]
	Backoff            Backoff       // [1s base, 5m max]
	TickInterval       time.Duration // scheduler resolution [20ms]
	RetainSucceeded    time.Duration // delete succeeded jobs older than this; 0 keeps them
	Now                func() time.Time
}

func (c *Config) defaults() {
	if c.DefaultMaxAttempts == 0 {
		c.DefaultMaxAttempts = 5
	}
	if c.DefaultLease == 0 {
		c.DefaultLease = 30 * time.Second
	}
	if c.MaxLease == 0 {
		c.MaxLease = time.Hour
	}
	if c.Backoff.Base == 0 {
		c.Backoff = Backoff{Base: time.Second, Max: 5 * time.Minute}
	}
	if c.TickInterval == 0 {
		c.TickInterval = 20 * time.Millisecond
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Broker owns all live jobs in memory and persists every change through a
// group-commit writer. A call returns only after its change is on disk.
type Broker struct {
	cfg   Config
	store *store
	wal   *committer

	mu        sync.Mutex
	jobs      map[string]*Job // scheduled, pending and leased jobs
	ready     map[string]*readyHeap
	scheduled timeHeap
	leases    timeHeap
	idem      map[string]string
	waiters   map[string][]chan struct{}
	seq       uint64
	token     uint64
	closed    bool
	metrics   *Metrics

	stop chan struct{}
	done chan struct{}
}

// Open loads the store and rebuilds the in-memory indexes.
func Open(cfg Config) (*Broker, error) {
	cfg.defaults()
	st, err := openStore(cfg.Path)
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg: cfg, store: st, jobs: map[string]*Job{}, ready: map[string]*readyHeap{},
		idem: map[string]string{}, waiters: map[string][]chan struct{}{}, metrics: newMetrics(),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	err = st.each(func(j *Job) error {
		if j.IdempotencyKey != "" {
			b.idem[j.Queue+"\x00"+j.IdempotencyKey] = j.ID
		}
		b.token = max(b.token, j.LeaseToken)
		b.metrics.queue(j.Queue)
		switch j.State {
		case StateSucceeded, StateDead:
			return nil
		}
		b.seq++
		j.seq = b.seq
		b.jobs[j.ID] = j
		switch j.State {
		case StatePending:
			b.readyHeap(j.Queue).push(j)
		case StateScheduled:
			heap.Push(&b.scheduled, timeEntry{j.RunAt, j.ID, 0})
		case StateLeased:
			// the worker may still be running it; let the lease run out normally
			heap.Push(&b.leases, timeEntry{j.LeaseUntil, j.ID, j.LeaseToken})
		}
		return nil
	})
	if err != nil {
		st.close()
		return nil, err
	}
	b.wal = newCommitter(st)
	go b.loop()
	return b, nil
}

// Close stops the scheduler and closes the store.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	for q, ws := range b.waiters {
		for _, w := range ws {
			close(w)
		}
		delete(b.waiters, q)
	}
	b.mu.Unlock()
	close(b.stop)
	<-b.done
	b.wal.close()
	return b.store.close()
}

func (b *Broker) readyHeap(q string) *readyHeap {
	h := b.ready[q]
	if h == nil {
		h = &readyHeap{}
		b.ready[q] = h
	}
	return h
}

// persist queues copies of the jobs for the group commit. Called with b.mu held.
func (b *Broker) persist(jobs ...*Job) uint64 {
	cs := make([]*Job, len(jobs))
	for i, j := range jobs {
		cs[i] = j.clone()
	}
	return b.wal.append(cs)
}

func (b *Broker) wake(q string) {
	for _, w := range b.waiters[q] {
		close(w)
	}
	delete(b.waiters, q)
}

// Enqueue adds a job. With an idempotency key that was already used in the
// same queue it returns the existing job and created=false.
func (b *Broker) Enqueue(req EnqueueRequest) (job *Job, created bool, err error) {
	if req.Queue == "" {
		return nil, false, errors.New("queue name is required")
	}
	if len(req.Payload) == 0 {
		req.Payload = []byte("null")
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, false, ErrClosed
	}
	if req.IdempotencyKey != "" {
		if id, ok := b.idem[req.Queue+"\x00"+req.IdempotencyKey]; ok {
			b.mu.Unlock()
			j, err := b.Get(id)
			if errors.Is(err, ErrNotFound) {
				return nil, false, fmt.Errorf("idempotency key maps to purged job %s", id)
			}
			return j, false, err
		}
	}
	now := b.cfg.Now()
	j := &Job{
		ID: newID(now), Queue: req.Queue, Payload: req.Payload, Priority: req.Priority,
		MaxAttempts: req.MaxAttempts, IdempotencyKey: req.IdempotencyKey,
		RunAt: now.Add(req.Delay), CreatedAt: now, UpdatedAt: now,
	}
	if j.MaxAttempts <= 0 {
		j.MaxAttempts = b.cfg.DefaultMaxAttempts
	}
	b.seq++
	j.seq = b.seq
	b.jobs[j.ID] = j
	if req.IdempotencyKey != "" {
		b.idem[req.Queue+"\x00"+req.IdempotencyKey] = j.ID
	}
	if req.Delay > 0 {
		j.State = StateScheduled
		heap.Push(&b.scheduled, timeEntry{j.RunAt, j.ID, 0})
	} else {
		j.State = StatePending
		b.readyHeap(j.Queue).push(j)
		b.wake(j.Queue)
	}
	b.metrics.queue(j.Queue).enqueued.Add(1)
	ticket := b.persist(j)
	out := j.clone()
	b.mu.Unlock()
	return out, true, b.wal.wait(ticket)
}

// Lease hands out up to n pending jobs from queue to worker for leaseFor. If
// none are ready it waits up to wait for one to arrive (long polling).
func (b *Broker) Lease(ctx context.Context, q, worker string, n int, leaseFor, wait time.Duration) ([]Lease, error) {
	if n <= 0 {
		n = 1
	}
	if leaseFor <= 0 {
		leaseFor = b.cfg.DefaultLease
	}
	leaseFor = min(leaseFor, b.cfg.MaxLease)
	var deadline <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		deadline = t.C
	}
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, ErrClosed
		}
		b.promoteLocked()
		now := b.cfg.Now()
		var got []*Job
		h := b.readyHeap(q)
		for len(got) < n && h.Len() > 0 {
			j := h.pop()
			if j.State != StatePending || b.jobs[j.ID] != j {
				continue // stale heap entry
			}
			b.token++
			j.State = StateLeased
			j.Attempts++
			j.LeaseToken = b.token
			j.LeasedBy = worker
			j.LeasedAt = now
			j.LeaseUntil = now.Add(leaseFor)
			j.UpdatedAt = now
			heap.Push(&b.leases, timeEntry{j.LeaseUntil, j.ID, j.LeaseToken})
			got = append(got, j)
		}
		if len(got) > 0 {
			ticket := b.persist(got...)
			out := make([]Lease, len(got))
			for i, j := range got {
				out[i] = Lease{Job: j.clone(), Token: j.LeaseToken}
			}
			b.metrics.queue(q).leased.Add(uint64(len(got)))
			b.mu.Unlock()
			return out, b.wal.wait(ticket)
		}
		if deadline == nil {
			b.mu.Unlock()
			return nil, nil
		}
		ch := make(chan struct{})
		b.waiters[q] = append(b.waiters[q], ch)
		b.mu.Unlock()
		select {
		case <-ch:
		case <-deadline:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// leased returns the job if token still holds its lease. Called with b.mu held.
func (b *Broker) leased(id string, token uint64) (*Job, error) {
	j := b.jobs[id]
	if j == nil {
		if _, err := b.store.get(id); err == nil {
			return nil, ErrLeaseLost
		}
		return nil, ErrNotFound
	}
	if j.State != StateLeased || j.LeaseToken != token {
		return nil, ErrLeaseLost
	}
	return j, nil
}

// Ack marks a leased job as succeeded. The token fences off a worker whose
// lease already expired: its late ack is rejected with ErrLeaseLost.
func (b *Broker) Ack(id string, token uint64, result []byte) error {
	b.mu.Lock()
	j, err := b.leased(id, token)
	if err != nil {
		b.mu.Unlock()
		return err
	}
	now := b.cfg.Now()
	m := b.metrics.queue(j.Queue)
	m.succeeded.Add(1)
	m.observe(now.Sub(j.LeasedAt))
	j.State = StateSucceeded
	j.Result = result
	j.FinishedAt, j.UpdatedAt = now, now
	j.LeaseUntil, j.LeasedBy = time.Time{}, ""
	delete(b.jobs, id)
	ticket := b.persist(j)
	b.mu.Unlock()
	return b.wal.wait(ticket)
}

// Nack reports a failed attempt. The job is retried after a backoff (or after
// retryAfter, if positive) unless it has used all its attempts, in which case
// it moves to the dead letter queue.
func (b *Broker) Nack(id string, token uint64, reason string, retryAfter time.Duration) (*Job, error) {
	b.mu.Lock()
	j, err := b.leased(id, token)
	if err != nil {
		b.mu.Unlock()
		return nil, err
	}
	b.metrics.queue(j.Queue).failed.Add(1)
	b.failLocked(j, reason, retryAfter)
	ticket := b.persist(j)
	out := j.clone()
	b.mu.Unlock()
	return out, b.wal.wait(ticket)
}

func (b *Broker) failLocked(j *Job, reason string, retryAfter time.Duration) {
	now := b.cfg.Now()
	j.LastError = reason
	j.LeaseUntil, j.LeasedBy = time.Time{}, ""
	j.UpdatedAt = now
	if j.Attempts >= j.MaxAttempts {
		j.State = StateDead
		j.FinishedAt = now
		delete(b.jobs, j.ID)
		b.metrics.queue(j.Queue).dead.Add(1)
		return
	}
	delay := retryAfter
	if delay <= 0 {
		delay = b.cfg.Backoff.Delay(j.Attempts)
	}
	j.State = StateScheduled
	j.RunAt = now.Add(delay)
	heap.Push(&b.scheduled, timeEntry{j.RunAt, j.ID, 0})
}

// Heartbeat extends a lease for a long-running job.
func (b *Broker) Heartbeat(id string, token uint64, extend time.Duration) (time.Time, error) {
	if extend <= 0 {
		extend = b.cfg.DefaultLease
	}
	extend = min(extend, b.cfg.MaxLease)
	b.mu.Lock()
	j, err := b.leased(id, token)
	if err != nil {
		b.mu.Unlock()
		return time.Time{}, err
	}
	j.LeaseUntil = b.cfg.Now().Add(extend)
	heap.Push(&b.leases, timeEntry{j.LeaseUntil, j.ID, j.LeaseToken})
	ticket := b.persist(j)
	until := j.LeaseUntil
	b.mu.Unlock()
	return until, b.wal.wait(ticket)
}

// Requeue moves a dead job back to pending with a fresh attempt budget.
func (b *Broker) Requeue(id string) (*Job, error) {
	b.mu.Lock()
	if b.jobs[id] != nil {
		b.mu.Unlock()
		return nil, ErrNotDead
	}
	j, err := b.store.get(id)
	switch {
	case err != nil:
		b.mu.Unlock()
		return nil, err
	case j == nil:
		b.mu.Unlock()
		return nil, ErrNotFound
	case j.State != StateDead:
		b.mu.Unlock()
		return nil, ErrNotDead
	}
	now := b.cfg.Now()
	j.State, j.Attempts, j.RunAt, j.UpdatedAt, j.FinishedAt = StatePending, 0, now, now, time.Time{}
	b.seq++
	j.seq = b.seq
	b.jobs[id] = j
	b.readyHeap(j.Queue).push(j)
	b.wake(j.Queue)
	ticket := b.persist(j)
	out := j.clone()
	b.mu.Unlock()
	return out, b.wal.wait(ticket)
}

// Get returns a snapshot of the job.
func (b *Broker) Get(id string) (*Job, error) {
	b.mu.Lock()
	if j := b.jobs[id]; j != nil {
		out := j.clone()
		b.mu.Unlock()
		return out, nil
	}
	b.mu.Unlock()
	j, err := b.store.get(id)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, ErrNotFound
	}
	return j, nil
}

// Dead lists dead-lettered jobs of a queue, oldest first.
func (b *Broker) Dead(q string, limit int) ([]*Job, error) {
	var out []*Job
	err := b.store.each(func(j *Job) error {
		if j.Queue == q && j.State == StateDead && (limit <= 0 || len(out) < limit) {
			out = append(out, j)
		}
		return nil
	})
	return out, err
}

// QueueStats is a point-in-time view of one queue.
type QueueStats struct {
	Queue     string `json:"queue"`
	Pending   int    `json:"pending"`
	Scheduled int    `json:"scheduled"`
	Leased    int    `json:"leased"`
	Enqueued  uint64 `json:"enqueued_total"`
	Succeeded uint64 `json:"succeeded_total"`
	Failed    uint64 `json:"failed_attempts_total"`
	Expired   uint64 `json:"leases_expired_total"`
	Dead      uint64 `json:"dead_total"`
}

func (b *Broker) Stats() []QueueStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	byQ := map[string]*QueueStats{}
	for name := range b.metrics.queues {
		byQ[name] = &QueueStats{Queue: name}
	}
	for _, j := range b.jobs {
		s := byQ[j.Queue]
		if s == nil {
			s = &QueueStats{Queue: j.Queue}
			byQ[j.Queue] = s
		}
		switch j.State {
		case StatePending:
			s.Pending++
		case StateScheduled:
			s.Scheduled++
		case StateLeased:
			s.Leased++
		}
	}
	var out []QueueStats
	for name, s := range byQ {
		if m := b.metrics.queues[name]; m != nil {
			s.Enqueued, s.Succeeded, s.Failed = m.enqueued.Load(), m.succeeded.Load(), m.failed.Load()
			s.Expired, s.Dead = m.expired.Load(), m.dead.Load()
		}
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queue < out[j].Queue })
	return out
}

// promoteLocked moves due scheduled jobs to pending and expires overdue leases.
func (b *Broker) promoteLocked() {
	now := b.cfg.Now()
	var changed []*Job
	for b.scheduled.Len() > 0 && !b.scheduled.peek().at.After(now) {
		e := heap.Pop(&b.scheduled).(timeEntry)
		j := b.jobs[e.id]
		if j == nil || j.State != StateScheduled || !j.RunAt.Equal(e.at) {
			continue
		}
		j.State = StatePending
		j.UpdatedAt = now
		b.seq++
		j.seq = b.seq
		b.readyHeap(j.Queue).push(j)
		b.wake(j.Queue)
		changed = append(changed, j)
	}
	for b.leases.Len() > 0 && !b.leases.peek().at.After(now) {
		e := heap.Pop(&b.leases).(timeEntry)
		j := b.jobs[e.id]
		if j == nil || j.State != StateLeased || j.LeaseToken != e.token || !j.LeaseUntil.Equal(e.at) {
			continue // acked, nacked, or extended by a heartbeat
		}
		b.metrics.queue(j.Queue).expired.Add(1)
		b.failLocked(j, "lease expired (worker crashed or timed out)", 0)
		changed = append(changed, j)
	}
	if len(changed) > 0 {
		b.persist(changed...) // nobody waits on these; they are ordered before any later change
	}
}

func (b *Broker) loop() {
	defer close(b.done)
	t := time.NewTicker(b.cfg.TickInterval)
	defer t.Stop()
	var lastPurge time.Time
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
		}
		b.mu.Lock()
		b.promoteLocked()
		b.mu.Unlock()
		if b.cfg.RetainSucceeded > 0 && time.Since(lastPurge) > time.Minute {
			lastPurge = time.Now()
			b.store.deleteFinished(b.cfg.Now().Add(-b.cfg.RetainSucceeded))
		}
	}
}

// Metrics exposes the registry for the HTTP layer.
func (b *Broker) Metrics() *Metrics { return b.metrics }

// readyHeap orders pending jobs by priority (high first), then FIFO.
type readyHeap struct{ items []*Job }

func (h *readyHeap) Len() int { return len(h.items) }
func (h *readyHeap) Less(i, j int) bool {
	a, c := h.items[i], h.items[j]
	if a.Priority != c.Priority {
		return a.Priority > c.Priority
	}
	return a.seq < c.seq
}
func (h *readyHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *readyHeap) Push(x any)    { h.items = append(h.items, x.(*Job)) }
func (h *readyHeap) Pop() any {
	n := len(h.items)
	x := h.items[n-1]
	h.items = h.items[:n-1]
	return x
}
func (h *readyHeap) push(j *Job) { heap.Push(h, j) }
func (h *readyHeap) pop() *Job   { return heap.Pop(h).(*Job) }

// timeEntry is a lazily-invalidated heap entry: when popped it is checked
// against the job's current state and ignored if it no longer applies.
type timeEntry struct {
	at    time.Time
	id    string
	token uint64
}

type timeHeap []timeEntry

func (h timeHeap) Len() int           { return len(h) }
func (h timeHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h timeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timeHeap) Push(x any)        { *h = append(*h, x.(timeEntry)) }
func (h *timeHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
func (h timeHeap) peek() timeEntry { return h[0] }
