package queue

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is a tiny Prometheus-compatible registry: per-queue counters plus a
// histogram of how long jobs ran between lease and ack.
type Metrics struct {
	mu     sync.Mutex
	queues map[string]*queueMetrics
}

var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

type queueMetrics struct {
	enqueued, leased, succeeded, failed, expired, dead atomic.Uint64

	hmu     sync.Mutex
	buckets []uint64
	sum     float64
	count   uint64
}

func newMetrics() *Metrics { return &Metrics{queues: map[string]*queueMetrics{}} }

func (m *Metrics) queue(name string) *queueMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.queues[name]
	if q == nil {
		q = &queueMetrics{buckets: make([]uint64, len(durationBuckets))}
		m.queues[name] = q
	}
	return q
}

func (q *queueMetrics) observe(d time.Duration) {
	s := d.Seconds()
	q.hmu.Lock()
	defer q.hmu.Unlock()
	for i, ub := range durationBuckets {
		if s <= ub {
			q.buckets[i]++
		}
	}
	q.sum += s
	q.count++
}

// WritePrometheus writes the text exposition format. depth reports current
// queue sizes, which the broker computes at scrape time.
func (m *Metrics) WritePrometheus(w io.Writer, depth []QueueStats) {
	m.mu.Lock()
	names := make([]string, 0, len(m.queues))
	for n := range m.queues {
		names = append(names, n)
	}
	m.mu.Unlock()
	sort.Strings(names)

	counter := func(name, help string, get func(*queueMetrics) uint64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
		for _, n := range names {
			fmt.Fprintf(w, "%s{queue=%q} %d\n", name, n, get(m.queue(n)))
		}
	}
	counter("jobq_jobs_enqueued_total", "Jobs enqueued.", func(q *queueMetrics) uint64 { return q.enqueued.Load() })
	counter("jobq_jobs_leased_total", "Deliveries to workers (includes redeliveries).", func(q *queueMetrics) uint64 { return q.leased.Load() })
	counter("jobq_jobs_succeeded_total", "Jobs acked.", func(q *queueMetrics) uint64 { return q.succeeded.Load() })
	counter("jobq_attempts_failed_total", "Attempts nacked by workers.", func(q *queueMetrics) uint64 { return q.failed.Load() })
	counter("jobq_leases_expired_total", "Leases that ran out without ack or nack.", func(q *queueMetrics) uint64 { return q.expired.Load() })
	counter("jobq_jobs_dead_total", "Jobs moved to the dead letter queue.", func(q *queueMetrics) uint64 { return q.dead.Load() })

	fmt.Fprintf(w, "# HELP jobq_queue_depth Jobs currently in each state.\n# TYPE jobq_queue_depth gauge\n")
	for _, s := range depth {
		fmt.Fprintf(w, "jobq_queue_depth{queue=%q,state=\"pending\"} %d\n", s.Queue, s.Pending)
		fmt.Fprintf(w, "jobq_queue_depth{queue=%q,state=\"scheduled\"} %d\n", s.Queue, s.Scheduled)
		fmt.Fprintf(w, "jobq_queue_depth{queue=%q,state=\"leased\"} %d\n", s.Queue, s.Leased)
	}

	fmt.Fprintf(w, "# HELP jobq_job_duration_seconds Time from lease to ack.\n# TYPE jobq_job_duration_seconds histogram\n")
	for _, n := range names {
		q := m.queue(n)
		q.hmu.Lock()
		for i, ub := range durationBuckets {
			fmt.Fprintf(w, "jobq_job_duration_seconds_bucket{queue=%q,le=\"%g\"} %d\n", n, ub, q.buckets[i])
		}
		fmt.Fprintf(w, "jobq_job_duration_seconds_bucket{queue=%q,le=\"+Inf\"} %d\n", n, q.count)
		fmt.Fprintf(w, "jobq_job_duration_seconds_sum{queue=%q} %g\n", n, q.sum)
		fmt.Fprintf(w, "jobq_job_duration_seconds_count{queue=%q} %d\n", n, q.count)
		q.hmu.Unlock()
	}
}
