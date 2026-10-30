# Durable Job Queue

A background job queue in Go with at-least-once delivery, in the spirit of Sidekiq, Celery or SQS, packaged as one small binary. Producers enqueue jobs over HTTP. Workers lease them, run them and ack or nack. The broker handles retries, backoff, dead-lettering and crash recovery.

It uses no external services: state lives in an embedded bbolt file, and every change is on disk before the API replies.

## Features

- **Leases with fencing tokens.** A leased job belongs to one worker until the lease runs out. Each lease carries a fresh token, and ack/nack must present it. A worker that stalled past its lease (GC pause, network partition) gets `409 Conflict` instead of overwriting the result of the worker that took over.
- **Heartbeats** extend the lease for long jobs. The Go SDK sends them automatically.
- **Retries with exponential backoff and full jitter.** A handler can also ask for a specific delay, for example after a 429.
- **Dead letter queue** once `max_attempts` is used up, with an endpoint to inspect dead jobs and requeue them.
- **Priorities** (higher first, FIFO within a priority) and **delayed jobs**.
- **Idempotency keys** per queue, via a body field or an `Idempotency-Key` header, so a producer can retry an enqueue safely.
- **Long polling**: an idle worker's lease request waits until a job arrives, so there is no busy polling.
- **Group commit**: concurrent requests share one transaction and one fsync, which is what makes durable-before-reply affordable (numbers below).
- **Crash recovery**: on restart the in-memory indexes are rebuilt from disk. Leases that were outstanding simply expire, and those jobs are delivered again.
- **Prometheus metrics** at `/metrics`: counters per queue, queue depth by state, and a histogram of job run time.

## How it works

```
             enqueue                          lease (long poll)
 producer ───────────> ┌──────────────────────────────┐ <─────────── worker
                       │ broker (one mutex)           │
                       │  ready heap per queue        │ ───────────> job + token
                       │  scheduled heap (run_at)     │
                       │  lease heap (lease_until)    │ <─────────── ack / nack / heartbeat
                       └──────────────┬───────────────┘               (token checked)
                                      │ append snapshot, get ticket
                                      v
                       group-commit writer ── 1 bbolt txn + fsync per batch ──> jobq.db
                       (reply is sent only when the caller's ticket is durable)
```

State changes happen in memory under one lock and are appended to the commit queue in the same order, so the disk never sees them out of order. The timer heaps use lazy invalidation: an entry is checked against the job's current state when it pops, which makes heartbeats and acks O(log n) with no searching.

## Numbers

`jobq bench` runs the server in-process and drives it over real HTTP on loopback (Windows 11 laptop, NVMe SSD):

| Workload | Throughput |
|---|---|
| Enqueue, 1 producer, durable before reply | 400 jobs/s (one fsync per job) |
| Enqueue, 16 producers, durable before reply | 3,200 jobs/s (group commit, about 8x) |
| Lease + ack, 32 concurrent handlers | 2,700 jobs/s |

## Quick start

```bash
go build -o jobq ./cmd/jobq
./jobq serve -addr :8080 -db ./jobq.db

# enqueue
curl -X POST localhost:8080/v1/queues/emails/jobs \
  -H 'Idempotency-Key: welcome-user-42' \
  -d '{"payload": {"to": "a@example.com"}, "priority": 5, "max_attempts": 5}'

# run the demo worker (fails 20% of attempts to show retries)
./jobq worker -url http://localhost:8080 -queue emails

curl localhost:8080/v1/stats
curl localhost:8080/v1/queues/emails/dead
curl localhost:8080/metrics
```

Or with Docker:

```bash
docker build -t jobq .
docker run -p 8080:8080 -v jobq-data:/data jobq
```

## Go SDK

```go
c := client.New("http://localhost:8080")
c.Enqueue(ctx, "emails", Email{To: "a@example.com"}, client.EnqueueOptions{IdempotencyKey: "welcome-42"})

c.Work(ctx, "emails", func(ctx context.Context, j *queue.Job) (any, error) {
    var e Email
    json.Unmarshal(j.Payload, &e)
    if err := send(ctx, e); err != nil {
        return nil, err // nacked, retried with backoff
    }
    return map[string]string{"status": "sent"}, nil // acked
}, client.WorkerOptions{Concurrency: 8, Lease: 30 * time.Second})
```

`Work` leases in batches up to the free concurrency, renews leases at a third of their length, turns handler panics into nacks, and on shutdown finishes in-flight jobs before returning.

## API

| Method and path | Body | Notes |
|---|---|---|
| `POST /v1/queues/{q}/jobs` | `payload, priority, delay_ms, max_attempts, idempotency_key` | 201 created, or 200 with the existing job for a repeated key |
| `POST /v1/queues/{q}/lease` | `worker, n, lease_ms, wait_ms` | returns `{"leases": [{"job": ..., "token": N}]}` |
| `POST /v1/jobs/{id}/ack` | `token, result` | 409 if the lease was lost |
| `POST /v1/jobs/{id}/nack` | `token, error, retry_after_ms` | retry or dead-letter |
| `POST /v1/jobs/{id}/heartbeat` | `token, extend_ms` | |
| `POST /v1/jobs/{id}/requeue` | | dead job back to pending, attempts reset |
| `GET /v1/jobs/{id}` | | |
| `GET /v1/queues/{q}/dead?limit=` | | |
| `GET /v1/stats`, `GET /metrics`, `GET /healthz` | | |

## Tests

```bash
go test -race ./...
```

The broker tests use a fake clock, so lease expiry and backoff are deterministic. They cover priority and FIFO order, ack, redelivery after an expired lease with the stale worker fenced off, heartbeats, retries into the dead letter queue and back, delayed jobs, idempotency, restart recovery (succeeded, in-flight, pending and delayed jobs all come back in the right state), long-poll wakeup, and 16 concurrent workers draining 2,000 jobs with every job acked exactly once. The end-to-end test runs the HTTP server with three SDK workers whose handlers fail, rate-limit, panic, or hit a poison message, and checks that every good job succeeds, the poison job is dead-lettered after exactly 3 attempts, and the handler ran exactly the expected number of times.

## Trade-offs

This is a single-node broker: one process owns the queue, and durability comes from fsync, not replication. Delivery is at-least-once, so handlers should be idempotent (a crash after the work but before the ack causes a redelivery). Dead-letter listing scans the store, which is fine for an admin endpoint but not for millions of dead jobs.
