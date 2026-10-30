// Package queue is a durable job broker with at-least-once delivery.
//
// A job moves through these states:
//
//	scheduled --(run_at reached)--> pending --(lease)--> leased --(ack)--> succeeded
//	                                   ^                    |
//	                                   |                    +--(nack or lease expiry)--> scheduled (backoff)
//	                                   |                    +--(attempts exhausted)----> dead
//	                                   +------------(requeue from dead letter)-----------+
//
// Every transition is written to disk before it is acknowledged to the caller,
// so a broker restart loses nothing; leases that were outstanding when it went
// down simply expire and the jobs are delivered again.
package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type State string

const (
	StateScheduled State = "scheduled"
	StatePending   State = "pending"
	StateLeased    State = "leased"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead"
)

// Job is the stored record. Fields are exported for JSON.
type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	State          State           `json:"state"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LeasedAt       time.Time       `json:"leased_at,omitzero"`
	LeaseUntil     time.Time       `json:"lease_until,omitzero"`
	LeasedBy       string          `json:"leased_by,omitempty"`
	LeaseToken     uint64          `json:"lease_token,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	FinishedAt     time.Time       `json:"finished_at,omitzero"`

	seq uint64 // FIFO tie-break among equal priorities; not persisted
}

func (j *Job) clone() *Job {
	c := *j
	return &c
}

// newID returns a time-ordered, unique id: 12 hex chars of milliseconds, then randomness.
func newID(now time.Time) string {
	var r [6]byte
	rand.Read(r[:])
	return fmt.Sprintf("%012x%s", now.UnixMilli(), hex.EncodeToString(r[:]))
}

// EnqueueRequest describes a new job.
type EnqueueRequest struct {
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`        // higher runs first
	Delay          time.Duration   `json:"delay"`           // run no earlier than now+delay
	MaxAttempts    int             `json:"max_attempts"`    // default 5
	IdempotencyKey string          `json:"idempotency_key"` // same key in the same queue returns the existing job
}

// Lease is what a worker gets back: the job plus the token it must present to ack or nack.
type Lease struct {
	Job   *Job   `json:"job"`
	Token uint64 `json:"token"`
}
