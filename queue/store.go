package queue

import (
	"encoding/json"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketJobs = []byte("jobs")
	bucketIdem = []byte("idempotency") // queue + "\x00" + key -> job id
	bucketMeta = []byte("meta")
)

type store struct {
	db *bolt.DB
}

func openStore(path string) (*store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketJobs, bucketIdem, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &store{db: db}, nil
}

func idemKey(queue, key string) []byte { return []byte(queue + "\x00" + key) }

// put writes jobs (and their idempotency keys) in one transaction and one fsync.
func (s *store) put(jobs ...*Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketJobs)
		for _, j := range jobs {
			raw, err := json.Marshal(j)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(j.ID), raw); err != nil {
				return err
			}
			if j.IdempotencyKey != "" {
				if err := tx.Bucket(bucketIdem).Put(idemKey(j.Queue, j.IdempotencyKey), []byte(j.ID)); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *store) get(id string) (*Job, error) {
	var j *Job
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketJobs).Get([]byte(id))
		if raw == nil {
			return nil
		}
		j = &Job{}
		return json.Unmarshal(raw, j)
	})
	return j, err
}

// each visits every stored job in id (= creation) order.
func (s *store) each(fn func(*Job) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketJobs).ForEach(func(_, raw []byte) error {
			var j Job
			if err := json.Unmarshal(raw, &j); err != nil {
				return err
			}
			return fn(&j)
		})
	})
}

func (s *store) idempotent(queue, key string) (string, error) {
	var id string
	err := s.db.View(func(tx *bolt.Tx) error {
		id = string(tx.Bucket(bucketIdem).Get(idemKey(queue, key)))
		return nil
	})
	return id, err
}

// deleteFinished removes succeeded jobs that finished before cutoff.
func (s *store) deleteFinished(cutoff time.Time) (int, error) {
	n := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketJobs)
		var dead [][]byte
		var idem [][]byte
		err := b.ForEach(func(k, raw []byte) error {
			var j Job
			if err := json.Unmarshal(raw, &j); err != nil {
				return err
			}
			if j.State == StateSucceeded && j.FinishedAt.Before(cutoff) {
				dead = append(dead, append([]byte(nil), k...))
				if j.IdempotencyKey != "" {
					idem = append(idem, idemKey(j.Queue, j.IdempotencyKey))
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, k := range dead {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		for _, k := range idem {
			if err := tx.Bucket(bucketIdem).Delete(k); err != nil {
				return err
			}
		}
		n = len(dead)
		return nil
	})
	return n, err
}

func (s *store) close() error { return s.db.Close() }
