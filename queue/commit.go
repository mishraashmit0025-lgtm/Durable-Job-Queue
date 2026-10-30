package queue

import "sync"

// committer is a group-commit writer. Callers append job snapshots under the
// broker lock (which fixes their order) and get a ticket; a single goroutine
// writes everything appended so far in one transaction and one fsync, then
// releases every ticket it covered. Under load this turns hundreds of fsyncs
// into one.
type committer struct {
	st *store

	mu       sync.Mutex
	cond     *sync.Cond
	pending  []*Job
	appended uint64 // ticket of the last append
	durable  uint64 // highest ticket on disk
	err      error
	closed   bool
	done     chan struct{}
}

func newCommitter(st *store) *committer {
	c := &committer{st: st, done: make(chan struct{})}
	c.cond = sync.NewCond(&c.mu)
	go c.run()
	return c
}

func (c *committer) append(jobs []*Job) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = append(c.pending, jobs...)
	c.appended++
	c.cond.Broadcast()
	return c.appended
}

// wait blocks until ticket is durable and returns the first write error, if any.
func (c *committer) wait(ticket uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.durable < ticket && c.err == nil && !c.closed {
		c.cond.Wait()
	}
	if c.err != nil {
		return c.err
	}
	if c.durable < ticket {
		return ErrClosed
	}
	return nil
}

func (c *committer) run() {
	defer close(c.done)
	c.mu.Lock()
	for {
		for len(c.pending) == 0 && !c.closed {
			c.cond.Wait()
		}
		if len(c.pending) == 0 && c.closed {
			c.mu.Unlock()
			return
		}
		batch, upTo := c.pending, c.appended
		c.pending = nil
		c.mu.Unlock()

		// later snapshots of the same job overwrite earlier ones within the batch
		err := c.st.put(batch...)

		c.mu.Lock()
		if err != nil && c.err == nil {
			c.err = err
		}
		c.durable = upTo
		c.cond.Broadcast()
	}
}

// close flushes whatever is pending and stops the writer.
func (c *committer) close() {
	c.mu.Lock()
	c.closed = true
	c.cond.Broadcast()
	c.mu.Unlock()
	<-c.done
}
