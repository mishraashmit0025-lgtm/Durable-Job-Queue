package queue

import (
	"math/rand/v2"
	"time"
)

// Backoff is exponential with "full jitter" (AWS architecture blog, 2015): the
// delay before attempt n+1 is uniform in [0, min(Max, Base*2^n)). Jitter spreads
// out retries from many jobs that failed together, e.g. during an outage.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

func (b Backoff) Delay(attempt int) time.Duration {
	ceil := b.Base
	for i := 1; i < attempt && ceil < b.Max; i++ {
		ceil *= 2
	}
	ceil = min(ceil, b.Max)
	if ceil <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(ceil)))
}
