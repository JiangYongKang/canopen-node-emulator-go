package canopen

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestFakeClockOrdering(t *testing.T) {
	c := NewFakeClock()
	c2 := c.After(3 * time.Millisecond)
	c1 := c.After(1 * time.Millisecond)
	c3 := c.After(2 * time.Millisecond)
	c.Advance(5 * time.Millisecond)
	got := []int{atoi(<-c1), atoi(<-c3), atoi(<-c2)}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("order = %v", got)
	}
}

func atoi(ts time.Time) int {
	return int(ts.Sub(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)).Milliseconds())
}

// waitFor yields until cond becomes true.
func waitFor(cond func() bool) bool {
	for i := 0; i < 1000000; i++ {
		if cond() {
			return true
		}
		runtime.Gosched()
	}
	return cond()
}

func TestFakeClockPeriodicLoop(t *testing.T) {
	c := NewFakeClock()
	var ticks int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-c.After(10 * time.Millisecond):
				atomic.AddInt64(&ticks, 1)
			}
		}
	}()
	for i := int64(1); i <= 5; i++ {
		if !waitFor(func() bool { return c.pendingCount() == 1 }) {
			t.Fatalf("worker did not register timer at iter %d", i)
		}
		c.Advance(10 * time.Millisecond)
		if !waitFor(func() bool { return atomic.LoadInt64(&ticks) == i }) {
			t.Fatalf("tick %d not observed, got %d", i, atomic.LoadInt64(&ticks))
		}
	}
	close(stop)
}
