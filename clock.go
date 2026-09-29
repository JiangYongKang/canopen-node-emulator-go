package canopen

import (
	"sort"
	"sync"
	"time"
)

// Clock abstracts wall-clock time so timing-sensitive behaviour (heartbeat
// timeouts, PDO periods, SDO timeouts) can be driven deterministically.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
	NewTicker(d time.Duration) Ticker
}

// Ticker is a repeating timer.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// RealClock is backed by the standard library wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (RealClock) NewTicker(d time.Duration) Ticker       { return &realTicker{t: time.NewTicker(d)} }

type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }
func (r *realTicker) Stop()               { r.t.Stop() }

type fakeTimer struct {
	id int
	at time.Time
	ch chan time.Time
}

type fakeTicker struct {
	clock  *FakeClock
	period time.Duration
	ch     chan time.Time
	id     int
	next   time.Time
}

func (t *fakeTicker) C() <-chan time.Time { return t.ch }
func (t *fakeTicker) Stop()               { t.clock.removeTicker(t.id) }

// FakeClock is a manually advanced, deterministic clock. All channels have
// capacity 1, so Advance never blocks on an unscheduled consumer. Repeating
// tickers are re-armed by the clock itself and coalesce missed ticks, which
// keeps periodic loops robust regardless of goroutine scheduling.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []fakeTimer
	tickers []*fakeTicker
	nextID  int
}

// NewFakeClock returns a fake clock at a fixed, reproducible instant.
func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that fires once the clock advances by at least d.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if d <= 0 {
		ch <- c.now
		return ch
	}
	c.nextID++
	c.waiters = append(c.waiters, fakeTimer{id: c.nextID, at: c.now.Add(d), ch: ch})
	return ch
}

// Sleep blocks until the clock advances by at least d.
func (c *FakeClock) Sleep(d time.Duration) { <-c.After(d) }

// NewTicker returns a repeating fake ticker.
func (c *FakeClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		d = time.Nanosecond
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextID++
	tk := &fakeTicker{clock: c, period: d, ch: make(chan time.Time, 1), id: c.nextID, next: c.now.Add(d)}
	c.tickers = append(c.tickers, tk)
	return tk
}

func (c *FakeClock) removeTicker(id int) {
	c.mu.Lock()
	for i, t := range c.tickers {
		if t.id == id {
			c.tickers = append(c.tickers[:i], c.tickers[i+1:]...)
			break
		}
	}
	c.mu.Unlock()
}

// Advance moves the clock forward by d and fires due one-shot timers and
// ticker periods.
func (c *FakeClock) Advance(d time.Duration) {
	if d <= 0 {
		return
	}
	c.mu.Lock()
	c.now = c.now.Add(d)
	deadline := c.now

	var fire []fakeTimer
	kept := c.waiters[:0]
	for _, t := range c.waiters {
		if !t.at.After(deadline) {
			fire = append(fire, t)
		} else {
			kept = append(kept, t)
		}
	}
	c.waiters = kept
	sort.SliceStable(fire, func(i, j int) bool {
		if fire[i].at.Equal(fire[j].at) {
			return fire[i].id < fire[j].id
		}
		return fire[i].at.Before(fire[j].at)
	})

	type tick struct{ ch chan time.Time }
	var tickers []tick
	for _, tk := range c.tickers {
		if tk.next.IsZero() {
			tk.next = deadline
		}
		if !tk.next.After(deadline) {
			tickers = append(tickers, tick{tk.ch})
			for !tk.next.After(deadline) {
				tk.next = tk.next.Add(tk.period)
			}
		}
	}
	c.mu.Unlock()

	for _, t := range fire {
		select {
		case t.ch <- t.at:
		default:
		}
	}
	for _, tk := range tickers {
		select {
		case tk.ch <- deadline:
		default:
		}
	}
}

// pendingCount reports registered one-shot timers.
func (c *FakeClock) pendingCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

// activeCount reports one-shot timers plus live tickers, which is the right
// predicate for waiting until periodic loops have installed their timers.
func (c *FakeClock) activeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters) + len(c.tickers)
}
