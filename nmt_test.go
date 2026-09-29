package canopen

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNMTTransitions(t *testing.T) {
	m := NewNMTMachine(1)
	if m.State() != StateInitialising {
		t.Fatal("must start initialising")
	}
	// illegal directly to operational
	if _, ok := m.Transition(StateOperational); ok {
		t.Fatal("INIT->OP must be illegal")
	}
	if m.State() != StateInitialising {
		t.Fatal("illegal transition changed state")
	}
	m.Transition(StatePreOperational)
	m.Transition(StateOperational)
	m.Transition(StateStopped)
	if m.State() != StateStopped {
		t.Fatal("not stopped")
	}
	// reset command
	s, ok := m.ApplyCommand(CmdResetCommunication)
	if !ok || s != StatePreOperational {
		t.Fatalf("reset result = %s %v", s, ok)
	}
	h := m.History()
	if len(h) != 5 { // init->pre, pre->op, op->stop, stop->init, init->pre
		t.Fatalf("history len = %d: %+v", len(h), h)
	}
	// unknown command
	if _, ok := m.ApplyCommand(NMTCommand(77)); ok {
		t.Fatal("unknown command must fail")
	}
}

func TestNMTConcurrentNoRollback(t *testing.T) {
	m := NewNMTMachine(2)
	m.Transition(StatePreOperational)
	var wg sync.WaitGroup
	// Hammer operational/pre-op transitions concurrently; state must always
	// be a legal state and history append-only/no torn entries.
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				m.Transition(StateOperational)
			}
		}()
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				m.Transition(StatePreOperational)
			}
		}()
	}
	wg.Wait()
	s := m.State()
	if s != StateOperational && s != StatePreOperational {
		t.Fatalf("illegal final state %s", s)
	}
	prev := NMTState(StateInitialising)
	for _, tr := range m.History() {
		if !legalTransition(prev, tr.To) {
			t.Fatalf("history contains illegal step %s->%s", prev, tr.To)
		}
		prev = tr.To
	}
}

func hbFrame(node uint8, state NMTState) Frame {
	return Frame{CANID: 0x700 + uint32(node), DLC: 1, Data: [8]byte{byte(state)}}
}

func stepClock(c *FakeClock, d time.Duration) {
	if !waitFor(func() bool { return c.activeCount() >= 1 }) {
		panic("no timer registered")
	}
	c.Advance(d)
}

func TestHeartbeatTimeoutAndRecovery(t *testing.T) {
	c := NewFakeClock()
	bus := NewVirtualBus(c)
	var mu sync.Mutex
	var events []MonitorEvent
	mon := NewHeartbeatMonitor(bus, 5, 100*time.Millisecond, c, func(ev MonitorEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})
	mon.Start()
	defer mon.Stop()
	stepClock(c, 25*time.Millisecond) // let loop install its timer

	// one heartbeat -> alive
	bus.Publish(hbFrame(5, StateOperational))
	if !waitFor(func() bool { return mon.Alive() }) {
		t.Fatal("monitor never became alive")
	}
	// timeout: step 5x25ms = 125ms > 100ms
	for i := 0; i < 5; i++ {
		stepClock(c, 25*time.Millisecond)
	}
	if !waitFor(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return countKind(events, EvTimeout) == 1
	}) {
		t.Fatal("timeout not detected exactly once")
	}
	if mon.Alive() {
		t.Fatal("alive after timeout")
	}
	// further time advances must not emit a second timeout
	for i := 0; i < 4; i++ {
		stepClock(c, 25*time.Millisecond)
	}
	mu.Lock()
	if countKind(events, EvTimeout) != 1 {
		mu.Unlock()
		t.Fatal("timeout emitted more than once")
	}
	mu.Unlock()

	// late heartbeat -> recovery (not error), alive again
	bus.Publish(hbFrame(5, StateOperational))
	if !waitFor(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return countKind(events, EvRecovered) == 1
	}) {
		t.Fatal("late heartbeat did not produce recovery")
	}
	if !mon.Alive() {
		t.Fatal("not alive after late heartbeat")
	}
}

func TestHeartbeatDuplicatesNeverError(t *testing.T) {
	c := NewFakeClock()
	bus := NewVirtualBus(c)
	var timeouts int64
	var beats int64
	mon := NewHeartbeatMonitor(bus, 7, 100*time.Millisecond, c, func(ev MonitorEvent) {
		switch ev.Kind {
		case EvTimeout:
			atomic.AddInt64(&timeouts, 1)
		case EvHeartbeat:
			atomic.AddInt64(&beats, 1)
		}
	})
	mon.Start()
	defer mon.Stop()
	stepClock(c, 25*time.Millisecond)
	// burst of 50 duplicate heartbeats with no clock advance
	for i := 0; i < 50; i++ {
		bus.Publish(hbFrame(7, StatePreOperational))
	}
	if atomic.LoadInt64(&timeouts) != 0 {
		t.Fatal("duplicate heartbeats produced a timeout")
	}
	if !mon.Alive() {
		t.Fatal("not alive after duplicate burst")
	}
	// another repeated burst before timeout (50ms)
	for i := 0; i < 2; i++ {
		stepClock(c, 25*time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		bus.Publish(hbFrame(7, StatePreOperational))
	}
	// advance past the point where timeout WOULD have been without the
	// refreshed heartbeats (120ms total)
	for i := 0; i < 4; i++ {
		stepClock(c, 25*time.Millisecond)
	}
	if atomic.LoadInt64(&timeouts) != 0 {
		t.Fatal("repeated in-time heartbeats produced a timeout")
	}
}

func countKind(evs []MonitorEvent, k MonitorKind) int {
	n := 0
	for _, e := range evs {
		if e.Kind == k {
			n++
		}
	}
	return n
}
