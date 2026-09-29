package nmt

import (
	"context"
	"sync"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
)

// Monitor consumes heartbeat frames and detects producer timeouts.
//
// Rules:
//   - Only COB-IDs 0x701..0x77F and exactly one data byte are accepted;
//     everything else is ignored (address/DLC filtering).
//   - Only node ids given to NewMonitor are tracked.
//   - Every accepted heartbeat refreshes the deadline, including exact
//     duplicates or late beats: a valid heartbeat never raises an error.
//   - Timeout is evaluated by Check/Run and is edge-triggered: exactly
//     one offline event on the online->offline edge, exactly one online
//     event when a heartbeat arrives again afterwards (recovery).
//   - A node starts offline; its first heartbeat brings it online.
type Monitor struct {
	bus     *canbus.Bus
	timeout time.Duration
	tick    time.Duration
	now     func() time.Time
	onEvent func(MonitorEvent)

	mu    sync.Mutex
	nodes map[uint8]*MonitorStatus
	subID uint64
}

// MonitorEvent describes an edge (online/offline) transition.
type MonitorEvent struct {
	ID     uint8
	State  State
	Online bool
}

// NewMonitor creates a heartbeat consumer for the given node ids. The
// evaluation interval defaults to timeout/4 and can be tuned by tests.
func NewMonitor(bus *canbus.Bus, timeout time.Duration, ids ...uint8) *Monitor {
	m := &Monitor{
		bus:     bus,
		timeout: timeout,
		tick:    timeout / 4,
		now:     time.Now,
		nodes:   make(map[uint8]*MonitorStatus),
	}
	if m.tick <= 0 {
		m.tick = time.Millisecond
	}
	for _, id := range ids {
		m.nodes[id] = &MonitorStatus{ID: id, State: StateOffline, Online: false}
	}
	return m
}

// SetEventCallback registers a callback fired on online/offline edges.
// Must be called before Run.
func (m *Monitor) SetEventCallback(fn func(MonitorEvent)) {
	m.mu.Lock()
	m.onEvent = fn
	m.mu.Unlock()
}

// setNow injects a clock (tests).
func (m *Monitor) setNow(fn func() time.Time) {
	m.mu.Lock()
	m.now = fn
	m.mu.Unlock()
}

func validHeartbeatState(b byte) (State, bool) {
	switch State(b) {
	case StateInitialising, StateStopped, StateOperational, StatePreOperational:
		return State(b), true
	default:
		return 0, false
	}
}

// Feed processes one heartbeat frame. It returns true when the frame was
// accepted. Invalid frames (wrong COB, unknown node, bad DLC/state) are
// discarded silently and never alter deadlines or produce events.
func (m *Monitor) Feed(f canbus.Frame) bool {
	if f.ID < 0x701 || f.ID > 0x77F {
		return false
	}
	if f.DLC != 1 {
		return false
	}
	id := uint8(f.ID - 0x700)
	st, ok := validHeartbeatState(f.Data[0])
	if !ok {
		return false
	}

	m.mu.Lock()
	now := m.now()
	node, tracked := m.nodes[id]
	if !tracked {
		m.mu.Unlock()
		return false
	}

	wasOnline := node.Online
	node.State = st
	node.LastSeen = now
	node.Deadline = now.Add(m.timeout)
	node.Misses = 0
	node.Online = true
	cb := m.onEvent
	m.mu.Unlock()

	if !wasOnline && cb != nil {
		cb(MonitorEvent{ID: id, State: st, Online: true})
	}
	return true
}

// Check evaluates timeouts at time now. It returns edge events and marks
// nodes offline exactly once; repeated checks while a node stays silent
// do not re-emit and do not corrupt the state of other nodes.
func (m *Monitor) Check(now time.Time) []MonitorEvent {
	var events []MonitorEvent
	m.mu.Lock()
	for _, node := range m.nodes {
		if !node.Online {
			continue
		}
		if now.Before(node.Deadline) {
			continue
		}
		node.Online = false
		node.Misses++
		node.State = StateOffline
		events = append(events, MonitorEvent{ID: node.ID, State: StateOffline, Online: false})
	}
	cbs := append([]MonitorEvent(nil), events...)
	m.mu.Unlock()

	for _, ev := range cbs {
		if m.onEvent != nil {
			m.onEvent(ev)
		}
	}
	return events
}

// Status returns a snapshot for one node.
func (m *Monitor) Status(id uint8) (MonitorStatus, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, ok := m.nodes[id]
	if !ok {
		return MonitorStatus{}, false
	}
	return *node, true
}

// Run subscribes to the bus, feeds frames and periodically evaluates
// timeouts until ctx ends.
func (m *Monitor) Run(ctx context.Context) error {
	id, ch := m.bus.Subscribe(256)
	m.subID = id
	defer m.bus.Unsubscribe(id)

	t := time.NewTicker(m.tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-ch:
			if !ok {
				return nil
			}
			m.Feed(f)
		case now := <-t.C:
			m.Check(now)
		}
	}
}

// Stop releases the subscription.
func (m *Monitor) Stop() {
	if m.subID != 0 {
		m.bus.Unsubscribe(m.subID)
		m.subID = 0
	}
}
