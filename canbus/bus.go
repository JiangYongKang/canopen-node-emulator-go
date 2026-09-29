// Package canbus provides an in-process software CAN bus used by the
// CANopen node emulator. No real hardware or external service is required.
//
// The bus broadcasts every accepted frame to all subscribers. Frames are
// assigned a strictly monotonic sequence number which receivers use to
// recognize duplicate and stale frames. A software fault state
// (SetFaulted) simulates bus-off: while faulted, frames are accepted and
// counted but not delivered.
package canbus

import "sync"

// Drop marks a frame returned by an injection hook as discarded.
const Drop uint32 = 0xFFFFFFFF

// Frame is a single CAN data frame (classic CAN, 0..8 data bytes).
type Frame struct {
	ID     uint32
	DLC    byte
	Data   [8]byte
	Source uint8  // logical node id of the sender, 0 for the NMT master
	Seq    uint64 // monotonically increasing sequence number assigned by the bus
}

// HookFunc may inspect, mutate or drop a frame. Return a frame whose ID
// equals Drop to drop it. Hooks run synchronously inside Publish, before
// delivery; they must not call Publish (bus fault control is exercised
// separately via SetFaulted).
type HookFunc func(Frame) Frame

// Bus is the virtual CAN bus.
type Bus struct {
	mu     sync.RWMutex
	subsMu sync.RWMutex

	subs      map[uint64]subscriber
	nextSubID uint64

	hooksMu sync.RWMutex
	hooks   []HookFunc

	muCounters sync.Mutex
	seq        uint64
	published  uint64
	delivered  uint64
	dropped    uint64
	faulted    bool
}

type subscriber struct {
	ch   chan Frame
	full uint64
}

// NewBus creates a virtual bus.
func NewBus() *Bus {
	return &Bus{subs: make(map[uint64]subscriber)}
}

// Subscribe registers a frame receiver. The returned channel is buffered
// (buffer entries) and never blocked by the publisher: a slow consumer
// loses frames instead of stalling the bus.
func (b *Bus) Subscribe(buffer int) (uint64, <-chan Frame) {
	if buffer < 1 {
		buffer = 1
	}
	ch := make(chan Frame, buffer)

	b.mu.Lock()
	id := b.nextSubID
	b.nextSubID++
	b.subs[id] = subscriber{ch: ch}
	b.mu.Unlock()

	return id, ch
}

// Unsubscribe removes a receiver and closes its channel.
func (b *Bus) Unsubscribe(id uint64) {
	b.mu.Lock()
	sub, ok := b.subs[id]
	if ok {
		delete(b.subs, id)
	}
	b.mu.Unlock()
	if ok {
		close(sub.ch)
	}
}

// AddHook appends an injection hook. Hooks are applied in registration
// order; if any hook returns Drop the frame is discarded (and counted).
func (b *Bus) AddHook(h HookFunc) {
	b.hooksMu.Lock()
	b.hooks = append(b.hooks, h)
	b.hooksMu.Unlock()
}

// Publish runs all hooks and, unless the frame was dropped and the bus is
// not faulted, delivers the frame to every subscriber. Delivery to one
// subscriber never affects delivery to the others.
func (b *Bus) Publish(f Frame) {
	// Injection hooks first.
	b.hooksMu.RLock()
	hooks := b.hooks
	b.hooksMu.RUnlock()
	for _, h := range hooks {
		f = h(f)
		if f.ID == Drop {
			b.muCounters.Lock()
			b.dropped++
			b.muCounters.Unlock()
			return
		}
	}

	// Assign sequence order to accepted frames only.
	b.muCounters.Lock()
	b.seq++
	b.published++
	if f.Seq == 0 {
		f.Seq = b.seq
	}
	faulted := b.faulted
	b.muCounters.Unlock()

	if faulted {
		// Bus-off: frame is lost in transit.
		return
	}

	// Hold the subscriber read lock for the whole delivery: Unsubscribe
	// takes the write lock before closing a channel, so it cannot close a
	// channel while we are sending on it. Sends are non-blocking and
	// subscribers never take bus locks, so this cannot deadlock.
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, sub := range b.subs {
		select {
		case sub.ch <- f:
			b.muCounters.Lock()
			b.delivered++
			b.muCounters.Unlock()
		default:
			b.muCounters.Lock()
			b.dropped++
			b.muCounters.Unlock()
		}
	}
}

// SetFaulted toggles the bus-error fault state. While faulted, published
// frames are not delivered, which emulates a physical bus-off.
func (b *Bus) SetFaulted(faulted bool) {
	b.muCounters.Lock()
	b.faulted = faulted
	b.muCounters.Unlock()
}

// Faulted reports the bus-error state.
func (b *Bus) Faulted() bool {
	b.muCounters.Lock()
	defer b.muCounters.Unlock()
	return b.faulted
}

// Frames returns the number of frames accepted for transport.
func (b *Bus) Frames() uint64 {
	b.muCounters.Lock()
	defer b.muCounters.Unlock()
	return b.published
}

// Delivered returns the number of frame copies handed to subscribers.
func (b *Bus) Delivered() uint64 {
	b.muCounters.Lock()
	defer b.muCounters.Unlock()
	return b.delivered
}

// Dropped returns the number of dropped frame copies (hooks or backpressure).
func (b *Bus) Dropped() uint64 {
	b.muCounters.Lock()
	defer b.muCounters.Unlock()
	return b.dropped
}
