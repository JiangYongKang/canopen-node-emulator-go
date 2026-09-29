package canopen

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Frame is a single classical CAN frame (max 8 data bytes).
type Frame struct {
	CANID  uint32
	DLC    int
	Data   [8]byte
	SentAt time.Time
}

// FrameHandler receives frames delivered by the virtual bus.
type FrameHandler func(Frame)

type subscription struct {
	id     int
	owner  uint8 // node ID of the subscriber; 0 = anonymous monitor
	mask   uint32
	filter uint32
	h      FrameHandler
}

// BusStats records what happened to frames published on the bus.
// All fields are read atomically via Stats.
type BusStats struct {
	Sent       int64
	Delivered  int64
	Dropped    int64
	Delayed    int64
	Duplicated int64
	Corrupted  int64
	Reordered  int64
}

// VirtualBus is a software CAN bus. Subscriptions use the hardware-style
// acceptance rule (acceptanceID & mask) == (filter & mask). Delivery is
// synchronous and ordered in the absence of delay/reorder faults, so a given
// subscriber observes frames in the order they were published.
type VirtualBus struct {
	clock Clock
	log   *Logger

	mu        sync.Mutex
	subs      map[int]*subscription
	nextSub   int
	fault     FaultSpec
	faultGate func(FaultSpec, Frame) bool // optional deterministic per-frame fault override
	busOff    bool
	stopCh    chan struct{}
	stopped   bool

	sent       atomic.Int64
	delivered  atomic.Int64
	dropped    atomic.Int64
	delayed    atomic.Int64
	duplicated atomic.Int64
	corrupted  atomic.Int64
	reordered  atomic.Int64
}

// NewVirtualBus creates a bus driven by clock (may be a FakeClock).
func NewVirtualBus(clock Clock) *VirtualBus {
	if clock == nil {
		clock = RealClock{}
	}
	return &VirtualBus{clock: clock, subs: make(map[int]*subscription), stopCh: make(chan struct{})}
}

// withLogger attaches a frame logger.
func (b *VirtualBus) withLogger(l *Logger) *VirtualBus { b.log = l; return b }

// Publish puts an anonymous frame on the bus; it may be delivered back to any
// matching subscriber (used by bus-level producers such as the NMT master).
func (b *VirtualBus) Publish(f Frame) { b.PublishFrom(0, f) }

// PublishFrom publishes a frame on behalf of node fromNode. A frame is never
// delivered back to its sender, mirroring normal CAN controller behaviour.
func (b *VirtualBus) PublishFrom(fromNode uint8, f Frame) {
	if f.DLC < 0 {
		f.DLC = 0
	}
	if f.DLC > 8 {
		f.DLC = 8
	}
	if f.SentAt.IsZero() {
		f.SentAt = b.clock.Now()
	}
	b.sent.Add(1)

	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return
	}
	fault := b.fault
	off := b.busOff
	targets := make([]*subscription, 0, len(b.subs))
	for _, s := range b.subs {
		if s.owner == fromNode && fromNode != 0 {
			continue
		}
		if f.CANID&s.mask == s.filter&s.mask {
			targets = append(targets, s)
		}
	}
	b.mu.Unlock()

	if off {
		b.log.Framef("BUSOFF", f, "bus-off: frame dropped")
		b.dropped.Add(int64(len(targets)))
		return
	}

	for _, s := range targets {
		b.deliverTo(s, f, fault)
	}
}

func (b *VirtualBus) deliverTo(s *subscription, f Frame, fault FaultSpec) {
	// Faults only act on frames matching their target node set.
	if fault.Kind != FaultNone && b.matches(fault, f.CANID) {
		hit := roll(fault.Probability)
		b.mu.Lock()
		gate := b.faultGate
		b.mu.Unlock()
		if gate != nil {
			hit = gate(fault, f)
		}
		if hit {
			switch fault.Kind {
			case FaultDrop:
				b.log.Framef("DROP", f, "injected drop")
				b.dropped.Add(1)
				return
			case FaultCorrupt:
				f = corrupt(f)
				b.corrupted.Add(1)
				b.log.Framef("CRPT", f, "injected corruption")
			case FaultDuplicate:
				// Two identical copies are delivered back to back; it is the
				// receiver's responsibility to be idempotent.
				b.duplicated.Add(1)
				b.dispatch(s, f)
				b.log.Framef("DUP2", f, "injected duplicate")
				b.dispatch(s, f)
				return
			case FaultDelay:
				b.delayed.Add(1)
				b.log.Framef("DLY1", f, "injected delay")
				go b.timedDeliver(s, f, delayOr(fault, time.Millisecond))
				return
			case FaultReorder:
				b.reordered.Add(1)
				b.log.Framef("ORD1", f, "injected reorder hold")
				go b.timedDeliver(s, f, delayOr(fault, time.Millisecond))
				return
			case FaultBusOff:
				b.mu.Lock()
				b.busOff = true
				b.mu.Unlock()
				b.log.Framef("BUSOFF", f, "bus-off induced by fault")
				b.dropped.Add(1)
				return
			}
		}
	}
	b.dispatch(s, f)
}

func (b *VirtualBus) timedDeliver(s *subscription, f Frame, d time.Duration) {
	select {
	case <-b.stopCh:
		return
	case <-b.clock.After(d):
	}
	b.mu.Lock()
	stopped := b.stopped
	b.mu.Unlock()
	if stopped {
		return
	}
	b.log.Framef("LATE", f, "fault-delayed frame now delivered")
	b.dispatch(s, f)
}

// dispatch hands one frame to one handler. Handler panics are contained so a
// buggy subscriber cannot tear down the bus. Delivery happens in the
// publisher goroutine to preserve per-subscriber ordering.
func (b *VirtualBus) dispatch(s *subscription, f Frame) {
	defer func() { _ = recover() }()
	b.delivered.Add(1)
	b.log.Framef("RX  ", f, "delivered to subscriber")
	s.h(f)
}

// nodeOfCOB extracts the producing node id from a COB-ID. CANopen encodes the
// node id in the low 7 bits for most communication objects; the region above
// 0x780 uses 0x80+n offsets, which is normalised here.
func nodeOfCOB(id uint32) uint8 {
	return uint8(id & 0x7F)
}

func (b *VirtualBus) matches(spec FaultSpec, id uint32) bool {
	if len(spec.Targets) == 0 {
		return true
	}
	node := nodeOfCOB(id)
	for _, t := range spec.Targets {
		if t == node {
			return true
		}
	}
	return false
}

// Subscribe registers a frame handler with a hardware-style ID mask.
// owner is the subscribing node (0 for an anonymous monitor).
func (b *VirtualBus) Subscribe(owner uint8, mask, filter uint32, h FrameHandler) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextSub++
	b.subs[b.nextSub] = &subscription{id: b.nextSub, owner: owner, mask: mask, filter: filter, h: h}
	return b.nextSub
}

// Unsubscribe removes a handler.
func (b *VirtualBus) Unsubscribe(id int) {
	b.mu.Lock()
	delete(b.subs, id)
	b.mu.Unlock()
}

// Inject applies a fault to subsequently published, matching frames.
func (b *VirtualBus) Inject(spec FaultSpec) {
	b.mu.Lock()
	b.fault = spec
	if spec.Kind == FaultBusOff {
		b.busOff = true
	}
	b.mu.Unlock()
}

// SetFaultGate installs an optional deterministic predicate that decides, per
// frame, whether the current fault applies. It is intended for tests that need
// reproducible "drop the first N frames, then pass" behaviour. Pass nil to
// revert to probability-based faulting.
func (b *VirtualBus) SetFaultGate(gate func(FaultSpec, Frame) bool) {
	b.mu.Lock()
	b.faultGate = gate
	b.mu.Unlock()
}

// Recover clears the injected fault and exits bus-off state. Frames already
// held by delay/reorder faults are cancelled.
func (b *VirtualBus) Recover() {
	b.mu.Lock()
	b.fault = FaultSpec{}
	b.busOff = false
	if b.stopped {
		b.mu.Unlock()
		return
	}
	close(b.stopCh)
	b.stopCh = make(chan struct{})
	b.mu.Unlock()
}

// Healthy reports whether the bus is not in bus-off state.
func (b *VirtualBus) Healthy() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.busOff
}

// Stop permanently stops the bus and cancels pending delayed frames.
func (b *VirtualBus) Stop() {
	b.mu.Lock()
	if b.stopped {
		b.mu.Unlock()
		return
	}
	b.stopped = true
	close(b.stopCh)
	b.mu.Unlock()
}

// Stats returns a snapshot of bus counters.
func (b *VirtualBus) Stats() BusStats {
	return BusStats{
		Sent:       b.sent.Load(),
		Delivered:  b.delivered.Load(),
		Dropped:    b.dropped.Load(),
		Delayed:    b.delayed.Load(),
		Duplicated: b.duplicated.Load(),
		Corrupted:  b.corrupted.Load(),
		Reordered:  b.reordered.Load(),
	}
}

func roll(p float64) bool {
	if p <= 0 {
		return false
	}
	if p >= 1 {
		return true
	}
	return rand.Float64() < p
}

func delayOr(spec FaultSpec, fallback time.Duration) time.Duration {
	if spec.Delay > 0 {
		return spec.Delay
	}
	return fallback
}

// corrupt deterministically flips the LSB of the first data byte.
func corrupt(f Frame) Frame {
	if f.DLC > 0 {
		f.Data[0] ^= 0x01
	}
	return f
}
