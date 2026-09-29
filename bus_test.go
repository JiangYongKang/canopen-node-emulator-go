package canopen

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBusMaskFilterAndLoopback(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	var gotNMT, gotHB int64
	mu := sync.Mutex{}
	frames := map[int]int{}
	// NMT master frame 0x000
	bus.Subscribe(0, 0x7FF, 0x000, func(f Frame) {
		mu.Lock()
		frames[int(f.CANID)]++
		mu.Unlock()
		atomic.AddInt64(&gotNMT, 1)
	})
	// node 1 listens to its own heartbeat COB 0x701 but must not get loopback
	bus.Subscribe(1, 0x7FF, 0x701, func(f Frame) { atomic.AddInt64(&gotHB, 1) })

	bus.Publish(Frame{CANID: 0x000, DLC: 2})
	bus.PublishFrom(1, Frame{CANID: 0x701, DLC: 1, Data: [8]byte{0x05}})

	if atomic.LoadInt64(&gotNMT) != 1 {
		t.Fatalf("NMT frames = %d, want 1", gotNMT)
	}
	if atomic.LoadInt64(&gotHB) != 0 {
		t.Fatalf("loopback heartbeat = %d, want 0", gotHB)
	}
}

func TestBusDropFault(t *testing.T) {
	c := NewFakeClock()
	bus := NewVirtualBus(c)
	var n int64
	bus.Subscribe(0, 0x7FF, 0x201, func(f Frame) { atomic.AddInt64(&n, 1) })
	bus.Inject(FaultSpec{Kind: FaultDrop, Probability: 1})
	bus.PublishFrom(2, Frame{CANID: 0x201, DLC: 8})
	if atomic.LoadInt64(&n) != 0 {
		t.Fatalf("delivered despite drop fault")
	}
	bus.Recover()
	bus.PublishFrom(2, Frame{CANID: 0x201, DLC: 8})
	if !waitFor(func() bool { return atomic.LoadInt64(&n) == 1 }) {
		t.Fatal("frame after recovery not delivered")
	}
	st := bus.Stats()
	if st.Dropped != 1 {
		t.Fatalf("dropped = %d, want 1", st.Dropped)
	}
}

func TestBusDuplicateAndCorrupt(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	var n int32
	var first [8]byte
	bus.Subscribe(0, 0x7FF, 0x181, func(f Frame) {
		atomic.AddInt32(&n, 1)
		first = f.Data
	})
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 1, Data: [8]byte{0xAA}})
	if n != 1 {
		t.Fatalf("normal publish count = %d", n)
	}
	bus.Inject(FaultSpec{Kind: FaultDuplicate, Probability: 1})
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 1, Data: [8]byte{0xAA}})
	if n != 3 {
		t.Fatalf("duplicate count = %d, want 3", n)
	}
	bus.Recover()

	bus.Inject(FaultSpec{Kind: FaultCorrupt, Probability: 1})
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 1, Data: [8]byte{0xAA}})
	if first[0] != 0xAB {
		t.Fatalf("corrupted byte = %#x, want 0xAB", first[0])
	}
}

func TestBusTargetFiltering(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	var n1, n2 int64
	bus.Subscribe(0, 0x700, 0x700, func(f Frame) { atomic.AddInt64(&n1, 1) })
	bus.Subscribe(0, 0x700, 0x700, func(f Frame) { atomic.AddInt64(&n2, 1) })
	// only target node 2's COB-IDs
	bus.Inject(FaultSpec{Kind: FaultDrop, Probability: 1, Targets: []uint8{2}})
	bus.PublishFrom(1, Frame{CANID: 0x701, DLC: 1}) // node 1: passes
	bus.PublishFrom(2, Frame{CANID: 0x702, DLC: 1}) // node 2: dropped
	if n1 != 1 || n2 != 1 {
		t.Fatalf("n1=%d n2=%d, want 1/1", n1, n2)
	}
}

var _ = time.Millisecond
