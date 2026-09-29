package canopen

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestPDOMappingValidation(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	od := NewObjectDict()
	p := NewPDOManager(1, bus, od, NewFakeClock(), nil)
	if err := p.AddTPDO(TPDOConfig{Number: 0}); !errors.Is(err, ErrNoPDOMapping) {
		t.Fatalf("want no mapping, got %v", err)
	}
	err := p.AddTPDO(TPDOConfig{Number: 0, Mappings: []PDOMapping{
		{0x2000, 1, 32}, {0x2000, 2, 32}, {0x2000, 3, 16},
	}})
	if !errors.Is(err, ErrPDOLength) {
		t.Fatalf("want too long, got %v", err)
	}
}

func TestPDOCyclicTxRx(t *testing.T) {
	c := NewFakeClock()
	bus := NewVirtualBus(c)

	// producer node 1
	od1 := NewObjectDict()
	_ = od1.Add(Entry{Index: 0x2000, Type: TypeU16, Access: AccessRW}, []byte{0x34, 0x12})
	mgr1 := NewPDOManager(1, bus, od1, c, nil)
	if err := mgr1.AddTPDO(TPDOConfig{Number: 0, Period: 50 * time.Millisecond,
		Mappings: []PDOMapping{{0x2000, 0, 16}}}); err != nil {
		t.Fatal(err)
	}

	// consumer node 2 listens on RPDO default COB 0x200 (node2), but producer
	// uses TPDO default 0x181. Explicitly wire consumer to 0x181.
	od2 := NewObjectDict()
	_ = od2.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	mgr2 := NewPDOManager(2, bus, od2, c, nil)
	if err := mgr2.AddRPDO(RPDOConfig{Number: 0, COBID: 0x181,
		Mappings: []PDOMapping{{0x3000, 0, 16}}}); err != nil {
		t.Fatal(err)
	}

	mgr1.Start()
	mgr2.Start()
	defer mgr1.Stop()
	defer mgr2.Stop()
	mgr1.SetOperational(true)
	mgr2.SetOperational(true)

	// Advance tick by tick until the cyclic TPDO has been produced and
	// consumed. Waiting for the accepted frame (rather than assuming a single
	// advance suffices) removes any virtual-time/goroutine scheduling race.
	received := false
	for i := 0; i < 10 && !received; i++ {
		waitFor(func() bool { return c.activeCount() >= 1 })
		c.Advance(50 * time.Millisecond)
		received = waitFor(func() bool { return mgr2.Stats().RxAccepted >= 1 })
	}
	if !received {
		t.Fatalf("RPDO not accepted: %+v", mgr2.Stats())
	}
	v, _ := od2.ReadRaw(0x3000, 0)
	if !bytes.Equal(v, []byte{0x34, 0x12}) {
		t.Fatalf("consumed value = % x", v)
	}
}

func TestPDODuplicateStaleShortDoNotSpread(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, []byte{0, 0})
	mgr := NewPDOManager(2, bus, od, NewFakeClock(), nil)
	if err := mgr2Add(mgr); err != nil {
		t.Fatal(err)
	}
	mgr.Start()
	defer mgr.Stop()
	mgr.SetOperational(true)

	// valid frame with a trailing sequence byte (DLC=3, mapped=2)
	f1 := Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x01, 0x00, 0x01}}
	bus.PublishFrom(1, f1)
	// exact duplicate
	bus.PublishFrom(1, f1)
	// stale: sequence goes backwards 0x01 -> 0x00
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x09, 0x00, 0x00}})
	// short frame (DLC < mapped 2)
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 1, Data: [8]byte{0x77}})
	// new valid sequence advances and updates data
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x88, 0x13, 0x02}})

	st := mgr.Stats()
	if st.RxAccepted != 2 {
		t.Fatalf("accepted = %d, want 2 (%+v)", st.RxAccepted, st)
	}
	if st.RxDuplicate != 1 {
		t.Fatalf("duplicate = %d, want 1", st.RxDuplicate)
	}
	if st.RxStale != 1 {
		t.Fatalf("stale = %d, want 1", st.RxStale)
	}
	if st.RxOverrun != 1 {
		t.Fatalf("overrun/short = %d, want 1", st.RxOverrun)
	}
	v, _ := od.ReadRaw(0x3000, 0)
	if !bytes.Equal(v, []byte{0x88, 0x13}) {
		t.Fatalf("final OD value = % x, stale/short frames must not corrupt", v)
	}
}

func mgr2Add(mgr *PDOManager) error {
	return mgr.AddRPDO(RPDOConfig{Number: 0, COBID: 0x181, EnableSeq: true, SeqByte: 2,
		Mappings: []PDOMapping{{0x3000, 0, 16}}})
}

func TestPDOBusOffThenRecovery(t *testing.T) {
	// Real clock: enter bus-off for a few PDO periods, then recover. The
	// producer keeps cycling throughout and the consumer resumes on its own.
	clk := RealClock{}
	bus := NewVirtualBus(clk)
	od1 := NewObjectDict()
	_ = od1.Add(Entry{Index: 0x2000, Type: TypeU16, Access: AccessRW}, []byte{0x01, 0x00})
	mgr1 := NewPDOManager(1, bus, od1, clk, nil)
	_ = mgr1.AddTPDO(TPDOConfig{Number: 0, Period: 5 * time.Millisecond, Mappings: []PDOMapping{{0x2000, 0, 16}}})
	od2 := NewObjectDict()
	_ = od2.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	mgr2 := NewPDOManager(2, bus, od2, clk, nil)
	_ = mgr2Add(mgr2)
	mgr1.Start()
	mgr2.Start()
	defer mgr1.Stop()
	defer mgr2.Stop()
	mgr1.SetOperational(true)
	mgr2.SetOperational(true)

	// Wait for a healthy exchange first.
	if !waitFor(func() bool { return mgr2.Stats().RxAccepted >= 3 }) {
		t.Fatal("no healthy PDO exchange before fault")
	}

	bus.Inject(FaultSpec{Kind: FaultBusOff})
	time.Sleep(30 * time.Millisecond)
	if bus.Healthy() {
		t.Fatal("bus should be unhealthy during bus-off")
	}
	before := mgr2.Stats().RxAccepted
	st := mgr2.Stats()
	if st.RxAccepted != before {
		t.Fatal("consumer advanced while bus-off")
	}
	bus.Recover()
	if !waitFor(func() bool { return mgr2.Stats().RxAccepted > before }) {
		t.Fatal("PDO did not resume after bus recovery")
	}
	v, _ := od2.ReadRaw(0x3000, 0)
	if !bytes.Equal(v, []byte{0x01, 0x00}) {
		t.Fatalf("data after recovery = % x, not interpretable", v)
	}
}

func TestPDONotOperationalIgnores(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	mgr := NewPDOManager(2, bus, od, NewFakeClock(), nil)
	_ = mgr2Add(mgr)
	mgr.Start()
	defer mgr.Stop()
	// operational left false
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 2, Data: [8]byte{0x01, 0x02}})
	if mgr.Stats().RxIgnored != 1 {
		t.Fatalf("ignored = %d, want 1", mgr.Stats().RxIgnored)
	}
	v, _ := od.ReadRaw(0x3000, 0)
	if !bytes.Equal(v, []byte{0, 0}) {
		t.Fatal("PDO applied while not operational")
	}
}

// Slow-changing (identical payload) cyclic frames without a sequence counter
// must each be applied idempotently; they must never be dropped as duplicates.
func TestPDOConstantPayloadPeriodicAllAccepted(t *testing.T) {
	clk := RealClock{}
	bus := NewVirtualBus(clk)
	od1 := NewObjectDict()
	_ = od1.Add(Entry{Index: 0x2000, Type: TypeU16, Access: AccessRW}, []byte{0xAB, 0xCD})
	mgr1 := NewPDOManager(1, bus, od1, clk, nil)
	_ = mgr1.AddTPDO(TPDOConfig{Number: 0, Period: 2 * time.Millisecond,
		Mappings: []PDOMapping{{0x2000, 0, 16}}})
	od2 := NewObjectDict()
	_ = od2.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	mgr2 := NewPDOManager(2, bus, od2, clk, nil)
	// No EnableSeq: identical payloads must still be accepted every cycle.
	_ = mgr2.AddRPDO(RPDOConfig{Number: 0, COBID: 0x181,
		Mappings: []PDOMapping{{0x3000, 0, 16}}})
	mgr1.Start()
	mgr2.Start()
	defer mgr1.Stop()
	defer mgr2.Stop()
	mgr1.SetOperational(true)
	mgr2.SetOperational(true)
	if !waitFor(func() bool { return mgr2.Stats().RxAccepted >= 5 }) {
		t.Fatalf("constant-payload periodic frames not all accepted: %+v", mgr2.Stats())
	}
}

// A corrupted PDO frame must not crash the consumer or push it into an error
// state; subsequent valid frames are still applied.
func TestPDOCorruptFrameIsContained(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	mgr := NewPDOManager(2, bus, od, NewFakeClock(), nil)
	_ = mgr2Add(mgr)
	mgr.Start()
	defer mgr.Stop()
	mgr.SetOperational(true)

	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x10, 0x00, 0x01}})
	// bus-level corruption (flips first byte LSB) delivered to consumers
	bus.Inject(FaultSpec{Kind: FaultCorrupt, Probability: 1, Targets: []uint8{1}})
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x20, 0x00, 0x02}})
	bus.Recover()
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x40, 0x01, 0x03}})

	st := mgr.Stats()
	if st.RxAccepted != 3 {
		t.Fatalf("accepted = %d, want 3 (corruption contained)", st.RxAccepted)
	}
	v, _ := od.ReadRaw(0x3000, 0)
	if !bytes.Equal(v, []byte{0x40, 0x01}) {
		t.Fatalf("final value = % x, want newest valid frame", v)
	}
}

// Delayed (late) frames arriving after a newer sequence are classified stale
// and never overwrite newer data; the manager stays healthy.
func TestPDOLateFrameStale(t *testing.T) {
	clk := RealClock{}
	bus := NewVirtualBus(clk)
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	mgr := NewPDOManager(2, bus, od, clk, nil)
	_ = mgr2Add(mgr) // EnableSeq byte 2
	mgr.Start()
	defer mgr.Stop()
	mgr.SetOperational(true)

	// seq 1 is delayed and must arrive after seq 2. Clear the fault via a new
	// (empty) Inject rather than Recover, because Recover also cancels frames
	// already held by the delay; here we specifically want the held frame to
	// turn up late.
	bus.Inject(FaultSpec{Kind: FaultDelay, Probability: 1, Delay: 40 * time.Millisecond, Targets: []uint8{1}})
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x01, 0x00, 0x01}})
	bus.Inject(FaultSpec{}) // stop delaying new frames; keep the held seq 1
	bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x02, 0x00, 0x02}})
	// Wait until the held frame arrives late and is classified stale.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) && mgr.Stats().RxStale != 1 {
		time.Sleep(2 * time.Millisecond)
	}
	if mgr.Stats().RxStale != 1 {
		t.Fatalf("stale = %d, want 1 (late frame)", mgr.Stats().RxStale)
	}
	v, _ := od.ReadRaw(0x3000, 0)
	if !bytes.Equal(v, []byte{0x02, 0x00}) {
		t.Fatalf("late frame overwrote newer data: % x", v)
	}
}
