package pdo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
)

func frame(cob uint32, seq uint64, data ...byte) canbus.Frame {
	var f canbus.Frame
	f.ID = cob
	f.Seq = seq
	f.DLC = byte(len(data))
	copy(f.Data[:], data)
	return f
}

func newRPDOWithBus(t *testing.T, bus *canbus.Bus, timeout time.Duration, ms []Mapping) (*RPDO, *od.Dictionary) {
	t.Helper()
	d := od.New()
	for _, m := range ms {
		access := od.AccessRW
		d.AddRaw(m.Index, m.Sub, m.Type, access, make([]byte, m.Length))
	}
	r, err := NewRPDO(bus, d, RPDOConfig{COB: 0x200, Node: 11, Timeout: timeout, Mappings: ms})
	if err != nil {
		t.Fatal(err)
	}
	r.SetLogger(t.Logf)
	return r, d
}

func sampleMappings() []Mapping {
	return []Mapping{
		{Index: 0x2000, Sub: 0, Type: od.TypeUInt16, Offset: 0, Length: 2},
		{Index: 0x2001, Sub: 0, Type: od.TypeUInt8, Offset: 2, Length: 1},
	}
}

func TestMappingValidation(t *testing.T) {
	bus := canbus.NewBus()
	d := od.New()

	// Overlapping mappings.
	bad := []Mapping{
		{Index: 0x2000, Sub: 0, Type: od.TypeUInt16, Offset: 0, Length: 2},
		{Index: 0x2001, Sub: 0, Type: od.TypeUInt16, Offset: 1, Length: 2},
	}
	if _, err := NewRPDO(bus, d, RPDOConfig{COB: 0x200, Mappings: bad}); err == nil {
		t.Fatal("overlap accepted")
	}
	// Mapping beyond the 8-byte frame.
	beyond := []Mapping{{Index: 0x2000, Sub: 0, Type: od.TypeUInt32, Offset: 6, Length: 4}}
	if _, err := NewRPDO(bus, d, RPDOConfig{COB: 0x200, Mappings: beyond}); err == nil {
		t.Fatal("out-of-range mapping accepted")
	}
	t.Log("invalid mappings rejected deterministically")
}

func TestRPDODuplicateAndReorder(t *testing.T) {
	bus := canbus.NewBus()
	r, d := newRPDOWithBus(t, bus, 0, sampleMappings())

	// Publish three frames directly with controlled seq.
	r.Feed(frame(0x200, 1, 0x01, 0x00, 0x0A))
	r.Feed(frame(0x200, 2, 0x02, 0x00, 0x0B))
	// Duplicate of seq 2: must not rewrite, lastSeq stays 2.
	r.Feed(frame(0x200, 2, 0x02, 0x00, 0x0B))
	// Stale/reordered seq 1: discarded.
	r.Feed(frame(0x200, 1, 0x01, 0x00, 0x0A))
	// New frame applies.
	r.Feed(frame(0x200, 3, 0x03, 0x00, 0x0C))

	if r.LastSeq() != 3 {
		t.Fatalf("lastSeq=%d want 3", r.LastSeq())
	}
	v0, _ := d.GetRaw(0x2000, 0)
	v1, _ := d.GetRaw(0x2001, 0)
	if string(v0) != "\x03\x00" || string(v1) != "\x0C" {
		t.Fatalf("applied data wrong: % X % X", v0, v1)
	}
	if snap := r.Snapshot(); len(snap) != 3 || snap[0] != 3 {
		t.Fatalf("snapshot wrong: % X", snap)
	}
	t.Logf("judgement: duplicate ignored, stale discarded, newest applied seq=%d data=% X",
		r.LastSeq(), r.Snapshot())
}

func TestRPDOAtomicCommitRejectsBadFrame(t *testing.T) {
	bus := canbus.NewBus()
	d := od.New()
	d.AddRaw(0x2000, 0, od.TypeUInt16, od.AccessRW, []byte{0xAA, 0xBB})
	d.AddRaw(0x2001, 0, od.TypeUInt8, od.AccessRO, []byte{0x01}) // read-only
	ms := sampleMappings()
	r, err := NewRPDO(bus, d, RPDOConfig{COB: 0x200, Node: 1, Timeout: 0, Mappings: ms})
	if err != nil {
		t.Fatal(err)
	}
	r.SetLogger(t.Logf)

	r.Feed(frame(0x200, 1, 0x11, 0x22, 0x33))
	// Because 0x2001 is read-only, the whole frame must roll back.
	v0, _ := d.GetRaw(0x2000, 0)
	if string(v0) != "\xAA\xBB" {
		t.Fatalf("partial write leaked after rejected frame: % X", v0)
	}
	if r.LastSeq() != 0 {
		t.Fatalf("rejected frame must not advance lastSeq: %d", r.LastSeq())
	}
	t.Logf("rejected multi-mapping frame left data intact: % X", v0)
}

func TestRPDOTimeoutKeepsData(t *testing.T) {
	bus := canbus.NewBus()
	base := time.Unix(1000, 0)
	clk := &fakeT{now: base}
	r, _ := newRPDOWithBus(t, bus, 50*time.Millisecond, sampleMappings())
	r.now = func() time.Time { return clk.now }

	r.Feed(frame(0x200, 1, 0x01, 0x00, 0x09))
	if st := r.State(base); st != PDOActive {
		t.Fatalf("state=%s want ACTIVE", st)
	}
	clk.now = base.Add(51 * time.Millisecond)
	if st := r.State(clk.now); st != PDOStale {
		t.Fatalf("state=%s want STALE", st)
	}
	// Retained data remains interpretable.
	if snap := r.Snapshot(); string(snap) != "\x01\x00\x09" {
		t.Fatalf("stale snapshot lost: % X", snap)
	}
	// A late valid frame recovers to ACTIVE and refreshes data.
	r.Feed(frame(0x200, 2, 0x02, 0x00, 0x0A))
	if st := r.State(clk.now); st != PDOActive {
		t.Fatalf("state=%s want ACTIVE after recovery", st)
	}
	t.Logf("timeout -> STALE with retained data % X; late frame -> ACTIVE", r.Snapshot())
}

func TestRPDOBusFaultFreezesAndRecovers(t *testing.T) {
	bus := canbus.NewBus()
	base := time.Unix(2000, 0)
	clk := &fakeT{now: base}
	r, _ := newRPDOWithBus(t, bus, 50*time.Millisecond, sampleMappings())
	r.now = func() time.Time { return clk.now }

	r.Feed(frame(0x200, 1, 1, 2, 3))
	bus.SetFaulted(true)
	// Frames during fault are ignored.
	r.Feed(frame(0x200, 2, 4, 5, 6))
	if st := r.State(base); st != PDOFaulted {
		t.Fatalf("state=%s want FAULTED", st)
	}
	if r.LastSeq() != 1 {
		t.Fatalf("fault-period frame advanced seq: %d", r.LastSeq())
	}

	bus.SetFaulted(false)
	if st := r.State(base); st != PDOActive {
		t.Fatalf("state=%s want ACTIVE right after fault clear", st)
	}
	r.Feed(frame(0x200, 3, 7, 8, 9))
	if snap := r.Snapshot(); string(snap) != "\x07\x08\x09" {
		t.Fatalf("post-recovery data wrong: % X", snap)
	}
	t.Logf("fault froze receiver at seq=%d; recovery applied % X", r.LastSeq(), r.Snapshot())
}

func TestTPDOBuildIsAtomicAndPeriodic(t *testing.T) {
	bus := canbus.NewBus()
	d := od.New()
	d.AddRaw(0x2000, 0, od.TypeUInt16, od.AccessRW, []byte{0x34, 0x12})
	d.AddRaw(0x2001, 0, od.TypeUInt8, od.AccessRO, []byte{0x2A})

	tp, err := NewTPDO(bus, d, TPDOConfig{COB: 0x180 + 11, Node: 11, Period: 20 * time.Millisecond, Mappings: sampleMappings()})
	if err != nil {
		t.Fatal(err)
	}
	tp.SetLogger(t.Logf)
	if err := tp.Send(); err != nil {
		t.Fatal(err)
	}

	id, ch := bus.Subscribe(4)
	defer bus.Unsubscribe(id)
	if err := tp.Send(); err != nil {
		t.Fatal(err)
	}
	f := <-ch
	if f.ID != 0x18B || string(f.Data[:3]) != "\x34\x12\x2A" {
		t.Fatalf("TPDO frame wrong: %+v", f)
	}

	// Survives a bus fault and resumes after it clears.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tp.Run(ctx)
	bus.SetFaulted(true)
	time.Sleep(45 * time.Millisecond)
	bus.SetFaulted(false)
	time.Sleep(45 * time.Millisecond)
	got := 0
	for {
		select {
		case <-ch:
			got++
		default:
			if got > 0 {
				t.Logf("TPDO resumed after fault; observed %d post-fault frames", got)
				return
			}
			time.Sleep(10 * time.Millisecond)
			// drain once more
			select {
			case <-ch:
				got++
				continue
			default:
				t.Fatal("TPDO did not resume after fault")
			}
		}
	}
}

func TestRPDOConcurrentFeedsNoRace(t *testing.T) {
	bus := canbus.NewBus()
	r, d := newRPDOWithBus(t, bus, 0, sampleMappings())
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				// seq increases globally-ish; duplicates/stale are legal.
				r.Feed(frame(0x200, uint64(g*200+i), byte(i), byte(g), byte(i)))
				_ = r.Snapshot()
				_ = r.LastSeq()
			}
		}(g)
	}
	wg.Wait()
	// All stored values must remain well-formed.
	v, err := d.GetRaw(0x2000, 0)
	if err != nil || len(v) != 2 {
		t.Fatalf("dictionary corrupted: % X err=%v", v, err)
	}
	t.Logf("concurrent feeds settled at seq=%d data=% X", r.LastSeq(), r.Snapshot())
}

type fakeT struct{ now time.Time }
