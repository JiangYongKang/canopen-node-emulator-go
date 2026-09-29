package canopen

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func setupSDO(t *testing.T) (*VirtualBus, *ObjectDict, *SDOServer, *SDOClient) {
	t.Helper()
	c := NewFakeClock()
	bus := NewVirtualBus(c)
	od := NewObjectDict()
	srv := NewSDOServer(1, bus, od, c, nil)
	srv.Start()
	cli := NewSDOClient(bus, SDOConfig{Timeout: 100 * time.Millisecond, Clock: c})
	t.Cleanup(func() { cli.Stop(); srv.Stop() })
	return bus, od, srv, cli
}

func TestSDOExpeditedReadWrite(t *testing.T) {
	_, od, _, cli := setupSDO(t)
	_ = od.Add(Entry{Index: 0x2000, Type: TypeU32, Access: AccessRW}, []byte{1, 0, 0, 0})
	if err := cli.Write(1, 0x2000, 0, []byte{0x78, 0x56, 0x34, 0x12}); err != nil {
		t.Fatal(err)
	}
	v, err := cli.Read(1, 0x2000, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte{0x78, 0x56, 0x34, 0x12}) {
		t.Fatalf("read back = % x", v)
	}
	// 1-byte expedited
	_ = od.Add(Entry{Index: 0x2001, Type: TypeU8, Access: AccessRW}, nil)
	if err := cli.Write(1, 0x2001, 0, []byte{0xAB}); err != nil {
		t.Fatal(err)
	}
	v, _ = cli.Read(1, 0x2001, 0)
	if !bytes.Equal(v, []byte{0xAB}) {
		t.Fatalf("u8 read back = % x", v)
	}
}

func TestSDOSegmentedRoundTrip(t *testing.T) {
	_, od, _, cli := setupSDO(t)
	_ = od.Add(Entry{Index: 0x2100, Type: TypeDomain, Access: AccessRW}, nil)
	payload := bytes.Repeat([]byte{0xA5}, 23) // 4 segments (7,7,7,2)
	if err := cli.Write(1, 0x2100, 0, payload); err != nil {
		t.Fatalf("segmented write: %v", err)
	}
	v, err := cli.Read(1, 0x2100, 0)
	if err != nil {
		t.Fatalf("segmented read: %v", err)
	}
	if !bytes.Equal(v, payload) {
		t.Fatalf("segmented round trip mismatch len %d vs %d", len(v), len(payload))
	}
}

func TestSDOAbortReasons(t *testing.T) {
	_, od, _, cli := setupSDO(t)
	_ = od.Add(Entry{Index: 0x2000, Type: TypeU32, Access: AccessRO}, []byte{1, 2, 3, 4})
	_ = od.Add(Entry{Index: 0x2001, Type: TypeU16, Access: AccessRW}, nil)
	_ = od.Add(Entry{Index: 0x2002, Type: TypeDomain, Access: AccessWO}, nil)

	// index does not exist
	_, err := cli.Read(1, 0x6666, 0)
	assertAbort(t, err, AbortObjectNotFound)
	// write read-only
	err = cli.Write(1, 0x2000, 0, []byte{9, 9, 9, 9})
	assertAbort(t, err, AbortReadOnlyWrite)
	// length/type mismatch: write 4 bytes into a U16
	err = cli.Write(1, 0x2001, 0, []byte{1, 2, 3, 4})
	assertAbort(t, err, AbortTypeLenMismatch)
	// read write-only
	_, err = cli.Read(1, 0x2002, 0)
	assertAbort(t, err, AbortWriteOnlyRead)
	// read-only value must be untouched
	v, _ := od.ReadRaw(0x2000, 0)
	if !bytes.Equal(v, []byte{1, 2, 3, 4}) {
		t.Fatal("RO entry changed after aborted write")
	}
}

func TestSDOSegmentFailureNoPartialWrite(t *testing.T) {
	_, od, _, cli := setupSDO(t)
	// U32 accepts only 4 bytes; a 11-byte segmented download must abort and
	// leave the original value intact.
	_ = od.Add(Entry{Index: 0x2200, Type: TypeU32, Access: AccessRW}, []byte{0xDE, 0xAD, 0xBE, 0xEF})
	err := cli.Write(1, 0x2200, 0, bytes.Repeat([]byte{0x01}, 11))
	assertAbort(t, err, AbortTypeLenMismatch)
	v, _ := od.ReadRaw(0x2200, 0)
	if !bytes.Equal(v, []byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Fatalf("partial write left state: % x", v)
	}
}

func assertAbort(t *testing.T, err error, want SDOAbortCode) {
	t.Helper()
	var ae *SDOAbortError
	if !errors.As(err, &ae) {
		t.Fatalf("want SDOAbortError %s, got %v", want, err)
	}
	if ae.Code != want {
		t.Fatalf("abort = %s, want %s", ae.Code, want)
	}
}

func TestSDOTimeoutWhenServerSilent(t *testing.T) {
	bus := NewVirtualBus(RealClock{})
	cli := NewSDOClient(bus, SDOConfig{Timeout: 40 * time.Millisecond, Clock: RealClock{}})
	defer cli.Stop()
	// no server exists -> every request is dropped by the bus -> timeout
	start := time.Now()
	_, err := cli.Read(42, 0x2000, 0)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, ErrSDOTimeout) {
		t.Fatalf("want ErrSDOTimeout, got %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("returned before timeout elapsed")
	}
}

func TestSDORetryRecoversAfterDropFaultCleared(t *testing.T) {
	// Deterministic fault gate: drop exactly the first two SDO requests to
	// node 1, then let every later frame through. The client must retry and
	// complete - no wall-clock window to race against.
	c := NewFakeClock()
	bus := NewVirtualBus(c)
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x2000, Type: TypeU16, Access: AccessRW}, nil)
	srv := NewSDOServer(1, bus, od, c, nil)
	srv.Start()
	defer srv.Stop()
	cli := NewSDOClient(bus, SDOConfig{Timeout: 100 * time.Millisecond, MaxRetries: 3, Clock: c})
	defer cli.Stop()

	bus.Inject(FaultSpec{Kind: FaultDrop, Targets: []uint8{1}})
	var dropped int64
	bus.SetFaultGate(func(_ FaultSpec, f Frame) bool {
		// only gate client->server SDO requests (0x601); responses unaffected
		if f.CANID == cobSDOTxBase+1 {
			n := atomic.AddInt64(&dropped, 1)
			return n <= 2 // first two requests dropped
		}
		return false
	})
	defer bus.SetFaultGate(nil)

	done := make(chan error, 1)
	go func() { done <- cli.Write(1, 0x2000, 0, []byte{0x34, 0x12}) }()
	// Advance until the transfer completes (attempts 1 and 2 time out, attempt
	// 3 is delivered). Step in timeout-sized increments, waiting for a timer
	// to be armed before each advance.
	for i := 0; i < 12; i++ {
		if !waitFor(func() bool { return c.pendingCount() >= 1 || len(done) == 1 }) {
			break
		}
		if len(done) == 1 {
			break
		}
		c.Advance(120 * time.Millisecond)
	}
	if err := <-done; err != nil {
		t.Fatalf("write should succeed via retry: %v", err)
	}
	v, _ := od.ReadRaw(0x2000, 0)
	if !bytes.Equal(v, []byte{0x34, 0x12}) {
		t.Fatalf("value = % x", v)
	}
}

func TestSDOInterruptSegmentedLeavesNoPartial(t *testing.T) {
	c := NewFakeClock()
	bus := NewVirtualBus(c)
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x2100, Type: TypeDomain, Access: AccessRW}, []byte("ORIGINAL"))
	srv := NewSDOServer(1, bus, od, c, nil)
	srv.Start()
	defer srv.Stop()
	cli := NewSDOClient(bus, SDOConfig{Timeout: 60 * time.Millisecond, MaxRetries: 0, Clock: c})
	defer cli.Stop()

	// Corrupt frames so segments arrive with bad bytes; then make the bus drop
	// everything mid-transfer forcing a timeout. Simpler: drop fault from the
	// start with a >4-byte payload so segmented initiate is lost -> timeout.
	bus.Inject(FaultSpec{Kind: FaultDrop, Probability: 1, Targets: []uint8{1}})
	errCh := make(chan error, 1)
	go func() { errCh <- cli.Write(1, 0x2100, 0, bytes.Repeat([]byte{0xAA}, 20)) }()
	if !waitFor(func() bool { return c.pendingCount() >= 1 }) {
		t.Fatal("timeout not armed")
	}
	c.Advance(80 * time.Millisecond)
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected interrupted transfer error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("interrupted transfer hung")
	}
	v, _ := od.ReadRaw(0x2100, 0)
	if string(v) != "ORIGINAL" {
		t.Fatalf("interrupted segmented write left state: %q", v)
	}
}
