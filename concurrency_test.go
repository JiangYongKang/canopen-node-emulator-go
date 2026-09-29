package canopen

import (
	"bytes"
	"testing"
	"time"
)

// Independent SDO clients run while heartbeat/PDO goroutines are live. To
// respect the CiA-301 single-default-channel rule, transfers against the same
// server are issued one at a time (each via a fresh client); concurrency comes
// from the many background goroutines touching the same bus and OD. The goal
// is zero races and exact segmented round-trips. A real clock is used because
// bus delivery is synchronous and in-process, so transfers complete instantly.
func TestConcurrentSDOClients(t *testing.T) {
	clk := RealClock{}
	bus := NewVirtualBus(clk)
	od := NewObjectDict()
	const clients = 8
	for i := 0; i < clients; i++ {
		_ = od.Add(Entry{Index: uint16(0x3000 + i), Type: TypeDomain, Access: AccessRW}, nil)
	}
	// Background bus traffic: a heartbeat producer and a cyclic TPDO.
	hb := newHeartbeatProducer(9, bus, NewNMTMachine(9), 2*time.Millisecond, clk, nil)
	hb.Start()
	defer hb.Stop()

	odT := NewObjectDict()
	_ = odT.Add(Entry{Index: 0x2000, Type: TypeU16, Access: AccessRW}, nil)
	tpdo := NewPDOManager(10, bus, odT, clk, nil)
	_ = tpdo.AddTPDO(TPDOConfig{Number: 0, Period: 2 * time.Millisecond,
		Mappings: []PDOMapping{{0x2000, 0, 16}}})
	tpdo.Start()
	defer tpdo.Stop()
	tpdo.SetOperational(true)

	srv := NewSDOServer(1, bus, od, clk, nil)
	srv.Start()
	defer srv.Stop()

	for i := 0; i < clients; i++ {
		idx := uint16(0x3000 + i)
		payload := bytes.Repeat([]byte{byte('A' + i)}, 13+i*3)
		cli := NewSDOClient(bus, SDOConfig{Timeout: 200 * time.Millisecond, Clock: clk})
		if err := cli.Write(1, idx, 0, payload); err != nil {
			cli.Stop()
			t.Fatalf("client %d write: %v", i, err)
		}
		got, err := cli.Read(1, idx, 0)
		cli.Stop()
		if err != nil {
			t.Fatalf("client %d read: %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("client %d mismatch len %d vs %d", i, len(got), len(payload))
		}
	}
}

// Out-of-order frames with increasing sequence numbers are accepted in arrival
// order (a delayed frame arriving late); a frame whose sequence is older than
// the last applied one is classified stale and ignored.
func TestPDOOutOfOrderArrival(t *testing.T) {
	bus := NewVirtualBus(NewFakeClock())
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	mgr := NewPDOManager(2, bus, od, NewFakeClock(), nil)
	_ = mgr2Add(mgr)
	mgr.Start()
	defer mgr.Stop()
	mgr.SetOperational(true)

	pub := func(val byte, seq byte) {
		bus.PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{val, 0x00, seq}})
	}
	pub(0x01, 1)
	pub(0x03, 3) // arrives "early"
	pub(0x02, 2) // now stale relative to seq 3 -> ignored
	pub(0x04, 4) // newest -> accepted

	st := mgr.Stats()
	if st.RxAccepted != 3 {
		t.Fatalf("accepted = %d, want 3", st.RxAccepted)
	}
	if st.RxStale != 1 {
		t.Fatalf("stale = %d, want 1", st.RxStale)
	}
	v, _ := od.ReadRaw(0x3000, 0)
	if v[0] != 0x04 {
		t.Fatalf("final value = % x, want newest 0x04", v)
	}
}
