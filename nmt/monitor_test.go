package nmt

import (
	"testing"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
)

// fakeClock is a controllable time source for deterministic monitor tests.
type fakeClock struct{ t time.Time }

func hbFrame(id uint8, state State, seq uint64) canbus.Frame {
	return canbus.Frame{ID: HeartbeatCOB(id), DLC: 1, Data: [8]byte{byte(state)}, Source: id, Seq: seq}
}

func newTestMonitor(t *testing.T, timeout time.Duration) (*Monitor, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Unix(0, 0)}
	m := NewMonitor(canbus.NewBus(), timeout, 1, 2)
	m.setNow(func() time.Time { return clk.t })
	return m, clk
}

func TestMonitorAddressAndFrameFiltering(t *testing.T) {
	m, _ := newTestMonitor(t, time.Second)

	// Wrong COB (not a heartbeat).
	if m.Feed(canbus.Frame{ID: 0x123, DLC: 1}) {
		t.Fatal("non-heartbeat COB accepted")
	}
	// Wrong DLC.
	if m.Feed(canbus.Frame{ID: HeartbeatCOB(1), DLC: 2}) {
		t.Fatal("heartbeat with DLC 2 accepted")
	}
	// Unknown node.
	if m.Feed(hbFrame(9, StateOperational, 1)) {
		t.Fatal("heartbeat for untracked node accepted")
	}
	// Invalid state byte.
	if m.Feed(hbFrame(1, State(6), 1)) {
		t.Fatal("heartbeat with invalid state accepted")
	}
	if _, ok := m.Status(1); ok {
		st, _ := m.Status(1)
		if st.Online {
			t.Fatal("invalid frames brought the node online")
		}
	}
	t.Log("address/DLC/state filtering: invalid frames never alter monitor state")
}

func TestMonitorDuplicateAndLateHeartbeats(t *testing.T) {
	m, clk := newTestMonitor(t, 100*time.Millisecond)
	var events []MonitorEvent
	m.SetEventCallback(func(ev MonitorEvent) { events = append(events, ev) })

	// First heartbeat: online edge exactly once.
	m.Feed(hbFrame(1, StateOperational, 100))
	st, _ := m.Status(1)
	if !st.Online || st.State != StateOperational {
		t.Fatalf("node not online/operational: %+v", st)
	}

	// Exact duplicate (same seq) and another valid beat: no extra edges.
	m.Feed(hbFrame(1, StateOperational, 100))
	m.Feed(hbFrame(1, StateOperational, 101))
	if len(events) != 1 {
		t.Fatalf("duplicate/late heartbeats produced %d events, want 1", len(events))
	}

	// Advance beyond deadline with no frames: exactly one offline edge.
	clk.t = clk.t.Add(101 * time.Millisecond)
	if evs := m.Check(clk.t); len(evs) != 1 || evs[0].Online {
		t.Fatalf("timeout events=%v", evs)
	}
	// Repeated checks while still silent: no repeated edges.
	clk.t = clk.t.Add(time.Second)
	if evs := m.Check(clk.t); len(evs) != 0 {
		t.Fatalf("repeated timeout produced events: %v", evs)
	}
	st, _ = m.Status(1)
	if st.Online || st.Misses != 1 {
		t.Fatalf("offline status wrong: %+v", st)
	}

	// A late heartbeat after timeout recovers the node exactly once.
	m.Feed(hbFrame(1, StateStopped, 102))
	st, _ = m.Status(1)
	if !st.Online || st.State != StateStopped {
		t.Fatalf("recovery failed: %+v", st)
	}
	if len(events) != 3 {
		t.Fatalf("want 3 edges (online, offline, online), got %d: %+v", len(events), events)
	}
	t.Logf("judgement: duplicate/late beats harmless; edges=%+v", events)
}

func TestMonitorOneNodeTimeoutDoesNotAffectOthers(t *testing.T) {
	m, clk := newTestMonitor(t, 100*time.Millisecond)
	m.Feed(hbFrame(1, StateOperational, 1))
	m.Feed(hbFrame(2, StateOperational, 2))

	clk.t = clk.t.Add(50 * time.Millisecond)
	m.Feed(hbFrame(2, StateOperational, 3)) // node 2 alive
	clk.t = clk.t.Add(51 * time.Millisecond)
	evs := m.Check(clk.t)
	if len(evs) != 1 || evs[0].ID != 1 {
		t.Fatalf("only node 1 should time out, got %+v", evs)
	}
	st2, _ := m.Status(2)
	if !st2.Online {
		t.Fatal("node 2 wrongly taken offline")
	}
	t.Logf("node1 timeout isolated: event=%+v node2 online=%v", evs, st2.Online)
}

func TestMonitorRecoveryStateFollowsHeartbeat(t *testing.T) {
	m, clk := newTestMonitor(t, 100*time.Millisecond)
	m.Feed(hbFrame(1, StatePreOperational, 1))
	clk.t = clk.t.Add(200 * time.Millisecond)
	m.Check(clk.t)
	m.Feed(hbFrame(1, StateOperational, 2))
	st, _ := m.Status(1)
	if !st.Online || st.State != StateOperational {
		t.Fatalf("recovered state must equal the received heartbeat, got %+v", st)
	}
	t.Logf("recovery with new state reflected: %s", st.State)
}
