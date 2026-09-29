package node

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/nmt"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
	"github.com/highcumontoa/canopen-node-emulator-go/pdo"
	"github.com/highcumontoa/canopen-node-emulator-go/sdo"
)

func sensorDict() *od.Dictionary {
	d := od.New()
	d.AddRaw(0x2000, 0, od.TypeUInt16, od.AccessRW, []byte{0, 0})
	d.AddRaw(0x2001, 0, od.TypeUInt8, od.AccessRO, []byte{0x2A})
	d.AddRaw(0x2100, 0, od.TypeVisibleString, od.AccessRW, []byte("multi-node-string"))
	return d
}

func consumerDict() *od.Dictionary {
	d := od.New()
	d.AddRaw(0x2000, 0, od.TypeUInt16, od.AccessRW, []byte{0, 0})
	d.AddRaw(0x2001, 0, od.TypeUInt8, od.AccessRW, []byte{0})
	return d
}

type cluster struct {
	bus    *canbus.Bus
	master *nmt.Master
	mon    *nmt.Monitor
	client *sdo.Client
	nodes  map[uint8]*Node
	cancel context.CancelFunc
	events []nmt.MonitorEvent
	evMu   sync.Mutex
}

func startCluster(t *testing.T) *cluster {
	t.Helper()
	bus := canbus.NewBus()
	ctx, cancel := context.WithCancel(context.Background())

	c := &cluster{bus: bus, master: nmt.NewMaster(bus), nodes: map[uint8]*Node{}, cancel: cancel}

	n1, err := New(bus, Config{ID: 1, HeartbeatPeriod: 30 * time.Millisecond, Dict: sensorDict()})
	if err != nil {
		t.Fatal(err)
	}
	if err := n1.AddTPDO(pdo.TPDOConfig{COB: 0x181, Period: 20 * time.Millisecond, Mappings: []pdo.Mapping{
		{Index: 0x2000, Sub: 0, Type: od.TypeUInt16, Offset: 0, Length: 2},
		{Index: 0x2001, Sub: 0, Type: od.TypeUInt8, Offset: 2, Length: 1},
	}}); err != nil {
		t.Fatal(err)
	}

	n2, err := New(bus, Config{ID: 2, HeartbeatPeriod: 30 * time.Millisecond, Dict: consumerDict()})
	if err != nil {
		t.Fatal(err)
	}
	if err := n2.AddRPDO(pdo.RPDOConfig{COB: 0x181, Timeout: 120 * time.Millisecond, Mappings: []pdo.Mapping{
		{Index: 0x2000, Sub: 0, Type: od.TypeUInt16, Offset: 0, Length: 2},
		{Index: 0x2001, Sub: 0, Type: od.TypeUInt8, Offset: 2, Length: 1},
	}}); err != nil {
		t.Fatal(err)
	}

	n3, err := New(bus, Config{ID: 3, HeartbeatPeriod: 30 * time.Millisecond, Dict: od.New()})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []*Node{n1, n2, n3} {
		n.SetLogger(t.Logf)
		if err := n.Start(ctx); err != nil {
			t.Fatal(err)
		}
		c.nodes[n.ID()] = n
	}

	c.mon = nmt.NewMonitor(bus, 90*time.Millisecond, 1, 2, 3)
	c.mon.SetEventCallback(func(ev nmt.MonitorEvent) {
		c.evMu.Lock()
		c.events = append(c.events, ev)
		c.evMu.Unlock()
		t.Logf("[MON] edge node=%d online=%v state=%s", ev.ID, ev.Online, ev.State)
	})
	go func() { _ = c.mon.Run(ctx) }()

	c.client = sdo.NewClient(bus, sdo.Config{Timeout: 300 * time.Millisecond, Retries: 3, SegmentTimeout: 150 * time.Millisecond})
	c.client.SetLogger(t.Logf)
	go func() { _ = c.client.Run(ctx) }()

	t.Cleanup(func() {
		cancel()
		for _, n := range c.nodes {
			n.Stop()
		}
		c.client.Stop()
		c.mon.Stop()
	})
	return c
}

func waitOnline(t *testing.T, c *cluster, id uint8) {
	t.Helper()
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if st, ok := c.mon.Status(id); ok && st.Online {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %d did not come online", id)
}

func TestNMTBroadcastAndUnicastFiltering(t *testing.T) {
	c := startCluster(t)
	waitOnline(t, c, 1)
	waitOnline(t, c, 2)
	waitOnline(t, c, 3)

	// Unicast to node 1 only.
	c.master.Start(1)
	time.Sleep(20 * time.Millisecond)
	if c.nodes[1].Slave().State() != nmt.StateOperational {
		t.Fatal("node1 not operational")
	}
	if c.nodes[2].Slave().State() != nmt.StatePreOperational {
		t.Fatalf("node2 wrongly changed by unicast: %s", c.nodes[2].Slave().State())
	}
	t.Logf("address filtering: node1=%s node2=%s",
		c.nodes[1].Slave().State(), c.nodes[2].Slave().State())

	// Broadcast stop affects all.
	c.master.Stop(0)
	time.Sleep(20 * time.Millisecond)
	for id, n := range c.nodes {
		if n.Slave().State() != nmt.StateStopped {
			t.Fatalf("node %d not stopped after broadcast: %s", id, n.Slave().State())
		}
	}
	t.Log("broadcast STOP applied to all nodes; conclusions stable")
}

func TestSDOAcrossNodesWithDistinctReasons(t *testing.T) {
	c := startCluster(t)
	waitOnline(t, c, 1)

	ctx := context.Background()
	if err := c.client.Download(ctx, 1, 0x2000, 0, []byte{0x78, 0x56}); err != nil {
		t.Fatal(err)
	}
	v, _, err := c.client.Upload(ctx, 1, 0x2000, 0)
	if err != nil || string(v) != "\x78\x56" {
		t.Fatalf("round trip failed: % X %v", v, err)
	}
	s, _, err := c.client.Upload(ctx, 1, 0x2100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(s) != "multi-node-string" {
		t.Fatalf("segmented upload: %q", s)
	}
	t.Logf("expedited=% X segmented=%q", v, s)

	// Failure reasons remain distinguishable through the full stack.
	var ae *od.AbortError
	_, _, err = c.client.Upload(ctx, 1, 0x6000, 0)
	if !errors.As(err, &ae) || ae.Code != od.AbortNoObject {
		t.Fatalf("missing index reason: %v", err)
	}
	if err := c.client.Download(ctx, 1, 0x2001, 0, []byte{1}); !errors.As(err, &ae) || ae.Code != od.AbortReadOnly {
		t.Fatalf("read-only reason: %v", err)
	}
	t.Logf("failure reasons through stack: %v / %v", od.AbortNoObject, od.AbortReadOnly)
}

func TestPDOPropagationAndIdempotency(t *testing.T) {
	c := startCluster(t)
	waitOnline(t, c, 1)
	c.master.Start(0)

	c.nodes[1].Dict().SetRaw(0x2000, 0, []byte{0xD0, 0x0F})
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		got, _ := c.nodes[2].Dict().GetRaw(0x2000, 0)
		if string(got) == "\xD0\x0F" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	got, _ := c.nodes[2].Dict().GetRaw(0x2000, 0)
	if string(got) != "\xD0\x0F" {
		t.Fatalf("PDO value not propagated: % X", got)
	}
	rp := c.nodes[2].RPDOs()[0]
	seq := rp.LastSeq()
	if seq == 0 {
		t.Fatal("no frame applied")
	}
	t.Logf("PDO propagated node1->node2: % X at seq=%d state=%s", got, seq, rp.State(time.Now()))

	// Re-feeding the exact last frame (duplicate) changes nothing.
	snap := append([]byte(nil), rp.Snapshot()...)
	rp.Feed(mustFrame(t, 0x181, seq, 0xD0, 0x0F, 0x2A))
	if rp.LastSeq() != seq {
		t.Fatal("duplicate advanced seq")
	}
	if string(rp.Snapshot()) != string(snap) {
		t.Fatal("duplicate changed data")
	}
	t.Log("duplicate PDO frame is idempotent")
}

func TestBusFaultRecoveryEndToEnd(t *testing.T) {
	c := startCluster(t)
	waitOnline(t, c, 1)
	waitOnline(t, c, 2)
	waitOnline(t, c, 3)
	c.master.Start(0)
	time.Sleep(30 * time.Millisecond)

	c.evMu.Lock()
	before := len(c.events)
	c.evMu.Unlock()

	c.bus.SetFaulted(true)
	t.Log(">> bus fault injected")
	// Wait until every node is observed offline exactly once.
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		c.evMu.Lock()
		off := 0
		for _, e := range c.events[before:] {
			if !e.Online {
				off++
			}
		}
		c.evMu.Unlock()
		if off == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.evMu.Lock()
	off := 0
	for _, e := range c.events[before:] {
		if !e.Online {
			off++
		}
	}
	c.evMu.Unlock()
	if off != 3 {
		t.Fatalf("expected 3 offline edges, got %d", off)
	}

	// RPDO retains last data and reports FAULTED.
	rp := c.nodes[2].RPDOs()[0]
	retained := rp.Snapshot()
	if st := rp.State(time.Now()); st != pdo.PDOFaulted {
		t.Fatalf("rpdo state during fault = %s", st)
	}

	c.bus.SetFaulted(false)
	t.Log(">> bus fault cleared")
	// Recovery: all three online again, exactly one edge each.
	deadline = time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		allOnline := true
		for id := uint8(1); id <= 3; id++ {
			st, _ := c.mon.Status(id)
			if !st.Online {
				allOnline = false
			}
		}
		if allOnline {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	for id := uint8(1); id <= 3; id++ {
		st, _ := c.mon.Status(id)
		if !st.Online {
			t.Fatalf("node %d did not recover online", id)
		}
	}
	// After recovery a fresh TPDO must drive the receiver back to ACTIVE.
	deadline = time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if rp.State(time.Now()) == pdo.PDOActive {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if st := rp.State(time.Now()); st != pdo.PDOActive {
		t.Fatalf("rpdo state after recovery = %s", st)
	}
	if snap := rp.Snapshot(); len(snap) != 3 {
		t.Fatalf("post-recovery snapshot malformed: % X", snap)
	}
	t.Logf("recovery complete: retained during fault=% X, resumed state=%s", retained, rp.State(time.Now()))
}

func TestSDOTransferInterruptedByFaultRetriesAndSucceeds(t *testing.T) {
	c := startCluster(t)
	waitOnline(t, c, 1)

	// Drop the first three SDO responses: initiate response and segment
	// responses vanish. The client restarts from initiate on each timeout
	// and must still finish the segmented transfer with identical data.
	var mu sync.Mutex
	dropped := 0
	c.bus.AddHook(func(f canbus.Frame) canbus.Frame {
		if f.ID == sdo.ResponseCOB(1) {
			mu.Lock()
			if dropped < 3 {
				dropped++
				mu.Unlock()
				f.ID = canbus.Drop
				return f
			}
			mu.Unlock()
		}
		return f
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _, err := c.client.Upload(ctx, 1, 0x2100, 0)
	if err != nil {
		t.Fatalf("interrupted segmented transfer did not recover: %v", err)
	}
	if string(s) != "multi-node-string" {
		t.Fatalf("recovered data mismatch: %q", s)
	}
	if dropped != 3 {
		t.Fatalf("expected 3 injected drops, got %d", dropped)
	}
	t.Logf("transfer survived %d dropped responses and returned identical data: %q", dropped, s)
}

func mustFrame(t *testing.T, cob uint32, seq uint64, data ...byte) (f canbus.Frame) {
	t.Helper()
	f.ID = cob
	f.Seq = seq
	f.DLC = byte(len(data))
	copy(f.Data[:], data)
	return f
}
