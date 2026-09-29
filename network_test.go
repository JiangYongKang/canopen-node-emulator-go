package canopen

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestNetwork(t *testing.T, n int) (*Network, *FakeClock) {
	t.Helper()
	c := NewFakeClock()
	net := NewNetwork(NetworkConfig{Clock: c})
	for i := 1; i <= n; i++ {
		od := NewObjectDict()
		_ = od.Add(Entry{Index: 0x2000, Type: TypeU32, Access: AccessRW, Name: "counter"}, nil)
		node, err := net.AddNode(NodeConfig{
			ID:              uint8(i),
			Name:            "node-" + itoa(i),
			HeartbeatPeriod: 50 * time.Millisecond,
			EnableSDO:       true,
			OD:              od,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(net.StopAll)
	return net, c
}

func TestNetworkBroadcastNMT(t *testing.T) {
	net, _ := newTestNetwork(t, 4)
	for _, node := range net.Nodes() {
		if node.State() != StatePreOperational {
			t.Fatalf("node %d state %s, want PREOP", node.ID(), node.State())
		}
	}
	net.NMTCommand(0, CmdEnterOperational)
	for _, node := range net.Nodes() {
		if node.State() != StateOperational {
			t.Fatalf("node %d not OPERATIONAL: %s", node.ID(), node.State())
		}
	}
	// single-node command must not affect peers
	net.NMTCommand(2, CmdEnterStopped)
	if net.nodes[1].State() != StateOperational {
		t.Fatal("unicast command leaked to node 1")
	}
	if net.nodes[2].State() != StateStopped {
		t.Fatal("node 2 not stopped")
	}
}

func TestConcurrentMultiNodeHeartbeatsStable(t *testing.T) {
	const n = 6
	net, c := newTestNetwork(t, n)
	var mu sync.Mutex
	timeouts := map[uint8]int{}
	recoveries := map[uint8]int{}
	monitors := make([]*HeartbeatMonitor, 0, n)
	for i := 1; i <= n; i++ {
		id := uint8(i)
		h := net.WatchNode(id, 120*time.Millisecond, func(ev MonitorEvent) {
			mu.Lock()
			defer mu.Unlock()
			switch ev.Kind {
			case EvTimeout:
				timeouts[ev.NodeID]++
			case EvRecovered:
				recoveries[ev.NodeID]++
			}
		})
		monitors = append(monitors, h)
	}
	t.Cleanup(func() {
		for _, h := range monitors {
			h.Stop()
		}
	})

	// Count heartbeat frames actually emitted per node so that we only advance
	// the fake clock once every producer goroutine has caught up. This avoids
	// virtual-time racing ahead of wall-clock-scheduled producer goroutines.
	var hbCount [n + 1]int64
	var hbMu sync.Mutex
	net.Bus().Subscribe(0, 0x780, 0x700, func(f Frame) {
		node := int(f.CANID & 0x7F)
		if node >= 1 && node <= n {
			hbMu.Lock()
			hbCount[node]++
			hbMu.Unlock()
		}
	})

	// Advance one heartbeat period at a time, always waiting until every node
	// has produced a new heartbeat before the next step.
	var emitted [n + 1]int64
	for tick := 0; tick < 6; tick++ {
		waitFor(func() bool { return c.activeCount() >= 2*n })
		c.Advance(50 * time.Millisecond)
		if !waitFor(func() bool {
			hbMu.Lock()
			defer hbMu.Unlock()
			for i := 1; i <= n; i++ {
				if hbCount[i] <= emitted[i] {
					return false
				}
			}
			return true
		}) {
			t.Fatalf("tick %d: not all nodes emitted a heartbeat: %v", tick, hbCount)
		}
		hbMu.Lock()
		for i := 1; i <= n; i++ {
			emitted[i] = hbCount[i]
		}
		hbMu.Unlock()
	}
	mu.Lock()
	got := len(timeouts)
	mu.Unlock()
	if got != 0 {
		t.Fatalf("unexpected timeouts with healthy producers: %v", timeouts)
	}
	// Monitors must be alive after the most recent heartbeat.
	if !waitFor(func() bool {
		for _, h := range monitors {
			if !h.Alive() {
				return false
			}
		}
		return true
	}) {
		for i := 1; i <= n; i++ {
			if !monitors[i-1].Alive() {
				t.Fatalf("monitor node %d not alive", i)
			}
		}
	}
}

func TestNodeOfflineAndRecovery(t *testing.T) {
	// Real clock: take node 2 offline, observe exactly one timeout, then bring
	// a node with the same id back and observe exactly one recovery. The real
	// clock avoids any virtual-time/monitor-goroutine scheduling race.
	clk := RealClock{}
	net := NewNetwork(NetworkConfig{Clock: clk})
	for i := 1; i <= 3; i++ {
		od := NewObjectDict()
		_ = od.Add(Entry{Index: 0x2000, Type: TypeU32, Access: AccessRW}, nil)
		node, err := net.AddNode(NodeConfig{ID: uint8(i), HeartbeatPeriod: 10 * time.Millisecond, EnableSDO: true, OD: od})
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(net.StopAll)

	var timeoutSeen, recoveredSeen int64
	h := net.WatchNode(2, 40*time.Millisecond, func(ev MonitorEvent) {
		switch ev.Kind {
		case EvTimeout:
			atomic.AddInt64(&timeoutSeen, 1)
		case EvRecovered:
			atomic.AddInt64(&recoveredSeen, 1)
		}
	})
	defer h.Stop()

	// Healthy phase.
	if !waitFor(func() bool { return h.Alive() }) {
		t.Fatal("monitor never became alive")
	}

	// Offline -> exactly one timeout, latched.
	net.RemoveNode(2)
	if !waitFor(func() bool { return atomic.LoadInt64(&timeoutSeen) == 1 }) {
		t.Fatalf("timeout count after offline = %d, want 1", timeoutSeen)
	}
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt64(&timeoutSeen) != 1 {
		t.Fatalf("timeout must latch once, got %d", timeoutSeen)
	}

	// Same node id rejoins -> exactly one recovery.
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x2000, Type: TypeU32, Access: AccessRW}, nil)
	node, err := net.AddNode(NodeConfig{ID: 2, HeartbeatPeriod: 10 * time.Millisecond, EnableSDO: true, OD: od})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return atomic.LoadInt64(&recoveredSeen) == 1 }) {
		t.Fatalf("recovery count = %d, want 1", recoveredSeen)
	}
	if !h.Alive() {
		t.Fatal("monitor not alive after node rejoined")
	}
}
