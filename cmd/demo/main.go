// Command demo runs a small local multi-node CANopen emulation without
// any real bus hardware: NMT transitions and heartbeat monitoring, SDO
// expedited/segmented transfer with failure reasons, periodic PDO
// exchange, and a bus-fault injection cycle showing timeout then
// recovery. Run with: go run ./cmd/demo
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/nmt"
	"github.com/highcumontoa/canopen-node-emulator-go/node"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
	"github.com/highcumontoa/canopen-node-emulator-go/pdo"
	"github.com/highcumontoa/canopen-node-emulator-go/sdo"
)

func main() {
	logger := log.New(os.Stdout, "", log.Ltime|log.Lmicroseconds)
	logf := logger.Printf

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bus := canbus.NewBus()

	// --- node 1: a sensor publishing a TPDO, with a segmented string OD entry ---
	d1 := od.New()
	d1.AddRaw(0x2000, 0, od.TypeUInt16, od.AccessRW, []byte{0x00, 0x00})
	d1.AddRaw(0x2001, 0, od.TypeUInt8, od.AccessRO, []byte{0x2A})
	d1.AddRaw(0x2100, 0, od.TypeVisibleString, od.AccessRW, []byte("hello-canopen"))
	n1, err := node.New(bus, node.Config{ID: 1, HeartbeatPeriod: 60 * time.Millisecond, Dict: d1})
	must(err)
	must(n1.AddTPDO(pdo.TPDOConfig{COB: 0x181, Period: 40 * time.Millisecond, Mappings: []pdo.Mapping{
		{Index: 0x2000, Sub: 0, Type: od.TypeUInt16, Offset: 0, Length: 2},
		{Index: 0x2001, Sub: 0, Type: od.TypeUInt8, Offset: 2, Length: 1},
	}}))

	// --- node 2: consumes the TPDO ---
	d2 := od.New()
	d2.AddRaw(0x2000, 0, od.TypeUInt16, od.AccessRW, []byte{0, 0})
	d2.AddRaw(0x2001, 0, od.TypeUInt8, od.AccessRW, []byte{0})
	n2, err := node.New(bus, node.Config{ID: 2, HeartbeatPeriod: 60 * time.Millisecond, Dict: d2})
	must(err)
	must(n2.AddRPDO(pdo.RPDOConfig{COB: 0x181, Timeout: 150 * time.Millisecond, Mappings: []pdo.Mapping{
		{Index: 0x2000, Sub: 0, Type: od.TypeUInt16, Offset: 0, Length: 2},
		{Index: 0x2001, Sub: 0, Type: od.TypeUInt8, Offset: 2, Length: 1},
	}}))

	n3, err := node.New(bus, node.Config{ID: 3, HeartbeatPeriod: 60 * time.Millisecond, Dict: od.New()})
	must(err)

	for _, n := range []*node.Node{n1, n2, n3} {
		n.SetLogger(logf)
		must(n.Start(ctx))
	}

	// NMT master + heartbeat consumer.
	master := nmt.NewMaster(bus)
	mon := nmt.NewMonitor(bus, 200*time.Millisecond, 1, 2, 3)
	mon.SetEventCallback(func(ev nmt.MonitorEvent) {
		logf("[MON] node %d edge online=%v state=%s", ev.ID, ev.Online, ev.State)
	})
	go func() { _ = mon.Run(ctx) }()

	// SDO client.
	client := sdo.NewClient(bus, sdo.Config{Timeout: 300 * time.Millisecond, Retries: 2, SegmentTimeout: 150 * time.Millisecond})
	client.SetLogger(logf)
	go func() { _ = client.Run(ctx) }()

	// Wait for the first heartbeats.
	time.Sleep(120 * time.Millisecond)
	master.Start(0) // broadcast operational
	logf("NMT broadcast OPERATIONAL sent")

	// Sensor value changes and PDO propagates to node 2.
	d1.SetRaw(0x2000, 0, []byte{0x34, 0x12})
	time.Sleep(120 * time.Millisecond)

	// SDO expedited read/write.
	v, _, err := client.Upload(ctx, 1, 0x2000, 0)
	must(err)
	logf("SDO expedited upload 0x2000:00 = % X", v)
	must(client.Download(ctx, 1, 0x2000, 0, []byte{0x78, 0x56}))

	// SDO segmented upload (>4 bytes).
	str, _, err := client.Upload(ctx, 1, 0x2100, 0)
	must(err)
	logf("SDO segmented upload 0x2100:00 = %q (%d bytes)", string(str), len(str))

	// Distinguishable protocol failures.
	if _, _, err := client.Upload(ctx, 1, 0x6000, 0); err != nil {
		logf("expected failure (missing index): %v", err)
	}
	if err := client.Download(ctx, 1, 0x2001, 0, []byte{1}); err != nil {
		logf("expected failure (read-only): %v", err)
	}
	if err := client.Download(ctx, 1, 0x2000, 0, []byte{1}); err != nil {
		logf("expected failure (length mismatch): %v", err)
	}

	// Bus fault injection: heartbeats/PDOs vanish, monitor times out,
	// RPDO goes FAULTED; clearing the fault restores everything.
	logf(">> inject bus fault")
	bus.SetFaulted(true)
	time.Sleep(300 * time.Millisecond)
	st, _ := mon.Status(1)
	logf("[MON] node1 during fault: online=%v state=%s misses=%d", st.Online, st.State, st.Misses)
	logf("[PDO] node2 RPDO state during fault: %s, last data retained=% X",
		n2.RPDOs()[0].State(time.Now()), n2.RPDOs()[0].Snapshot())
	logf(">> clear bus fault (recovery)")
	bus.SetFaulted(false)
	time.Sleep(300 * time.Millisecond)
	st, _ = mon.Status(1)
	logf("[MON] node1 after recovery: online=%v state=%s", st.Online, st.State)
	logf("[PDO] node2 RPDO state after recovery: %s snapshot=% X",
		n2.RPDOs()[0].State(time.Now()), n2.RPDOs()[0].Snapshot())

	logf("bus counters: accepted=%d dropped=%d", bus.Frames(), bus.Dropped())
	logf("demo finished")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
