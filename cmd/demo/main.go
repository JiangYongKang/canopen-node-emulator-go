// Command demo runs a fully in-process multi-node CANopen network: NMT state
// management, heartbeat monitoring, expedited + segmented SDO transfers,
// cyclic PDO exchange, fault injection and recovery. No CAN hardware or
// external service is required.
package main

import (
	"fmt"
	"os"
	"time"

	canopen "github.com/highcumontoa/canopen-node-emulator-go"
)

func main() {
	logger := canopen.NewLogger(os.Stdout, "DEMO")
	net := canopen.NewNetwork(canopen.NetworkConfig{Logger: logger})

	// Node 1: SDO server with a scalar and a domain object.
	od1 := canopen.NewObjectDict()
	must(od1.Add(canopen.Entry{Index: 0x2000, Name: "control", Type: canopen.TypeU16, Access: canopen.AccessRW}, []byte{0, 0}))
	must(od1.Add(canopen.Entry{Index: 0x2100, Name: "blob", Type: canopen.TypeDomain, Access: canopen.AccessRW}, nil))
	node1, err := net.AddNode(canopen.NodeConfig{ID: 1, Name: "sensor", HeartbeatPeriod: 100 * time.Millisecond, EnableSDO: true, OD: od1})
	must(err)
	must(node1.PDO().AddTPDO(canopen.TPDOConfig{Number: 0, Period: 100 * time.Millisecond,
		Mappings: []canopen.PDOMapping{{Index: 0x2000, Bits: 16}}}))

	// Node 2: consumes node 1's TPDO into its own OD object.
	od2 := canopen.NewObjectDict()
	must(od2.Add(canopen.Entry{Index: 0x3000, Name: "rx", Type: canopen.TypeU16, Access: canopen.AccessRW}, nil))
	node2, err := net.AddNode(canopen.NodeConfig{ID: 2, Name: "actuator", HeartbeatPeriod: 100 * time.Millisecond, OD: od2})
	must(err)
	must(node2.PDO().AddRPDO(canopen.RPDOConfig{Number: 0, COBID: 0x181,
		Mappings: []canopen.PDOMapping{{Index: 0x3000, Bits: 16}}}))

	must(net.StartAll())
	defer net.StopAll()

	// Monitor node 1 heartbeats.
	net.WatchNode(1, 300*time.Millisecond, func(ev canopen.MonitorEvent) {
		fmt.Printf(">>> monitor: node %d event %s state=%s\n", ev.NodeID, ev.Kind, ev.State)
	})

	// All nodes -> OPERATIONAL (PDOs start flowing).
	net.NMTCommand(0, canopen.CmdEnterOperational)
	time.Sleep(150 * time.Millisecond)

	// SDO client: expedited write, segmented write/read.
	cli := canopen.NewSDOClient(net.Bus(), canopen.SDOConfig{Timeout: 200 * time.Millisecond, MaxRetries: 2})
	must(cli.Write(1, 0x2000, 0, []byte{0x34, 0x12}))
	big := []byte("The quick brown fox jumps over the lazy dog.")
	must(cli.Write(1, 0x2100, 0, big))
	got, err := cli.Read(1, 0x2100, 0)
	must(err)
	fmt.Printf(">>> segmented read back %d bytes: %q\n", len(got), string(got))
	time.Sleep(150 * time.Millisecond)
	if v, _ := od2.ReadRaw(0x3000, 0); len(v) == 2 {
		fmt.Printf(">>> node2 received TPDO value: 0x%02x%02x\n", v[1], v[0])
	}

	// Protocol-level failure reasons are distinguishable.
	err = cli.Write(1, 0x2000, 0, []byte{1, 2, 3})
	if code, ok := canopen.AsAbort(err); ok {
		fmt.Printf(">>> expected abort for wrong length: %s\n", code)
	}

	// Inject a bus-off, then recover: PDOs resume automatically.
	net.Inject(canopen.FaultSpec{Kind: canopen.FaultBusOff})
	time.Sleep(300 * time.Millisecond)
	net.Recover()
	time.Sleep(250 * time.Millisecond)
	fmt.Println(">>> demo complete")
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}
