package canopen

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestLoggingPrintsFramesStatesAndReasons(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "TEST")
	clk := RealClock{}
	net := NewNetwork(NetworkConfig{Clock: clk, Logger: logger})

	od1 := NewObjectDict()
	_ = od1.Add(Entry{Index: 0x2000, Type: TypeU16, Access: AccessRW}, []byte{0xAA, 0xBB})
	node1, _ := net.AddNode(NodeConfig{ID: 1, HeartbeatPeriod: 5 * time.Millisecond, EnableSDO: true, OD: od1})
	if err := node1.PDO().AddTPDO(TPDOConfig{Number: 0, Period: 5 * time.Millisecond,
		Mappings: []PDOMapping{{0x2000, 0, 16}}}); err != nil {
		t.Fatal(err)
	}

	od2 := NewObjectDict()
	_ = od2.Add(Entry{Index: 0x3000, Type: TypeU16, Access: AccessRW}, nil)
	node2, _ := net.AddNode(NodeConfig{ID: 2, HeartbeatPeriod: 5 * time.Millisecond, OD: od2})
	if err := node2.PDO().AddRPDO(RPDOConfig{Number: 0, COBID: 0x181, EnableSeq: true, SeqByte: 2,
		Mappings: []PDOMapping{{0x3000, 0, 16}}}); err != nil {
		t.Fatal(err)
	}
	if err := net.StartAll(); err != nil {
		t.Fatal(err)
	}
	defer net.StopAll()

	// NMT state change
	net.NMTCommand(0, CmdEnterOperational)

	// SDO transfer (expedited write then read)
	cli := NewSDOClient(net.Bus(), SDOConfig{Timeout: 100 * time.Millisecond, Clock: clk, Logger: logger})
	defer cli.Stop()
	if err := cli.Write(1, 0x2000, 0, []byte{0x11, 0x22}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Read(1, 0x2000, 0); err != nil {
		t.Fatal(err)
	}

	// Let cyclic heartbeats and TPDOs flow.
	time.Sleep(40 * time.Millisecond)

	// Two RPDO frames carrying the same sequence counter: the first is applied
	// and the second is classified as a duplicate (decision reason logged).
	net.Bus().PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x11, 0x22, 0xF0}})
	net.Bus().PublishFrom(1, Frame{CANID: 0x181, DLC: 3, Data: [8]byte{0x11, 0x22, 0xF0}})

	// Stop all background producers before inspecting the buffer so no logger
	// goroutine is concurrently writing to it.
	net.StopAll()
	cli.Stop()

	out := buf.String()
	for _, want := range []string{
		"FRM",                // key frames
		"NMT",                // state transition
		"OPERATIONAL",        // target state text
		"SDO",                // SDO frames / decision
		"heartbeat",          // heartbeat frames
		"TPDO",               // PDO transmission
		"same RPDO sequence", // decision basis for duplicate handling
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q", want)
		}
	}
	if t.Failed() {
		t.Logf("captured log:\n%s", out)
	}
}
