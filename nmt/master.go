package nmt

import "github.com/highcumontoa/canopen-node-emulator-go/canbus"

// COBIDNMT is the NMT COB-ID (master -> all/single slaves).
const COBIDNMT uint32 = 0x000

// Master issues NMT commands on COB-ID 0x000.
type Master struct {
	bus *canbus.Bus
}

// NewMaster creates an NMT master.
func NewMaster(bus *canbus.Bus) *Master {
	return &Master{bus: bus}
}

// Command sends a unicast (id in 1..127) or broadcast (id == 0) NMT
// command. Each slave decides by the target byte whether the frame is
// addressed to it, so address filtering is a property of the receiver.
func (m *Master) Command(c Command, id uint8) {
	var f canbus.Frame
	f.ID = COBIDNMT
	f.DLC = 2
	f.Data[0] = byte(c)
	f.Data[1] = id
	f.Source = 0
	m.bus.Publish(f)
}

// Start sets node id operational (0 = all).
func (m *Master) Start(id uint8) { m.Command(CmdStart, id) }

// Stop sets node id stopped (0 = all).
func (m *Master) Stop(id uint8) { m.Command(CmdStop, id) }

// EnterPreOperational puts node id into pre-operational (0 = all).
func (m *Master) EnterPreOperational(id uint8) { m.Command(CmdEnterPreOperational, id) }

// ResetNode resets one/all node(s).
func (m *Master) ResetNode(id uint8) { m.Command(CmdResetNode, id) }

// ResetCommunication resets communication of one/all node(s).
func (m *Master) ResetCommunication(id uint8) { m.Command(CmdResetCommunication, id) }
