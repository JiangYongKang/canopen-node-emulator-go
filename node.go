package canopen

import "time"

// NodeConfig configures a CANopen node.
type NodeConfig struct {
	ID              uint8
	Name            string
	HeartbeatPeriod time.Duration // 0 disables heartbeat producing
	EnableSDO       bool
	OD              *ObjectDict
	Clock           Clock
	Logger          *Logger
}

// Node bundles NMT state machine, heartbeat producer, optional SDO server and
// PDO manager for one CANopen device.
type Node struct {
	bus    *VirtualBus
	cfg    NodeConfig
	od     *ObjectDict
	nmt    *NMTMachine
	hb     *heartbeatProducer
	sdo    *SDOServer
	pdo    *PDOManager
	clock  Clock
	logger *Logger
}

// NewNode constructs (but does not start) a node.
func NewNode(bus *VirtualBus, cfg NodeConfig) (*Node, error) {
	if cfg.ID == 0 || cfg.ID > 127 {
		return nil, ErrInvalidNodeID
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	if cfg.OD == nil {
		cfg.OD = NewObjectDict()
	}
	n := &Node{bus: bus, cfg: cfg, od: cfg.OD, clock: cfg.Clock, logger: cfg.Logger}
	n.nmt = NewNMTMachine(cfg.ID).withClock(cfg.Clock, cfg.Logger)
	n.hb = newHeartbeatProducer(cfg.ID, bus, n.nmt, cfg.HeartbeatPeriod, cfg.Clock, cfg.Logger)
	if cfg.EnableSDO {
		n.sdo = NewSDOServer(cfg.ID, bus, cfg.OD, cfg.Clock, cfg.Logger)
	}
	n.pdo = NewPDOManager(cfg.ID, bus, cfg.OD, cfg.Clock, cfg.Logger)
	return n, nil
}

// ID returns the node id.
func (n *Node) ID() uint8 { return n.cfg.ID }

// Name returns the configured name.
func (n *Node) Name() string {
	if n.cfg.Name == "" {
		return "node-" + itoa(int(n.cfg.ID))
	}
	return n.cfg.Name
}

// State returns the NMT state.
func (n *Node) State() NMTState { return n.nmt.State() }

// SetState performs a legal NMT transition and applies side effects:
// PDOs run only in OPERATIONAL and the heartbeat producer reflects new state.
func (n *Node) SetState(s NMTState) {
	from := n.State()
	to, changed := n.nmt.Transition(s)
	if changed && from == StateInitialising && to == StatePreOperational {
		n.hb.PublishBootup()
	}
	n.pdo.SetOperational(to == StateOperational)
}

// ApplyNMT applies an NMT master command and returns the resulting state.
func (n *Node) ApplyNMT(cmd NMTCommand) NMTState {
	to, ok := n.nmt.ApplyCommand(cmd)
	if !ok {
		return n.State()
	}
	switch cmd {
	case CmdResetNode, CmdResetCommunication:
		n.hb.PublishBootup()
	}
	n.pdo.SetOperational(to == StateOperational)
	return to
}

// OD returns the node's object dictionary.
func (n *Node) OD() *ObjectDict { return n.od }

// SDO returns the SDO server, which may be nil if SDO was disabled.
func (n *Node) SDO() *SDOServer { return n.sdo }

// PDO returns the PDO manager.
func (n *Node) PDO() *PDOManager { return n.pdo }

// NMT returns the state machine.
func (n *Node) NMT() *NMTMachine { return n.nmt }

// HeartbeatPeriod returns the configured heartbeat period.
func (n *Node) HeartbeatPeriod() time.Duration { return n.cfg.HeartbeatPeriod }

// Start brings the node up: INITIALISING -> PRE-OPERATIONAL (boot-up frame),
// starts the SDO server, PDO manager and heartbeat producer.
func (n *Node) Start() error {
	n.SetState(StatePreOperational)
	if n.sdo != nil {
		n.sdo.Start()
	}
	n.pdo.Start()
	n.hb.Start()
	return nil
}

// Stop shuts down every subsystem in reverse order.
func (n *Node) Stop() {
	n.hb.Stop()
	if n.sdo != nil {
		n.sdo.Stop()
	}
	n.pdo.Stop()
}
