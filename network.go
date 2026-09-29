package canopen

import (
	"sort"
	"sync"
	"time"
)

// FaultKind enumerates the injectable bus faults.
type FaultKind int

const (
	FaultNone      FaultKind = iota
	FaultDrop                // frames vanish
	FaultDelay               // frames arrive late
	FaultDuplicate           // frames are delivered twice
	FaultReorder             // frame order is perturbed
	FaultCorrupt             // payload bytes are altered
	FaultBusOff              // bus goes bus-off until Recover
)

func (k FaultKind) String() string {
	switch k {
	case FaultDrop:
		return "DROP"
	case FaultDelay:
		return "DELAY"
	case FaultDuplicate:
		return "DUPLICATE"
	case FaultReorder:
		return "REORDER"
	case FaultCorrupt:
		return "CORRUPT"
	case FaultBusOff:
		return "BUS-OFF"
	}
	return "NONE"
}

// FaultSpec describes one injected fault. Empty Targets matches every node.
type FaultSpec struct {
	Kind        FaultKind
	Targets     []uint8 // node IDs matched by COB-ID base; empty = all
	Probability float64
	Delay       time.Duration
}

// NetworkConfig configures a virtual CANopen network.
type NetworkConfig struct {
	Clock  Clock
	Logger *Logger
}

// Network owns one virtual bus, a set of nodes and the NMT master role.
type Network struct {
	clock  Clock
	logger *Logger
	bus    *VirtualBus

	mu    sync.RWMutex
	nodes map[uint8]*Node
}

// NewNetwork creates an empty network.
func NewNetwork(cfg NetworkConfig) *Network {
	clock := cfg.Clock
	if clock == nil {
		clock = RealClock{}
	}
	bus := NewVirtualBus(clock)
	bus.withLogger(cfg.Logger)
	return &Network{clock: clock, logger: cfg.Logger, bus: bus, nodes: make(map[uint8]*Node)}
}

// Bus exposes the virtual bus (for direct fault injection / inspection).
func (n *Network) Bus() *VirtualBus { return n.bus }

// AddNode creates, registers and returns a new node.
func (n *Network) AddNode(cfg NodeConfig) (*Node, error) {
	if cfg.Clock == nil {
		cfg.Clock = n.clock
	}
	if cfg.Logger == nil {
		cfg.Logger = n.logger
	}
	node, err := NewNode(n.bus, cfg)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	if _, exists := n.nodes[cfg.ID]; exists {
		n.mu.Unlock()
		return nil, ErrInvalidNodeID
	}
	n.nodes[cfg.ID] = node
	n.mu.Unlock()
	return node, nil
}

// Node returns a registered node.
func (n *Network) Node(id uint8) (*Node, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	node, ok := n.nodes[id]
	return node, ok
}

// Nodes returns all nodes ordered by id for deterministic iteration.
func (n *Network) Nodes() []*Node {
	n.mu.RLock()
	out := make([]*Node, 0, len(n.nodes))
	for _, node := range n.nodes {
		out = append(out, node)
	}
	n.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID() < out[j].ID() })
	return out
}

// RemoveNode stops and deregisters a node (node offline / removal scenario).
func (n *Network) RemoveNode(id uint8) {
	n.mu.Lock()
	node, ok := n.nodes[id]
	if ok {
		delete(n.nodes, id)
	}
	n.mu.Unlock()
	if ok {
		node.Stop()
		if n.logger != nil {
			n.logger.Infof("node %d removed from network (offline)", id)
		}
	}
}

// StartAll starts every registered node.
func (n *Network) StartAll() error {
	for _, node := range n.Nodes() {
		if err := node.Start(); err != nil {
			return err
		}
	}
	return nil
}

// StopAll stops every registered node.
func (n *Network) StopAll() {
	for _, node := range n.Nodes() {
		node.Stop()
	}
}

// NMTCommand sends an NMT master frame on COB 0x000. target=0 addresses all
// nodes (broadcast); otherwise only the matching node applies it.
func (n *Network) NMTCommand(target uint8, cmd NMTCommand) {
	f := Frame{CANID: 0x000, DLC: 2, Data: [8]byte{byte(cmd), target}}
	n.bus.Publish(f)
	if n.logger != nil {
		n.logger.Framef("TX", f, "NMT master command "+cmdName(cmd)+" target="+itoa(int(target)))
	}
	if target == 0 {
		for _, node := range n.Nodes() {
			node.ApplyNMT(cmd)
		}
		return
	}
	if node, ok := n.Node(target); ok {
		node.ApplyNMT(cmd)
	}
}

// WatchNode creates and starts a heartbeat monitor for one node.
func (n *Network) WatchNode(watched uint8, timeout time.Duration, cb func(MonitorEvent)) *HeartbeatMonitor {
	h := NewHeartbeatMonitor(n.bus, watched, timeout, n.clock, cb)
	h.withLogger(n.logger)
	h.Start()
	return h
}

// Inject applies a bus fault.
func (n *Network) Inject(spec FaultSpec) {
	if n.logger != nil {
		n.logger.Infof("network: inject fault %s targets=%v p=%.2f", spec.Kind, spec.Targets, spec.Probability)
	}
	n.bus.Inject(spec)
}

// Recover clears the injected fault and exits bus-off.
func (n *Network) Recover() {
	if n.logger != nil {
		n.logger.Infof("network: bus recovery requested")
	}
	n.bus.Recover()
}

func cmdName(cmd NMTCommand) string {
	switch cmd {
	case CmdEnterOperational:
		return "ENTER-OPERATIONAL"
	case CmdEnterStopped:
		return "ENTER-STOPPED"
	case CmdEnterPreOperational:
		return "ENTER-PREOPERATIONAL"
	case CmdResetNode:
		return "RESET-NODE"
	case CmdResetCommunication:
		return "RESET-COMMUNICATION"
	}
	return "UNKNOWN"
}
