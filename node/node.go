// Package node assembles one emulated CANopen node: NMT slave/heartbeat,
// SDO server and PDOs sharing one object dictionary.
package node

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/nmt"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
	"github.com/highcumontoa/canopen-node-emulator-go/pdo"
	"github.com/highcumontoa/canopen-node-emulator-go/sdo"
)

// Node is one emulated CANopen device.
type Node struct {
	bus      *canbus.Bus
	id       uint8
	dict     *od.Dictionary
	slave    *nmt.Slave
	hb       *nmt.Producer
	srv      *sdo.Server
	logf     func(string, ...any)
	tpdos    []*pdo.TPDO
	rpdos    []*pdo.RPDO
	hbPeriod time.Duration

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
	subID  uint64
}

// Config configures a node.
type Config struct {
	ID              uint8
	HeartbeatPeriod time.Duration
	Dict            *od.Dictionary
}

// New creates a node in NMT Pre-operational.
func New(bus *canbus.Bus, cfg Config) (*Node, error) {
	if cfg.ID < 1 || cfg.ID > 127 {
		return nil, errNodeID(cfg.ID)
	}
	if cfg.Dict == nil {
		cfg.Dict = od.New()
	}
	if cfg.HeartbeatPeriod == 0 {
		cfg.HeartbeatPeriod = 100 * time.Millisecond
	}
	n := &Node{
		bus:      bus,
		id:       cfg.ID,
		dict:     cfg.Dict,
		hbPeriod: cfg.HeartbeatPeriod,
		logf:     log.Printf,
	}
	n.slave = nmt.NewSlave(bus, cfg.ID)
	n.hb = nmt.NewProducer(bus, cfg.ID, cfg.HeartbeatPeriod, func() nmt.State {
		return n.slave.State()
	})
	n.srv = sdo.NewServer(bus, cfg.ID, cfg.Dict)
	n.srv.SetLogger(n.logf)
	return n, nil
}

type nodeIDError uint8

func (e nodeIDError) Error() string { return "node: id must be 1..127" }

func errNodeID(id uint8) error { return nodeIDError(id) }

// ID returns the node id.
func (n *Node) ID() uint8 { return n.id }

// Dict returns the object dictionary.
func (n *Node) Dict() *od.Dictionary { return n.dict }

// Slave returns the NMT slave.
func (n *Node) Slave() *nmt.Slave { return n.slave }

// SetLogger overrides the logger used by all subsystems.
func (n *Node) SetLogger(f func(string, ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	n.logf = f
	n.srv.SetLogger(f)
	for _, t := range n.tpdos {
		t.SetLogger(f)
	}
	for _, r := range n.rpdos {
		r.SetLogger(f)
	}
}

// AddTPDO registers a transmit PDO.
func (n *Node) AddTPDO(cfg pdo.TPDOConfig) error {
	cfg.Node = n.id
	t, err := pdo.NewTPDO(n.bus, n.dict, cfg)
	if err != nil {
		return err
	}
	t.SetLogger(n.logf)
	n.tpdos = append(n.tpdos, t)
	return nil
}

// AddRPDO registers a receive PDO.
func (n *Node) AddRPDO(cfg pdo.RPDOConfig) error {
	cfg.Node = n.id
	r, err := pdo.NewRPDO(n.bus, n.dict, cfg)
	if err != nil {
		return err
	}
	r.SetLogger(n.logf)
	n.rpdos = append(n.rpdos, r)
	return nil
}

// TPDOs returns registered transmit PDOs.
func (n *Node) TPDOs() []*pdo.TPDO { return append([]*pdo.TPDO(nil), n.tpdos...) }

// RPDOs returns registered receive PDOs.
func (n *Node) RPDOs() []*pdo.RPDO { return append([]*pdo.RPDO(nil), n.rpdos...) }

// Start runs NMT command listening, heartbeat production, the SDO server
// and every PDO until Stop or ctx cancellation.
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cancel != nil {
		return errAlreadyStarted
	}
	ctx, n.cancel = context.WithCancel(ctx)

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.runNMTListener(ctx)
	}()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.hb.Run(ctx)
	}()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		_ = n.srv.Run(ctx)
	}()
	for _, t := range n.tpdos {
		t := t
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			t.Run(ctx)
		}()
	}
	for _, r := range n.rpdos {
		r := r
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			r.Run(ctx)
		}()
	}
	n.logf("node %d started in state %s (hb=%s)", n.id, n.slave.State(), n.hbPeriod)
	return nil
}

// runNMTListener applies only NMT frames addressed to this node (target
// id == node id) or broadcast (target id == 0). Address filtering is
// performed here; other nodes' unicast frames are ignored.
func (n *Node) runNMTListener(ctx context.Context) {
	id, ch := n.bus.Subscribe(256)
	n.subID = id
	defer n.bus.Unsubscribe(id)

	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-ch:
			if !ok {
				return
			}
			if f.ID != nmt.COBIDNMT || f.DLC != 2 {
				continue
			}
			target := f.Data[1]
			if target != 0 && target != n.id {
				continue
			}
			cmd := nmt.Command(f.Data[0])
			before := n.slave.State()
			if err := n.slave.ApplyCommand(cmd); err != nil {
				n.logf("node %d rejects NMT %s: %v", n.id, cmd, err)
				continue
			}
			n.logf("node %d NMT %s: %s -> %s", n.id, cmd, before, n.slave.State())
		}
	}
}

// Stop halts all services.
func (n *Node) Stop() {
	n.mu.Lock()
	cancel := n.cancel
	n.cancel = nil
	n.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	n.wg.Wait()
	for _, r := range n.rpdos {
		r.Stop()
	}
}
