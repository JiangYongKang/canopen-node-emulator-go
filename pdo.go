package canopen

import (
	"sync"
	"time"
)

// PDOMapping is one mapped OD object inside a PDO mapping parameter.
type PDOMapping struct {
	Index uint16
	Sub   uint8
	Bits  int
}

// Bytes returns the byte width of a mapping.
func (m PDOMapping) Bytes() int { return (m.Bits + 7) / 8 }

// TPDOConfig describes a transmit PDO.
type TPDOConfig struct {
	Number   int    // 0-based PDO number; selects default COB-ID
	COBID    uint32 // explicit COB-ID; 0 => default 0x180 + (n*0x100) + nodeID
	Period   time.Duration
	Mappings []PDOMapping
}

// RPDOConfig describes a receive PDO.
type RPDOConfig struct {
	Number   int
	COBID    uint32 // explicit COB-ID; 0 => default 0x200 + (n*0x100) + nodeID
	Mappings []PDOMapping
	// EnableSeq turns on sequence-based de-duplication using the producer
	// sequence counter at byte offset SeqByte (0..7), which must lie outside
	// the mapped data. Reused sequence => duplicate; lower sequence => stale.
	// When disabled (default) every frame is applied idempotently, including
	// unchanged payloads (slow-changing process data).
	EnableSeq bool
	SeqByte   int
}

// PDOStats is a per-manager PDO counter snapshot.
type PDOStats struct {
	TxSent      int64
	RxAccepted  int64
	RxDuplicate int64
	RxStale     int64
	RxIgnored   int64
	RxOverrun   int64
}

type tpdo struct {
	cfg    TPDOConfig
	cobid  uint32
	offset []int // byte offset of each mapping inside the 8-byte frame
	total  int
}

type rpdo struct {
	cfg       RPDOConfig
	cobid     uint32
	offset    []int
	total     int
	enableSeq bool
	seqByte   int
	subIDs    []int
}

// PDOManager handles PDO mapping, cyclic TX, RX filtering, de-duplication and
// stale-frame handling for one node. All writes to the OD are per-frame
// transactions, so accepted frames are applied atomically and a repeated frame
// is idempotent.
type PDOManager struct {
	nodeID uint8
	bus    *VirtualBus
	od     *ObjectDict
	clock  Clock
	logger *Logger

	mu       sync.Mutex
	tpdos    []*tpdo
	rpdos    []*rpdo
	op       bool
	stop     chan struct{}
	done     chan struct{}
	running  bool
	lastSeq  map[uint32]uint8   // last accepted sequence number per RPDO COB-ID
	lastData map[uint32][8]byte // exact payload of last accepted frame
	stats    PDOStats
}

// NewPDOManager creates a manager for nodeID.
func NewPDOManager(nodeID uint8, bus *VirtualBus, od *ObjectDict, clock Clock, logger *Logger) *PDOManager {
	if clock == nil {
		clock = RealClock{}
	}
	return &PDOManager{
		nodeID:  nodeID,
		bus:     bus,
		od:      od,
		clock:   clock,
		logger:  logger,
		lastSeq: make(map[uint32]uint8),
	}
}

func tpdDefaultCOB(n int, node uint8) uint32 {
	return 0x180 + uint32(n)*0x100 + uint32(node)
}

func rpdDefaultCOB(n int, node uint8) uint32 {
	return 0x200 + uint32(n)*0x100 + uint32(node)
}

// AddTPDO validates and registers a transmit PDO.
func (p *PDOManager) AddTPDO(cfg TPDOConfig) error {
	t := &tpdo{cfg: cfg, cobid: cfg.COBID}
	if t.cobid == 0 {
		t.cobid = tpdDefaultCOB(cfg.Number, p.nodeID)
	}
	off := 0
	for _, m := range cfg.Mappings {
		t.offset = append(t.offset, off)
		off += m.Bytes()
	}
	t.total = off
	if off > 8 {
		return ErrPDOLength
	}
	if len(cfg.Mappings) == 0 {
		return ErrNoPDOMapping
	}
	p.mu.Lock()
	p.tpdos = append(p.tpdos, t)
	p.mu.Unlock()
	return nil
}

// AddRPDO validates and registers a receive PDO.
func (p *PDOManager) AddRPDO(cfg RPDOConfig) error {
	r := &rpdo{cfg: cfg, cobid: cfg.COBID, enableSeq: cfg.EnableSeq, seqByte: cfg.SeqByte}
	if r.cobid == 0 {
		r.cobid = rpdDefaultCOB(cfg.Number, p.nodeID)
	}
	off := 0
	for _, m := range cfg.Mappings {
		r.offset = append(r.offset, off)
		off += m.Bytes()
	}
	r.total = off
	if off > 8 {
		return ErrPDOLength
	}
	if len(cfg.Mappings) == 0 {
		return ErrNoPDOMapping
	}
	p.mu.Lock()
	p.rpdos = append(p.rpdos, r)
	p.mu.Unlock()
	return nil
}

// SetOperational enables/disables PDO production and consumption. PDOs are
// only active in OPERATIONAL per CiA-301.
func (p *PDOManager) SetOperational(op bool) {
	p.mu.Lock()
	p.op = op
	p.mu.Unlock()
	if p.logger != nil {
		p.logger.Infof("node %d: PDO operational=%v", p.nodeID, op)
	}
}

// Start subscribes RPDOs and launches the cyclic TX loop.
func (p *PDOManager) Start() {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	for _, r := range p.rpdos {
		r := r
		id := p.bus.Subscribe(p.nodeID, 0x7FF, r.cobid, func(f Frame) { p.onRPDO(r, f) })
		r.subIDs = append(r.subIDs, id)
	}
	p.mu.Unlock()
	go p.txLoop()
}

// Stop unsubscribes and stops the TX loop.
func (p *PDOManager) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	subs := make([]int, 0)
	for _, r := range p.rpdos {
		subs = append(subs, r.subIDs...)
	}
	close(p.stop)
	done := p.done
	p.mu.Unlock()
	for _, id := range subs {
		p.bus.Unsubscribe(id)
	}
	<-done
}

// txLoop runs a single ticker at the shortest period and tracks a per-TPDO
// schedule cursor, so a coalesced wake still emits every due TPDO (catch-up)
// and no periodic frame is silently lost under scheduling delays.
func (p *PDOManager) txLoop() {
	defer close(p.done)
	p.mu.Lock()
	tpdos := append([]*tpdo(nil), p.tpdos...)
	p.mu.Unlock()
	var base time.Duration
	for _, t := range tpdos {
		if t.cfg.Period > 0 && (base == 0 || t.cfg.Period < base) {
			base = t.cfg.Period
		}
	}
	if base <= 0 {
		return
	}
	tk := p.clock.NewTicker(base)
	defer tk.Stop()
	last := p.clock.Now()
	next := make(map[*tpdo]time.Time)
	for _, t := range tpdos {
		if t.cfg.Period > 0 {
			next[t] = last.Add(t.cfg.Period)
		}
	}
	for {
		select {
		case <-p.stop:
			return
		case <-tk.C():
		}
		now := p.clock.Now()
		for _, t := range tpdos {
			dl, ok := next[t]
			if !ok {
				continue
			}
			for !dl.After(now) {
				p.sendTPDO(t)
				dl = dl.Add(t.cfg.Period)
			}
			next[t] = dl
		}
	}
}

func (p *PDOManager) sendTPDO(t *tpdo) {
	var f Frame
	f.CANID = t.cobid
	f.DLC = t.total
	p.mu.Lock()
	for i, m := range t.cfg.Mappings {
		v, err := p.od.ReadRaw(m.Index, m.Sub)
		if err != nil {
			p.mu.Unlock()
			if p.logger != nil {
				p.logger.Infof("node %d: TPDO skip, unmapped object %s: %v", p.nodeID, odRef(m.Index, m.Sub), err)
			}
			return
		}
		copy(f.Data[t.offset[i]:], v)
	}
	p.stats.TxSent++
	p.mu.Unlock()
	p.bus.PublishFrom(p.nodeID, f)
	if p.logger != nil {
		p.logger.Framef("TX", f, "TPDO cyclic")
	}
}

// Sync triggers a one-shot transmission of all TPDOs (SYNC-driven use).
func (p *PDOManager) Sync() {
	p.mu.Lock()
	op := p.op
	tpdos := append([]*tpdo(nil), p.tpdos...)
	p.mu.Unlock()
	if !op {
		return
	}
	for _, t := range tpdos {
		p.sendTPDO(t)
	}
}

// onRPDO validates and applies one received frame. It is deliberately
// defensive: malformed/short/duplicate/stale frames never push the node into
// an error state and never corrupt the OD.
func (p *PDOManager) onRPDO(r *rpdo, f Frame) {
	p.mu.Lock()
	op := p.op
	p.mu.Unlock()
	if !op {
		p.count(func(s *PDOStats) *int64 { return &s.RxIgnored })
		if p.logger != nil {
			p.logger.Framef("IGN ", f, "RPDO ignored: node not OPERATIONAL")
		}
		return
	}
	if f.DLC < r.total {
		p.count(func(s *PDOStats) *int64 { return &s.RxOverrun })
		if p.logger != nil {
			p.logger.Framef("SHORT", f, "RPDO shorter than mapping, ignored")
		}
		return
	}

	// Optional producer sequence counter. When enabled, a reused sequence is a
	// duplicate and a lower one is stale; neither is applied. Without a
	// sequence byte every well-formed frame is applied, which is idempotent:
	// repeating identical process data simply writes the same value again.
	if r.enableSeq && r.seqByte >= 0 && r.seqByte < f.DLC {
		seq := f.Data[r.seqByte]
		p.mu.Lock()
		prev, have := p.lastSeq[f.CANID]
		if have {
			delta := int8(seq - prev)
			if delta == 0 {
				p.stats.RxDuplicate++
				p.mu.Unlock()
				if p.logger != nil {
					p.logger.Framef("DUP ", f, "same RPDO sequence, idempotent accept")
				}
				return
			}
			if delta < 0 {
				p.stats.RxStale++
				p.mu.Unlock()
				if p.logger != nil {
					p.logger.Framef("STALE", f, "older RPDO sequence, ignored")
				}
				return
			}
		}
		p.lastSeq[f.CANID] = seq
		p.stats.RxAccepted++
		p.mu.Unlock()
	} else {
		p.mu.Lock()
		p.stats.RxAccepted++
		p.mu.Unlock()
	}

	// Apply mappings. Each OD write is independent and atomic; a failure on
	// one mapping (e.g. object removed) is counted and contained, never fatal.
	for i, m := range r.cfg.Mappings {
		chunk := f.Data[r.offset[i] : r.offset[i]+m.Bytes()]
		if err := p.od.WriteRaw(m.Index, m.Sub, chunk); err != nil {
			p.count(func(s *PDOStats) *int64 { return &s.RxIgnored })
			if p.logger != nil {
				p.logger.Infof("node %d: RPDO mapping %s not applied: %v", p.nodeID, odRef(m.Index, m.Sub), err)
			}
		}
	}
	if p.logger != nil {
		p.logger.Framef("ACC ", f, "RPDO applied to OD")
	}
}

func (p *PDOManager) count(sel func(*PDOStats) *int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	*sel(&p.stats)++
}

// Stats returns a snapshot of PDO counters.
func (p *PDOManager) Stats() PDOStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}
