package pdo

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
)

// RPDO is one receive PDO.
type RPDO struct {
	bus      *canbus.Bus
	dict     *od.Dictionary
	cob      uint32
	node     uint8 // owning node (for logs)
	mappings []Mapping
	timeout  time.Duration
	logf     func(string, ...any)

	mu       sync.RWMutex
	lastSeq  uint64
	lastData [8]byte
	lastDLC  byte
	lastSeen time.Time
	state    PDOState
	now      func() time.Time
	subID    uint64

	onApply func(canbus.Frame)
}

// RPDOConfig configures an RPDO.
type RPDOConfig struct {
	COB      uint32
	Node     uint8
	Timeout  time.Duration
	Mappings []Mapping
}

// NewRPDO creates a receive PDO and validates its mapping.
func NewRPDO(bus *canbus.Bus, dict *od.Dictionary, cfg RPDOConfig) (*RPDO, error) {
	if err := validateMappings(cfg.Mappings); err != nil {
		return nil, err
	}
	return &RPDO{
		bus:      bus,
		dict:     dict,
		cob:      cfg.COB,
		node:     cfg.Node,
		timeout:  cfg.Timeout,
		mappings: append([]Mapping(nil), cfg.Mappings...),
		state:    PDOIdle,
		now:      time.Now,
		logf:     log.Printf,
	}, nil
}

// SetLogger overrides the logger.
func (r *RPDO) SetLogger(f func(string, ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	r.logf = f
}

// SetApplyCallback registers a hook invoked after a new frame was applied
// (never for duplicates/reorders). Must be set before Run.
func (r *RPDO) SetApplyCallback(fn func(canbus.Frame)) { r.onApply = fn }

// Feed processes one inbound frame.
//
// Decision table (deterministic for identical input):
//   - wrong COB/DLC or faulted bus  : ignored, state unchanged
//   - Seq < lastSeq                 : stale/reordered, discarded
//   - Seq == lastSeq                : duplicate, acknowledged, no write
//   - Seq > lastSeq                 : applied atomically to the OD
//
// Applied data always passes OD validation per mapped entry; if any
// entry rejects its bytes, the whole frame is rejected (no partial write).
func (r *RPDO) Feed(f canbus.Frame) {
	if f.ID != r.cob {
		return
	}
	if f.DLC > 8 {
		return
	}
	if r.bus.Faulted() {
		r.logf("RPDO node %d COB 0x%X ignored frame seq=%d (bus faulted)", r.node, r.cob, f.Seq)
		return
	}

	r.mu.Lock()
	lastSeq := r.lastSeq
	switch {
	case f.Seq < lastSeq && lastSeq != 0:
		r.mu.Unlock()
		r.logf("RPDO node %d COB 0x%X discard stale seq=%d < lastSeq=%d",
			r.node, r.cob, f.Seq, lastSeq)
		return
	case f.Seq == lastSeq && lastSeq != 0:
		r.mu.Unlock()
		r.logf("RPDO node %d COB 0x%03X ignore duplicate seq=%d", r.node, r.cob, f.Seq)
		return
	}

	// Validate every mapping before mutating anything.
	for _, m := range r.mappings {
		if m.Offset+int(f.DLC) > 8 || m.Offset+m.size() > int(f.DLC) {
			// Frame shorter than the mapping expects: reject whole frame.
			r.mu.Unlock()
			r.logf("RPDO node %d COB 0x%X reject frame seq=%d dlc=%d too short for mapping 0x%04X:%02X",
				r.node, r.cob, f.Seq, f.DLC, m.Index, m.Sub)
			return
		}
	}
	// Stage every mapped value and commit through an atomic dictionary
	// transaction: validation precedes installation, so a rejected frame
	// never leaves a partially applied mapping.
	tx := r.dict.Begin()
	for _, m := range r.mappings {
		v := append([]byte(nil), f.Data[m.Offset:m.Offset+m.size()]...)
		tx.Set(m.Index, m.Sub, v)
	}
	if aerr := tx.Commit(); aerr != nil {
		r.mu.Unlock()
		r.logf("RPDO node %d COB 0x%X reject seq=%d atomic commit: 0x%04X:%02X %s",
			r.node, r.cob, f.Seq, aerr.Index, aerr.SubIndex, aerr)
		return
	}

	r.lastSeq = f.Seq
	r.lastData = f.Data
	r.lastDLC = f.DLC
	r.lastSeen = r.now()
	prev := r.state
	r.state = PDOActive
	cb := r.onApply
	r.mu.Unlock()

	if prev != PDOActive {
		r.logf("RPDO node %d COB 0x%X state %s -> ACTIVE (seq=%d)", r.node, r.cob, prev, f.Seq)
	}
	r.logf("RPDO node %d COB 0x%X applied seq=%d data=% X", r.node, r.cob, f.Seq, f.Data[:f.DLC])
	if cb != nil {
		cb(f)
	}
}

// Snapshot returns a copy of the last applied payload (nil until first
// frame). The snapshot stays valid during STALE/FAULTED, so recovered or
// retained data is always interpretable.
func (r *RPDO) Snapshot() []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.state == PDOIdle {
		return nil
	}
	out := make([]byte, r.lastDLC)
	copy(out, r.lastData[:r.lastDLC])
	return out
}

// LastSeq returns the bus sequence number of the last applied frame.
func (r *RPDO) LastSeq() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastSeq
}

// State evaluates timeout/fault and returns the current receiver state.
// State transitions are edge-logged only.
func (r *RPDO) State(now time.Time) PDOState {
	r.mu.Lock()
	defer r.mu.Unlock()

	target := r.state
	if r.bus.Faulted() {
		target = PDOFaulted
	} else if r.state == PDOFaulted {
		// Fault cleared: keep last known state; timeout decides next.
		target = PDOActive
		if r.timeout > 0 && now.After(r.lastSeen.Add(r.timeout)) {
			target = PDOStale
		}
	} else if r.state == PDOActive && r.timeout > 0 && now.After(r.lastSeen.Add(r.timeout)) {
		target = PDOStale
	}
	if target != r.state {
		r.logf("RPDO node %d COB 0x%X state %s -> %s", r.node, r.cob, r.state, target)
		r.state = target
	}
	return r.state
}

// Run subscribes to the bus and periodically evaluates timeouts until
// ctx ends.
func (r *RPDO) Run(ctx context.Context) {
	id, ch := r.bus.Subscribe(256)
	r.subID = id
	defer r.bus.Unsubscribe(id)

	tickDur := r.timeout / 4
	if tickDur <= 0 {
		tickDur = 5 * time.Millisecond
	}
	tick := time.NewTicker(tickDur)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case f, ok := <-ch:
			if !ok {
				return
			}
			r.Feed(f)
		case now := <-tick.C:
			r.State(now)
		}
	}
}

// Stop releases resources.
func (r *RPDO) Stop() {
	if r.subID != 0 {
		r.bus.Unsubscribe(r.subID)
		r.subID = 0
	}
}
