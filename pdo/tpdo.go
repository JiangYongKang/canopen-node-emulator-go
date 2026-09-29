package pdo

import (
	"context"
	"log"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
)

// TPDO is one transmit PDO. Building a frame is all-or-nothing: if any
// mapped object cannot be read, no frame is sent and the dictionary is
// never touched.
type TPDO struct {
	bus      *canbus.Bus
	dict     *od.Dictionary
	cob      uint32
	node     uint8
	mappings []Mapping
	period   time.Duration
	logf     func(string, ...any)
}

// TPDOConfig configures a TPDO.
type TPDOConfig struct {
	COB      uint32
	Node     uint8
	Period   time.Duration
	Mappings []Mapping
}

// NewTPDO creates a transmit PDO after validating its mappings.
func NewTPDO(bus *canbus.Bus, dict *od.Dictionary, cfg TPDOConfig) (*TPDO, error) {
	if err := validateMappings(cfg.Mappings); err != nil {
		return nil, err
	}
	return &TPDO{
		bus:      bus,
		dict:     dict,
		cob:      cfg.COB,
		node:     cfg.Node,
		mappings: append([]Mapping(nil), cfg.Mappings...),
		period:   cfg.Period,
		logf:     log.Printf,
	}, nil
}

// SetLogger overrides the logger.
func (t *TPDO) SetLogger(f func(string, ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	t.logf = f
}

// build reads all mapped objects first and only packs a frame when every
// read succeeded, so a missing object never produces a half-populated PDO.
func (t *TPDO) build() (canbus.Frame, error) {
	var f canbus.Frame
	f.ID = t.cob
	f.Source = t.node

	values := make([][]byte, len(t.mappings))
	maxEnd := 0
	for i, m := range t.mappings {
		v, aerr := t.dict.GetRaw(m.Index, m.Sub)
		if aerr != nil {
			return f, aerr
		}
		if len(v) != m.size() {
			return f, &MappingError{Index: m.Index, Sub: m.Sub, Reason: "object length differs from mapping"}
		}
		values[i] = v
		if m.Offset+len(v) > maxEnd {
			maxEnd = m.Offset + len(v)
		}
	}
	for i, m := range t.mappings {
		copy(f.Data[m.Offset:], values[i])
	}
	f.DLC = byte(maxEnd)
	return f, nil
}

// Send builds and publishes one frame. A bus fault simply swallows the
// frame; Send itself never fails due to transport, only due to mapping.
func (t *TPDO) Send() error {
	f, err := t.build()
	if err != nil {
		return err
	}
	t.bus.Publish(f)
	t.logf("TPDO node %d sent COB 0x%X dlc=%d data=% X", t.node, f.ID, f.DLC, f.Data[:f.DLC])
	return nil
}

// Run periodically sends frames until ctx ends. It keeps running across
// bus faults: frames lost during bus-off are simply absent on the wire,
// and sending resumes automatically when the fault is cleared.
func (t *TPDO) Run(ctx context.Context) {
	if t.period <= 0 {
		<-ctx.Done()
		return
	}
	tick := time.NewTicker(t.period)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := t.Send(); err != nil {
				t.logf("TPDO node %d COB 0x%X build failed: %v", t.node, t.cob, err)
			}
		}
	}
}
