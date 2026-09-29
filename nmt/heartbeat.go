package nmt

import (
	"context"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
)

// HeartbeatCOB returns the heartbeat COB-ID for node id (0x700 + id).
func HeartbeatCOB(id uint8) uint32 { return 0x700 + uint32(id) }

// Producer periodically emits heartbeat frames (COB 0x700+id, one data
// byte carrying the current NMT state). A first frame is sent
// immediately so a freshly started node is observable without delay.
type Producer struct {
	bus     *canbus.Bus
	id      uint8
	period  time.Duration
	stateFn func() State
}

// NewProducer creates a heartbeat producer. stateFn is read on every beat
// so the emitted byte always reflects the current slave state.
func NewProducer(bus *canbus.Bus, id uint8, period time.Duration, stateFn func() State) *Producer {
	return &Producer{bus: bus, id: id, period: period, stateFn: stateFn}
}

// emit sends one heartbeat frame.
func (p *Producer) emit() {
	var f canbus.Frame
	f.ID = HeartbeatCOB(p.id)
	f.DLC = 1
	f.Data[0] = byte(p.stateFn())
	f.Source = p.id
	p.bus.Publish(f)
}

// Run emits heartbeats until ctx is cancelled. Frames lost during a bus
// fault cause consumers to time out; once the fault clears, subsequent
// frames let them recover, without any state change in this producer.
func (p *Producer) Run(ctx context.Context) {
	if p.period <= 0 {
		// Manual mode: the caller emits via Emit.
		<-ctx.Done()
		return
	}
	p.emit()
	t := time.NewTicker(p.period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.emit()
		}
	}
}

// Emit sends one heartbeat immediately (useful in tests/manual mode).
func (p *Producer) Emit() { p.emit() }

// Period returns the configured heartbeat period.
func (p *Producer) Period() time.Duration { return p.period }
