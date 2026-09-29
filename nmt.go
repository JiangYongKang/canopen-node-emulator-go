package canopen

import (
	"sync"
	"time"
)

// NMTState is a CiA-301 NMT slave state. The numeric values match the state
// byte carried by heartbeat/boot-up frames.
type NMTState uint8

const (
	StateInitialising   NMTState = 0
	StateStopped        NMTState = 4
	StateOperational    NMTState = 5
	StatePreOperational NMTState = 127
)

func (s NMTState) String() string {
	switch s {
	case StateInitialising:
		return "INITIALISING"
	case StateStopped:
		return "STOPPED"
	case StateOperational:
		return "OPERATIONAL"
	case StatePreOperational:
		return "PRE-OPERATIONAL"
	default:
		return "UNKNOWN"
	}
}

// NMTCommand is the command specifier in an NMT master frame (COB 0x000).
type NMTCommand uint8

const (
	CmdEnterOperational    NMTCommand = 1
	CmdEnterStopped        NMTCommand = 2
	CmdEnterPreOperational NMTCommand = 128
	CmdResetNode           NMTCommand = 129
	CmdResetCommunication  NMTCommand = 130
)

// NMTTransition records one state change (append-only history).
type NMTTransition struct {
	From NMTState
	To   NMTState
	At   time.Time
}

// NMTMachine is the per-node state machine. Transitions are serialised by a
// mutex: concurrent commands are applied one at a time, the history is
// append-only and a rejected/illegal transition can never roll the state
// backwards.
type NMTMachine struct {
	mu     sync.Mutex
	nodeID uint8
	state  NMTState
	log    []NMTTransition
	clock  Clock
	logger *Logger
}

// NewNMTMachine creates a machine for nodeID, starting in INITIALISING.
func NewNMTMachine(nodeID uint8) *NMTMachine {
	return &NMTMachine{nodeID: nodeID, state: StateInitialising, clock: RealClock{}}
}

func (m *NMTMachine) withClock(c Clock, l *Logger) *NMTMachine {
	m.clock = c
	m.logger = l
	return m
}

// State returns the current state.
func (m *NMTMachine) State() NMTState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Transition applies state to if it is legal from the current state.
// Self-transitions succeed but report changed=false. It returns the resulting
// state and whether a real change (plus history entry) occurred.
func (m *NMTMachine) Transition(to NMTState) (NMTState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if to == m.state {
		return m.state, false
	}
	if !legalTransition(m.state, to) {
		if m.logger != nil {
			m.logger.Infof("node %d: reject NMT %s -> %s (illegal), stay %s",
				m.nodeID, m.state, to, m.state)
		}
		return m.state, false
	}
	from := m.state
	m.state = to
	m.log = append(m.log, NMTTransition{From: from, To: to, At: m.clock.Now()})
	if m.logger != nil {
		m.logger.Infof("node %d: NMT %s -> %s", m.nodeID, from, to)
	}
	return to, true
}

// TransitionCAS applies to only if the state currently equals from.
// It is the compare-and-set primitive used for race-free coordinated changes.
func (m *NMTMachine) TransitionCAS(from, to NMTState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != from || !legalTransition(m.state, to) {
		return false
	}
	if to == m.state {
		return true
	}
	m.log = append(m.log, NMTTransition{From: from, To: to, At: m.clock.Now()})
	m.state = to
	return true
}

func legalTransition(from, to NMTState) bool {
	switch from {
	case StateInitialising:
		return to == StatePreOperational
	case StatePreOperational:
		return to == StateOperational || to == StateStopped
	case StateOperational:
		return to == StateStopped || to == StatePreOperational
	case StateStopped:
		return to == StateOperational || to == StatePreOperational
	}
	return false
}

// ApplyCommand maps an NMT master command to transitions. Reset commands pass
// through INITIALISING and end in PRE-OPERATIONAL (both are recorded).
// It returns the resulting state and false for an unknown command.
func (m *NMTMachine) ApplyCommand(cmd NMTCommand) (NMTState, bool) {
	switch cmd {
	case CmdEnterOperational:
		s, _ := m.Transition(StateOperational)
		return s, true
	case CmdEnterStopped:
		s, _ := m.Transition(StateStopped)
		return s, true
	case CmdEnterPreOperational:
		s, _ := m.Transition(StatePreOperational)
		return s, true
	case CmdResetNode, CmdResetCommunication:
		m.mu.Lock()
		now := m.clock.Now()
		from := m.state
		m.state = StateInitialising
		m.log = append(m.log, NMTTransition{From: from, To: StateInitialising, At: now})
		m.state = StatePreOperational
		m.log = append(m.log, NMTTransition{From: StateInitialising, To: StatePreOperational, At: now})
		m.mu.Unlock()
		if m.logger != nil {
			m.logger.Infof("node %d: NMT reset (%s -> INITIALISING -> PRE-OPERATIONAL)", m.nodeID, from)
		}
		return StatePreOperational, true
	}
	return m.State(), false
}

// History returns a copy of the append-only transition history.
func (m *NMTMachine) History() []NMTTransition {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]NMTTransition, len(m.log))
	copy(out, m.log)
	return out
}

// heartbeatProducer cyclically sends COB 0x700+id carrying the NMT state byte.
type heartbeatProducer struct {
	nodeID uint8
	bus    *VirtualBus
	m      *NMTMachine
	clock  Clock
	logger *Logger
	period time.Duration

	stop chan struct{}
	done chan struct{}
	mu   sync.Mutex
	on   bool
}

func newHeartbeatProducer(nodeID uint8, bus *VirtualBus, m *NMTMachine, period time.Duration, clock Clock, logger *Logger) *heartbeatProducer {
	return &heartbeatProducer{nodeID: nodeID, bus: bus, m: m, period: period, clock: clock, logger: logger}
}

// PublishBootup sends the single boot-up frame (data byte 0x00).
func (p *heartbeatProducer) PublishBootup() {
	f := Frame{CANID: 0x700 + uint32(p.nodeID), DLC: 1}
	p.bus.PublishFrom(p.nodeID, f)
	if p.logger != nil {
		p.logger.Framef("TX", f, "boot-up")
	}
}

func (p *heartbeatProducer) Start() {
	if p.period <= 0 {
		return
	}
	p.mu.Lock()
	if p.on {
		p.mu.Unlock()
		return
	}
	p.on = true
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	p.mu.Unlock()
	go p.loop()
}

func (p *heartbeatProducer) Stop() {
	p.mu.Lock()
	if !p.on {
		p.mu.Unlock()
		return
	}
	p.on = false
	close(p.stop)
	done := p.done
	p.mu.Unlock()
	<-done
}

func (p *heartbeatProducer) loop() {
	defer close(p.done)
	tk := p.clock.NewTicker(p.period)
	defer tk.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-tk.C():
		}
		state := p.m.State()
		f := Frame{CANID: 0x700 + uint32(p.nodeID), DLC: 1, Data: [8]byte{byte(state)}, SentAt: p.clock.Now()}
		p.bus.PublishFrom(p.nodeID, f)
		if p.logger != nil {
			p.logger.Framef("TX", f, "heartbeat state="+state.String())
		}
	}
}

// MonitorKind classifies a heartbeat monitor event.
type MonitorKind int

const (
	EvHeartbeat MonitorKind = iota
	EvTimeout
	EvRecovered
)

func (k MonitorKind) String() string {
	switch k {
	case EvHeartbeat:
		return "HEARTBEAT"
	case EvTimeout:
		return "TIMEOUT"
	case EvRecovered:
		return "RECOVERED"
	}
	return "?"
}

// MonitorEvent is emitted on heartbeats, timeout and recovery.
type MonitorEvent struct {
	NodeID uint8
	Kind   MonitorKind
	State  NMTState
	At     time.Time
}

// HeartbeatMonitor consumes heartbeat frames (COB 0x700+id) from one node and
// detects producer timeouts. A timeout is latched exactly once; a later
// (possibly long-overdue) heartbeat clears it and is reported as recovery,
// never as an error. Duplicate/repeated heartbeats are always accepted.
type HeartbeatMonitor struct {
	bus     *VirtualBus
	watched uint8
	timeout time.Duration
	check   time.Duration
	clock   Clock
	logger  *Logger
	onEvent func(MonitorEvent)
	subID   int

	mu       sync.Mutex
	last     time.Time
	state    NMTState
	alive    bool // a heartbeat has been seen and no timeout is pending
	timedOut bool
	started  bool
	stop     chan struct{}
	done     chan struct{}
}

// NewHeartbeatMonitor builds a monitor. onEvent must be non-nil and must not
// block (it is invoked from the monitor's goroutines).
func NewHeartbeatMonitor(bus *VirtualBus, watched uint8, timeout time.Duration, clock Clock, onEvent func(MonitorEvent)) *HeartbeatMonitor {
	h := &HeartbeatMonitor{
		bus:     bus,
		watched: watched,
		timeout: timeout,
		check:   timeout / 4,
		clock:   clock,
		onEvent: onEvent,
	}
	if h.check <= 0 {
		h.check = timeout
	}
	if h.timeout <= 0 {
		h.timeout = time.Second
	}
	return h
}

func (h *HeartbeatMonitor) withLogger(l *Logger) *HeartbeatMonitor { h.logger = l; return h }

// Start subscribes to the node's heartbeat COB-ID and runs the timeout loop.
func (h *HeartbeatMonitor) Start() {
	h.mu.Lock()
	if h.started {
		h.mu.Unlock()
		return
	}
	h.started = true
	h.stop = make(chan struct{})
	h.done = make(chan struct{})
	h.mu.Unlock()

	h.subID = h.bus.Subscribe(0, 0x7FF, 0x700+uint32(h.watched), h.onFrame)
	go h.loop()
}

// Stop unsubscribes and stops the timeout loop.
func (h *HeartbeatMonitor) Stop() {
	h.mu.Lock()
	if !h.started {
		h.mu.Unlock()
		return
	}
	h.started = false
	sub := h.subID
	close(h.stop)
	done := h.done
	h.mu.Unlock()
	h.bus.Unsubscribe(sub)
	<-done
}

// Alive reports whether the producer is currently considered alive.
func (h *HeartbeatMonitor) Alive() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.alive
}

func (h *HeartbeatMonitor) onFrame(f Frame) {
	now := h.clock.Now()
	var evs []MonitorEvent
	h.mu.Lock()
	wasTimeout := h.timedOut
	h.last = now
	h.timedOut = false
	h.alive = true
	if f.DLC > 0 {
		h.state = NMTState(f.Data[0])
	}
	st := h.state
	if wasTimeout {
		evs = append(evs, MonitorEvent{h.watched, EvRecovered, st, now})
	} else {
		evs = append(evs, MonitorEvent{h.watched, EvHeartbeat, st, now})
	}
	h.mu.Unlock()
	if h.logger != nil {
		h.logger.Framef("RX", f, "heartbeat accepted from node "+itoa(int(h.watched)))
	}
	for _, ev := range evs {
		h.emit(ev)
	}
}

func (h *HeartbeatMonitor) loop() {
	defer close(h.done)
	tk := h.clock.NewTicker(h.check)
	defer tk.Stop()
	for {
		select {
		case <-h.stop:
			return
		case <-tk.C():
			h.evaluate()
		}
	}
}

func (h *HeartbeatMonitor) evaluate() {
	now := h.clock.Now()
	var ev *MonitorEvent
	h.mu.Lock()
	if h.alive && !h.timedOut && now.Sub(h.last) > h.timeout {
		h.timedOut = true
		h.alive = false
		st := h.state
		ev = &MonitorEvent{h.watched, EvTimeout, st, now}
	}
	h.mu.Unlock()
	if ev != nil {
		if h.logger != nil {
			h.logger.Infof("node %d: heartbeat TIMEOUT after %s (last state=%s)",
				h.watched, h.timeout, ev.State)
		}
		h.emit(*ev)
	}
}

func (h *HeartbeatMonitor) emit(ev MonitorEvent) {
	defer func() { _ = recover() }()
	h.onEvent(ev)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		b[n] = '-'
	}
	return string(b[n:])
}
