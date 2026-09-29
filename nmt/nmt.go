// Package nmt implements CANopen network management state machines,
// NMT command handling and heartbeat production/consumption.
package nmt

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
)

// State is an NMT state (CiA 301 §7.3.2.2). StateOffline is a local
// observer state used by heartbeat consumers; it is never sent on the
// wire and never affects the remote node's own slave FSM.
type State uint8

// NMT states.
const (
	StateInitialising   State = 0
	StateStopped        State = 1
	StateOperational    State = 5
	StatePreOperational State = 127
	StateOffline        State = 128 // local consumer-only: heartbeat timeout
)

// String renders a state for logs.
func (s State) String() string {
	switch s {
	case StateInitialising:
		return "INITIALISING"
	case StateStopped:
		return "STOPPED"
	case StateOperational:
		return "OPERATIONAL"
	case StatePreOperational:
		return "PRE-OPERATIONAL"
	case StateOffline:
		return "OFFLINE"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", uint8(s))
	}
}

// Command is an NMT command specifier (CiA 301 Table 7-3).
type Command uint8

// NMT commands.
const (
	CmdStart               Command = 0x01
	CmdStop                Command = 0x02
	CmdEnterPreOperational Command = 0x80
	CmdResetNode           Command = 0x81
	CmdResetCommunication  Command = 0x82
)

// String renders a command for logs.
func (c Command) String() string {
	switch c {
	case CmdStart:
		return "START(0x01)"
	case CmdStop:
		return "STOP(0x02)"
	case CmdEnterPreOperational:
		return "ENTER-PREOP(0x80)"
	case CmdResetNode:
		return "RESET-NODE(0x81)"
	case CmdResetCommunication:
		return "RESET-COMM(0x82)"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02X)", uint8(c))
	}
}

// ErrIllegalTransition is returned when an NMT transition is not allowed.
var ErrIllegalTransition = errors.New("nmt: illegal state transition")

// errBadState is returned for unknown wire states in a heartbeat.
var errBadState = errors.New("nmt: unknown state code")

// Slave is one node's NMT slave state machine. All access is serialized.
type Slave struct {
	mu  sync.RWMutex
	id  uint8
	bus *canbus.Bus
	st  State
}

// NewSlave creates a slave. Per CiA 301 the node powers up in
// Initialising and autonomously enters Pre-operational.
func NewSlave(bus *canbus.Bus, id uint8) *Slave {
	return &Slave{id: id, bus: bus, st: StatePreOperational}
}

// ID returns the node id.
func (s *Slave) ID() uint8 { return s.id }

// State returns the current state.
func (s *Slave) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.st
}

// apply performs the transition table for a command assuming valid state.
func targetFor(c Command) State {
	switch c {
	case CmdStart:
		return StateOperational
	case CmdStop:
		return StateStopped
	case CmdEnterPreOperational, CmdResetNode, CmdResetCommunication:
		return StatePreOperational
	default:
		return StateInitialising
	}
}

// ApplyCommand applies an NMT command. Reset commands traverse
// Initialising before reaching Pre-operational; invalid commands or
// illegal transitions are rejected without changing the current state.
func (s *Slave) ApplyCommand(c Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch c {
	case CmdStart, CmdStop, CmdEnterPreOperational:
		// Allowed from every non-initialising state.
	case CmdResetNode, CmdResetCommunication:
		// Allowed from every non-initialising state.
	default:
		return fmt.Errorf("nmt: unknown command 0x%02X", uint8(c))
	}

	if s.st == StateInitialising {
		return fmt.Errorf("%w: %s + %s", ErrIllegalTransition, s.st, c)
	}
	if s.st == StateOffline {
		// Offline is a local observer state; a slave itself is never
		// offline. Defensive guard only.
		return fmt.Errorf("%w: %s + %s", ErrIllegalTransition, s.st, c)
	}

	if c == CmdResetNode || c == CmdResetCommunication {
		s.st = StateInitialising
		// Initialising is transient; settle in Pre-operational.
		s.st = StatePreOperational
		return nil
	}
	s.st = targetFor(c)
	return nil
}

// SetState attempts a direct transition. Used by tests/recovery glue;
// Initialising may only be left towards Pre-operational.
func (s *Slave) SetState(st State) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if st == s.st {
		return nil
	}
	if st == StateOffline {
		return fmt.Errorf("%w: a slave cannot enter observer Offline", ErrIllegalTransition)
	}
	if s.st == StateInitialising && st != StatePreOperational {
		return fmt.Errorf("%w: %s -> %s", ErrIllegalTransition, s.st, st)
	}
	s.st = st
	return nil
}

// MonitorStatus is one monitored node's consumer state snapshot.
type MonitorStatus struct {
	ID       uint8
	State    State
	Online   bool
	Deadline time.Time
	LastSeen time.Time
	Misses   int
}

func timeNow() time.Time { return time.Now() }
