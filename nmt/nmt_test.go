package nmt

import (
	"sync"
	"testing"
)

func TestSlaveTransitions(t *testing.T) {
	s := NewSlave(nil, 1)
	if s.State() != StatePreOperational {
		t.Fatalf("initial state = %s", s.State())
	}
	steps := []struct {
		cmd  Command
		want State
	}{
		{CmdStart, StateOperational},
		{CmdStop, StateStopped},
		{CmdEnterPreOperational, StatePreOperational},
		{CmdStart, StateOperational},
		{CmdResetNode, StatePreOperational},
		{CmdResetCommunication, StatePreOperational},
	}
	for _, st := range steps {
		if err := s.ApplyCommand(st.cmd); err != nil {
			t.Fatalf("%s: %v", st.cmd, err)
		}
		if s.State() != st.want {
			t.Fatalf("after %s state=%s want %s", st.cmd, s.State(), st.want)
		}
		t.Logf("command %-22s -> %s", st.cmd, s.State())
	}
}

func TestSlaveRejectsUnknownCommandAndOfflineSet(t *testing.T) {
	s := NewSlave(nil, 1)
	if err := s.ApplyCommand(Command(0x77)); err == nil {
		t.Fatal("unknown command accepted")
	}
	if s.State() != StatePreOperational {
		t.Fatal("state changed after rejected command")
	}
	if err := s.SetState(StateOffline); err == nil {
		t.Fatal("slave must not enter observer Offline")
	}
	t.Log("illegal/unknown commands rejected without state regression")
}

func TestSlaveConcurrentCommandsNoRace(t *testing.T) {
	s := NewSlave(nil, 1)
	var wg sync.WaitGroup
	cmds := []Command{CmdStart, CmdStop, CmdEnterPreOperational}
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				_ = s.ApplyCommand(cmds[(g+i)%len(cmds)])
				_ = s.State()
			}
		}(g)
	}
	wg.Wait()
	final := s.State()
	if final != StateOperational && final != StateStopped && final != StatePreOperational {
		t.Fatalf("illegal final state %s", final)
	}
	t.Logf("concurrent NMT commands settled in a valid state: %s", final)
}
