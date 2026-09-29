// Package sdo implements SDO upload/download (expedited and segmented)
// on the virtual CAN bus, with protocol-level abort codes, retries and
// cancellation.
package sdo

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
)

// Additional abort codes (CiA 301 §7.2.4.13).
const (
	AbortToggleBit    od.AbortCode = 0x05030000
	AbortSdoTimeout   od.AbortCode = 0x05040000
	AbortCmdSpecifier od.AbortCode = 0x05040001
	AbortInvalidBlock od.AbortCode = 0x05040002
	AbortInvalidSeq   od.AbortCode = 0x05040003
	AbortCRCError     od.AbortCode = 0x05040004
	AbortOutOfMemory  od.AbortCode = 0x05040005
	AbortPdoMapping   od.AbortCode = 0x06040041
	AbortPdoLength    od.AbortCode = 0x06040042
	AbortValueTooHigh od.AbortCode = 0x06090030
	AbortValueTooLow  od.AbortCode = 0x06090031
	AbortBusy         od.AbortCode = 0x08000022
)

// AbortError mirrors od.AbortError for callers of this package.
type AbortError = od.AbortError

// ErrInvalidFrame is returned for malformed frames (internal plumbing).
var ErrInvalidFrame = errors.New("sdo: invalid frame")

// Config holds SDO transfer parameters.
type Config struct {
	Timeout        time.Duration
	Retries        int
	SegmentTimeout time.Duration
}

// DefaultConfig returns conservative local-emulation defaults.
func DefaultConfig() Config {
	return Config{Timeout: 500 * time.Millisecond, Retries: 2, SegmentTimeout: 200 * time.Millisecond}
}

// Server is one node's SDO server (request 0x600+id, response 0x580+id).
// One transfer per node is served at a time (single-channel SDO server,
// matching the default single channel 0x1200).
type Server struct {
	bus  *canbus.Bus
	dict *od.Dictionary
	id   uint8
	logf func(string, ...any)

	mu    sync.Mutex // serializes transfers; matches the single channel
	subID uint64
	up    *uploadState
	down  *downloadState
}

// NewServer creates an SDO server.
func NewServer(bus *canbus.Bus, id uint8, dict *od.Dictionary) *Server {
	return &Server{bus: bus, dict: dict, id: id, logf: log.Printf}
}

// SetLogger overrides the logger (tests inject a testing logger).
func (s *Server) SetLogger(f func(string, ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	s.logf = f
}

func (s *Server) sendAbort(index uint16, sub uint8, code od.AbortCode) {
	var f canbus.Frame
	f.ID = ResponseCOB(s.id)
	f.DLC = 8
	f.Data = encodeAbort(index, sub, uint32(code))
	f.Source = s.id
	s.logf("SDO server node %d -> ABORT 0x%04X:%02X code=0x%08X", s.id, index, sub, uint32(code))
	s.bus.Publish(f)
}

func abortToCode(err *od.AbortError) od.AbortCode {
	if err == nil {
		return 0
	}
	return err.Code
}

// Run subscribes to the bus and serves SDO requests addressed to this
// node until ctx ends. Requests for other nodes are ignored (address
// filtering at the receiver).
func (s *Server) Run(ctx context.Context) error {
	id, ch := s.bus.Subscribe(256)
	s.mu.Lock()
	s.subID = id
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.subID == id {
			s.subID = 0
		}
		s.mu.Unlock()
		s.bus.Unsubscribe(id)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-ch:
			if !ok {
				return nil
			}
			if f.ID != RequestCOB(s.id) {
				continue
			}
			s.mu.Lock()
			s.handle(ctx, f)
			s.mu.Unlock()
		}
	}
}

// handle dispatches one client request frame.
func (s *Server) handle(ctx context.Context, f canbus.Frame) {
	if f.DLC != 8 {
		s.sendAbort(0, 0, AbortCmdSpecifier)
		return
	}
	cs := f.Data[0] >> 5
	index := uint16(f.Data[1]) | uint16(f.Data[2])<<8
	sub := f.Data[3]

	switch cs {
	case ccsInitiateUpload:
		s.handleInitiateUpload(index, sub)
	case ccsUploadSegment:
		s.handleUploadSegment(f, index, sub)
	case ccsInitiateDownload:
		s.handleInitiateDownload(f, index, sub)
	case ccsDownloadSegment:
		s.handleDownloadSegment(f)
	case ccsAbort:
		// Client aborted: nothing is installed and we simply idle again.
		s.logf("SDO server node %d received client abort for 0x%04X:%02X", s.id, index, sub)
	default:
		s.sendAbort(index, sub, AbortCmdSpecifier)
	}
	_ = ctx
}

// --- upload state ---

type uploadState struct {
	index  uint16
	sub    uint8
	data   []byte
	pos    int
	toggle byte
}

// --- download state ---

type downloadState struct {
	index  uint16
	sub    uint8
	size   int
	buf    []byte
	toggle byte
}

// Stop releases the bus subscription held by Run.
func (s *Server) Stop() {
	s.mu.Lock()
	id := s.subID
	s.subID = 0
	s.mu.Unlock()
	if id != 0 {
		s.bus.Unsubscribe(id)
	}
}
