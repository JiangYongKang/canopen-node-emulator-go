package canopen

import (
	"errors"
	"sync"
	"time"
)

// SDO COB-ID bases.
const (
	cobSDORxBase = 0x580 // server -> client (response)
	cobSDOTxBase = 0x600 // client -> server (request)
)

// SDOAbortCode is a CiA-301 SDO abort code.
type SDOAbortCode uint32

const (
	AbortToggleBit         SDOAbortCode = 0x05030000
	AbortTimeout           SDOAbortCode = 0x05040000
	AbortInvalidCmd        SDOAbortCode = 0x05040001
	AbortInvalidBlockSize  SDOAbortCode = 0x05040002
	AbortInvalidSeq        SDOAbortCode = 0x05040003
	AbortCRC               SDOAbortCode = 0x05040004
	AbortOutOfMemory       SDOAbortCode = 0x05040005
	AbortUnsupportedAccess SDOAbortCode = 0x06010000
	AbortWriteOnlyRead     SDOAbortCode = 0x06010001
	AbortReadOnlyWrite     SDOAbortCode = 0x06010002
	AbortObjectNotFound    SDOAbortCode = 0x06020000
	AbortCannotMapPDO      SDOAbortCode = 0x06030000
	AbortPDOTooLong        SDOAbortCode = 0x06040041
	AbortHardware          SDOAbortCode = 0x06060000
	AbortTypeLenMismatch   SDOAbortCode = 0x06070010
	AbortTypeLenTooHigh    SDOAbortCode = 0x06070012
	AbortTypeLenTooShort   SDOAbortCode = 0x06070013
	AbortNoSubIndex        SDOAbortCode = 0x06090011
	AbortGeneral           SDOAbortCode = 0x08000000
	AbortStoreUnsupported  SDOAbortCode = 0x08000020
	AbortLocalControl      SDOAbortCode = 0x08000022
)

func (c SDOAbortCode) String() string {
	switch c {
	case AbortToggleBit:
		return "TOGGLE_BIT_NOT_ALTERNATED"
	case AbortTimeout:
		return "TIMEOUT"
	case AbortInvalidCmd:
		return "INVALID_COMMAND_SPECIFIER"
	case AbortInvalidSeq:
		return "INVALID_SEQUENCE_NUMBER"
	case AbortUnsupportedAccess:
		return "UNSUPPORTED_ACCESS"
	case AbortWriteOnlyRead:
		return "OBJECT_IS_WRITE_ONLY"
	case AbortReadOnlyWrite:
		return "OBJECT_IS_READ_ONLY"
	case AbortObjectNotFound:
		return "OBJECT_NOT_FOUND"
	case AbortTypeLenMismatch:
		return "TYPE_LENGTH_MISMATCH"
	case AbortTypeLenTooHigh:
		return "DATA_TOO_LONG"
	case AbortTypeLenTooShort:
		return "DATA_TOO_SHORT"
	case AbortNoSubIndex:
		return "SUBINDEX_NOT_EXIST"
	default:
		return "ABORT_0x08000000"
	}
}

// SDOAbortError lets callers distinguish protocol-level failure reasons.
type SDOAbortError struct {
	Code  SDOAbortCode
	Index uint16
	Sub   uint8
}

func (e *SDOAbortError) Error() string {
	return "canopen: SDO abort " + e.Code.String() + " @" + odRef(e.Index, e.Sub)
}

// sdoTimeoutErr marks a local client timeout. It is distinct from a wire
// protocol abort because a timeout is transient and eligible for retry.
type sdoTimeoutErr struct{}

func (sdoTimeoutErr) Error() string        { return "canopen: SDO transfer timed out" }
func (sdoTimeoutErr) Is(target error) bool { return target == ErrSDOTimeout }

// AsAbort extracts the SDO abort code from an error, if it is one.
func AsAbort(err error) (SDOAbortCode, bool) {
	var ae *SDOAbortError
	if errors.As(err, &ae) {
		return ae.Code, true
	}
	return 0, false
}

func odRef(index uint16, sub uint8) string {
	const hexd = "0123456789abcdef"
	return "0x" + string([]byte{
		hexd[(index>>12)&0xF], hexd[(index>>8)&0xF], hexd[(index>>4)&0xF], hexd[index&0xF],
	}) + "/0x" + string([]byte{hexd[(sub>>4)&0xF], hexd[sub&0xF]})
}

// abortCodeFor maps an OD access error to the matching CiA-301 abort code.
func abortCodeFor(err error) SDOAbortCode {
	switch {
	case errors.Is(err, ErrNotFound):
		return AbortObjectNotFound
	case errors.Is(err, ErrNoSubIndex):
		return AbortNoSubIndex
	case errors.Is(err, ErrReadOnly):
		return AbortReadOnlyWrite
	case errors.Is(err, ErrWriteOnly):
		return AbortWriteOnlyRead
	case errors.Is(err, ErrLengthMismatch), errors.Is(err, ErrTypeMismatch):
		return AbortTypeLenMismatch
	default:
		return AbortGeneral
	}
}

// SDOConfig configures an SDO client.
type SDOConfig struct {
	Timeout    time.Duration
	MaxRetries int
	Clock      Clock
	Logger     *Logger
}

func (cfg SDOConfig) withDefaults() SDOConfig {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 500 * time.Millisecond
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}
	return cfg
}

// ---- Server-side session state ----

type sdoPhase int

const (
	sdoIdle  sdoPhase = iota
	sdoDlSeg          // segmented download in progress
	sdoUlSeg          // segmented upload in progress
)

type sdoSession struct {
	phase  sdoPhase
	index  uint16
	sub    uint8
	buf    []byte
	toggle byte // expected next toggle bit
}

// SDOServer answers expedited and segmented uploads/downloads for one node.
// It processes one client at a time (per CiA-301 default SDO channel) and
// commits segmented downloads to the OD only on a valid end-block, so failed
// transfers never leave a partial write.
type SDOServer struct {
	nodeID uint8
	bus    *VirtualBus
	od     *ObjectDict
	clock  Clock
	logger *Logger
	subID  int

	mu      sync.Mutex
	session sdoSession
	active  bool
}

// NewSDOServer creates a server for nodeID.
func NewSDOServer(nodeID uint8, bus *VirtualBus, od *ObjectDict, clock Clock, logger *Logger) *SDOServer {
	if clock == nil {
		clock = RealClock{}
	}
	return &SDOServer{nodeID: nodeID, bus: bus, od: od, clock: clock, logger: logger}
}

// Start subscribes to this node's SDO request COB-ID (0x600+id).
func (s *SDOServer) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active {
		return
	}
	s.active = true
	s.subID = s.bus.Subscribe(s.nodeID, 0x7FF, cobSDOTxBase+uint32(s.nodeID), s.handle)
}

// Stop unsubscribes and discards any in-progress session.
func (s *SDOServer) Stop() {
	s.mu.Lock()
	if !s.active {
		s.mu.Unlock()
		return
	}
	s.active = false
	sub := s.subID
	s.session = sdoSession{}
	s.mu.Unlock()
	s.bus.Unsubscribe(sub)
}

// Active reports whether the server is subscribed.
func (s *SDOServer) Active() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.active
}

func (s *SDOServer) respond(f Frame) {
	s.bus.PublishFrom(s.nodeID, f)
	if s.logger != nil {
		s.logger.Framef("TX", f, "SDO response node "+itoa(int(s.nodeID)))
	}
}

func (s *SDOServer) abort(index uint16, sub uint8, code SDOAbortCode) {
	f := Frame{CANID: cobSDORxBase + uint32(s.nodeID), DLC: 8}
	f.Data[0] = 0x80
	putIndexSub(&f, index, sub)
	f.Data[4] = byte(code)
	f.Data[5] = byte(code >> 8)
	f.Data[6] = byte(code >> 16)
	f.Data[7] = byte(code >> 24)
	s.respond(f)
}

func putIndexSub(f *Frame, index uint16, sub uint8) {
	f.Data[1] = byte(index)
	f.Data[2] = byte(index >> 8)
	f.Data[3] = sub
}

func indexSubOf(f Frame) (uint16, uint8) {
	return uint16(f.Data[1]) | uint16(f.Data[2])<<8, f.Data[3]
}

// handle is invoked from the bus delivery goroutine; all session mutation is
// serialised by the server mutex.
func (s *SDOServer) handle(f Frame) {
	if f.DLC < 4 {
		return
	}
	if s.logger != nil {
		s.logger.Framef("RX", f, "SDO request node "+itoa(int(s.nodeID)))
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cmd := f.Data[0]
	if cmd == 0x80 { // client abort
		s.session = sdoSession{}
		if s.logger != nil {
			s.logger.Infof("node %d: SDO client aborted transfer; partial data discarded", s.nodeID)
		}
		return
	}

	// A pending segmented session takes precedence, because its segment
	// command byte shares space with other specifiers (download segments use
	// the toggle bit; upload segment requests are 0x60).
	switch s.session.phase {
	case sdoDlSeg:
		s.handleDownloadSegment(f)
		return
	case sdoUlSeg:
		s.handleUploadSegment(f)
		return
	}

	switch {
	case cmd == 0x40: // initiate upload
		s.handleUploadInit(f)
	case cmd&0x20 != 0: // initiate download (expedited 0x23/0x2B.. or segmented 0x21)
		s.handleDownloadInit(f)
	default:
		index, sub := indexSubOf(f)
		s.abort(index, sub, AbortInvalidCmd)
	}
}

func (s *SDOServer) handleDownloadInit(f Frame) {
	index, sub := indexSubOf(f)
	expedited := f.Data[0]&0x02 != 0
	sizeIndicated := f.Data[0]&0x01 != 0
	if expedited {
		var n int
		if sizeIndicated {
			n = 4 - int((f.Data[0]>>2)&0x03)
		} else {
			n = 4
		}
		data := f.Data[4 : 4+n]
		if err := s.od.WriteRaw(index, sub, data); err != nil {
			s.abort(index, sub, abortCodeFor(err))
			return
		}
		var r Frame
		r.CANID, r.DLC = cobSDORxBase+uint32(s.nodeID), 8
		r.Data[0] = 0x60
		putIndexSub(&r, index, sub)
		s.respond(r)
		return
	}
	// segmented download: validate object + access now, but do NOT write.
	meta, ok := s.od.Meta(index, sub)
	if !ok {
		s.abort(index, sub, AbortObjectNotFound)
		return
	}
	if !meta.Access.writable() {
		s.abort(index, sub, AbortReadOnlyWrite)
		return
	}
	s.session = sdoSession{phase: sdoDlSeg, index: index, sub: sub, toggle: 0}
	var r Frame
	r.CANID, r.DLC = cobSDORxBase+uint32(s.nodeID), 8
	r.Data[0] = 0x60
	putIndexSub(&r, index, sub)
	s.respond(r)
}

func (s *SDOServer) handleDownloadSegment(f Frame) {
	sess := &s.session
	if sess.phase != sdoDlSeg {
		index, sub := indexSubOf(f)
		s.abort(index, sub, AbortInvalidCmd)
		return
	}
	toggle := f.Data[0] & 0x10
	if toggle != (sess.toggle << 4) {
		s.abort(sess.index, sess.sub, AbortToggleBit)
		s.session = sdoSession{}
		return
	}
	last := f.Data[0]&0x01 != 0
	n := 7 - int((f.Data[0]>>1)&0x07)
	sess.buf = append(sess.buf, f.Data[1:1+n]...)
	sess.toggle ^= 1

	if last {
		// Atomic commit: WriteRaw validates length/access and replaces the
		// value in one step. On failure nothing has been written and the
		// buffered partial data is discarded.
		if err := s.od.WriteRaw(sess.index, sess.sub, sess.buf); err != nil {
			s.abort(sess.index, sess.sub, abortCodeFor(err))
			s.session = sdoSession{}
			return
		}
		if s.logger != nil {
			s.logger.Infof("node %d: segmented download committed %d bytes @%s",
				s.nodeID, len(sess.buf), odRef(sess.index, sess.sub))
		}
		s.session = sdoSession{}
	}
	var r Frame
	r.CANID, r.DLC = cobSDORxBase+uint32(s.nodeID), 8
	// Download-segment server ack (0x20) echoes the segment's toggle bit.
	r.Data[0] = 0x20 | toggle
	s.respond(r)
}

func (s *SDOServer) handleUploadInit(f Frame) {
	index, sub := indexSubOf(f)
	data, err := s.od.ReadRaw(index, sub)
	if err != nil {
		s.abort(index, sub, abortCodeFor(err))
		return
	}
	if len(data) <= 4 {
		// expedited upload
		var r Frame
		r.CANID, r.DLC = cobSDORxBase+uint32(s.nodeID), 8
		// expedited: 0x43 base + ((4-n)<<2) => 0x43(n=4)..0x4F(n=1)
		r.Data[0] = 0x43 | byte((4-len(data))&0x03)<<2
		putIndexSub(&r, index, sub)
		copy(r.Data[4:], data)
		s.respond(r)
		return
	}
	// segmented upload initiate
	s.session = sdoSession{phase: sdoUlSeg, index: index, sub: sub, buf: data, toggle: 0}
	var r Frame
	r.CANID, r.DLC = cobSDORxBase+uint32(s.nodeID), 8
	r.Data[0] = 0x41 // segmented transfer
	putIndexSub(&r, index, sub)
	putU32(r.Data[4:], uint32(len(data)))
	s.respond(r)
}

func (s *SDOServer) handleUploadSegment(f Frame) {
	sess := &s.session
	if sess.phase != sdoUlSeg {
		index, sub := indexSubOf(f)
		s.abort(index, sub, AbortInvalidCmd)
		return
	}
	toggle := f.Data[0] & 0x10
	if toggle != (sess.toggle << 4) {
		s.abort(sess.index, sess.sub, AbortToggleBit)
		s.session = sdoSession{}
		return
	}
	chunk := sess.buf
	if len(chunk) > 7 {
		chunk = chunk[:7]
	}
	rest := sess.buf[len(chunk):]
	var r Frame
	r.CANID, r.DLC = cobSDORxBase+uint32(s.nodeID), 8
	r.Data[0] = 0x00 | (sess.toggle<<4)&0x10
	if len(rest) == 0 {
		r.Data[0] |= 0x01
	}
	r.Data[0] |= byte((7-len(chunk))&0x07) << 1
	copy(r.Data[1:], chunk)
	sess.buf = rest
	sess.toggle ^= 1
	if len(rest) == 0 {
		s.session = sdoSession{}
	}
	s.respond(r)
}

func putU32(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

// ---- Client ----

// sdoCall is one in-flight client transfer. At most one transfer targets a
// given server at a time, which matches the single default SDO channel.
type sdoCall struct {
	resp chan Frame
}

// SDOClient performs SDO transfers against remote servers: expedited and
// segmented upload/download, with timeout and bounded retry. A transfer that
// is interrupted (timeout, abort, bus fault) is torn down cleanly and a fresh
// initiate is sent on retry, so partial segments never survive.
type SDOClient struct {
	bus    *VirtualBus
	cfg    SDOConfig
	logger *Logger

	mu     sync.Mutex
	subID  int
	active bool
	calls  map[uint8]*sdoCall // keyed by server node id
}

// NewSDOClient creates a client subscribed to all server response COB-IDs.
func NewSDOClient(bus *VirtualBus, cfg SDOConfig) *SDOClient {
	cfg = cfg.withDefaults()
	c := &SDOClient{bus: bus, cfg: cfg, logger: cfg.Logger, calls: make(map[uint8]*sdoCall)}
	c.active = true
	// mask 0x780 keeps COBs 0x580..0x5FF; filter 0x580.
	c.subID = bus.Subscribe(0, 0x780, cobSDORxBase, c.onFrame)
	return c
}

// Stop unsubscribes the client.
func (c *SDOClient) Stop() {
	c.mu.Lock()
	if !c.active {
		c.mu.Unlock()
		return
	}
	c.active = false
	sub := c.subID
	calls := c.calls
	c.calls = make(map[uint8]*sdoCall)
	c.mu.Unlock()
	c.bus.Unsubscribe(sub)
	for _, call := range calls {
		close(call.resp)
	}
}

func (c *SDOClient) onFrame(f Frame) {
	server := uint8(f.CANID & 0x7F)
	c.mu.Lock()
	call := c.calls[server]
	c.mu.Unlock()
	if call == nil || f.DLC < 1 {
		return
	}
	if c.logger != nil {
		c.logger.Framef("RX", f, "SDO client response from node "+itoa(int(server)))
	}
	select {
	case call.resp <- f:
	default:
		// stale/unexpected frame: ignore rather than corrupt a new transfer.
		if c.logger != nil {
			c.logger.Infof("SDO client: stale response from node %d ignored", server)
		}
	}
}

func (c *SDOClient) begin(server uint8) (*sdoCall, func()) {
	call := &sdoCall{resp: make(chan Frame, 4)}
	c.mu.Lock()
	c.calls[server] = call
	c.mu.Unlock()
	return call, func() {
		c.mu.Lock()
		if c.calls[server] == call {
			delete(c.calls, server)
		}
		c.mu.Unlock()
	}
}

func (c *SDOClient) send(server uint8, f Frame) {
	f.CANID = cobSDOTxBase + uint32(server)
	if f.DLC == 0 {
		f.DLC = 8
	}
	c.bus.PublishFrom(0, f)
	if c.logger != nil {
		c.logger.Framef("TX", f, "SDO client request to node "+itoa(int(server)))
	}
}

// Read uploads an object from serverID.
func (c *SDOClient) Read(serverID uint8, index uint16, sub uint8) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			if c.logger != nil {
				c.logger.Infof("SDO read @%s retry %d after: %v", odRef(index, sub), attempt, lastErr)
			}
		}
		data, err := c.readOnce(serverID, index, sub)
		if err == nil {
			return data, nil
		}
		lastErr = err
		// A wire protocol abort is definitive; a timeout is transient.
		var ae *SDOAbortError
		if errors.As(err, &ae) {
			return nil, err
		}
	}
	return nil, lastErr
}

// Write downloads data to serverID. Values up to 4 bytes use expedited
// transfer; longer values use segmented transfer.
func (c *SDOClient) Write(serverID uint8, index uint16, sub uint8, data []byte) error {
	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			if c.logger != nil {
				c.logger.Infof("SDO write @%s retry %d after: %v", odRef(index, sub), attempt, lastErr)
			}
		}
		err := c.writeOnce(serverID, index, sub, data)
		if err == nil {
			return nil
		}
		lastErr = err
		var ae *SDOAbortError
		if errors.As(err, &ae) {
			return err
		}
	}
	return lastErr
}

func (c *SDOClient) readOnce(serverID uint8, index uint16, sub uint8) ([]byte, error) {
	call, end := c.begin(serverID)
	defer end()

	var init Frame
	init.DLC = 8
	init.Data[0] = 0x40
	putIndexSub(&init, index, sub)
	c.send(serverID, init)

	resp, err := c.wait(call)
	if err != nil {
		return nil, err
	}
	if err := checkAbort(resp, index, sub); err != nil {
		return nil, err
	}
	switch resp.Data[0] {
	case 0x43, 0x47, 0x4B, 0x4F:
		// expedited upload (size indicated): n=4..1
		n := 4 - int((resp.Data[0]>>2)&0x03)
		return append([]byte(nil), resp.Data[4:4+n]...), nil
	default:
		// segmented upload (initiate response 0x41)
		total := int(u32of(resp.Data[4:]))
		out := make([]byte, 0, total)
		var toggle byte
		for {
			var req Frame
			req.DLC = 8
			req.Data[0] = 0x60 | (toggle<<4)&0x10
			c.send(serverID, req)
			seg, err := c.wait(call)
			if err != nil {
				return nil, err
			}
			if err := checkAbort(seg, index, sub); err != nil {
				return nil, err
			}
			if seg.Data[0]&0x10 != (toggle<<4)&0x10 {
				return nil, &SDOAbortError{Code: AbortToggleBit, Index: index, Sub: sub}
			}
			n := 7 - int((seg.Data[0]>>1)&0x07)
			out = append(out, seg.Data[1:1+n]...)
			last := seg.Data[0]&0x01 != 0
			toggle ^= 1
			if last {
				break
			}
		}
		return out, nil
	}
}

func (c *SDOClient) writeOnce(serverID uint8, index uint16, sub uint8, data []byte) error {
	call, end := c.begin(serverID)
	defer end()

	if len(data) <= 4 {
		var init Frame
		init.DLC = 8
		init.Data[0] = 0x23 | byte((4-len(data))&0x03)<<2 // expedited, size indicated
		putIndexSub(&init, index, sub)
		copy(init.Data[4:], data)
		c.send(serverID, init)
		resp, err := c.wait(call)
		if err != nil {
			return err
		}
		return checkAbort(resp, index, sub)
	}

	// segmented download initiate
	var init Frame
	init.DLC = 8
	init.Data[0] = 0x21 // segmented, size indicated
	putIndexSub(&init, index, sub)
	putU32(init.Data[4:], uint32(len(data)))
	c.send(serverID, init)
	resp, err := c.wait(call)
	if err != nil {
		return err
	}
	if err := checkAbort(resp, index, sub); err != nil {
		return err
	}
	var toggle byte
	for off := 0; off < len(data); {
		chunk := data[off:]
		last := false
		if len(chunk) > 7 {
			chunk = chunk[:7]
		} else {
			last = true
		}
		var seg Frame
		seg.DLC = 8
		seg.Data[0] = (toggle << 4) | byte((7-len(chunk))&0x07)<<1
		if last {
			seg.Data[0] |= 0x01
		}
		copy(seg.Data[1:], chunk)
		c.send(serverID, seg)
		resp, err := c.wait(call)
		if err != nil {
			return err
		}
		if err := checkAbort(resp, index, sub); err != nil {
			return err
		}
		if resp.Data[0]&0x10 != (toggle<<4)&0x10 {
			return &SDOAbortError{Code: AbortToggleBit, Index: index, Sub: sub}
		}
		toggle ^= 1
		off += len(chunk)
	}
	return nil
}

func (c *SDOClient) wait(call *sdoCall) (Frame, error) {
	select {
	case f, ok := <-call.resp:
		if !ok {
			return Frame{}, ErrSDOAborted
		}
		return f, nil
	case <-c.cfg.Clock.After(c.cfg.Timeout):
		return Frame{}, sdoTimeoutErr{}
	}
}

func checkAbort(f Frame, index uint16, sub uint8) error {
	if f.DLC >= 1 && f.Data[0] == 0x80 {
		code := SDOAbortCode(u32of(f.Data[4:]))
		return &SDOAbortError{Code: code, Index: index, Sub: sub}
	}
	return nil
}

func u32of(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}
