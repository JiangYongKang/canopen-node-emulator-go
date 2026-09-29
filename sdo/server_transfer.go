package sdo

import (
	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
)

// reply publishes an 8-byte SDO response frame.
func (s *Server) reply(d [8]byte) {
	var f canbus.Frame
	f.ID = ResponseCOB(s.id)
	f.DLC = 8
	f.Data = d
	f.Source = s.id
	s.bus.Publish(f)
}

func (s *Server) clearUpload()   { s.up = nil }
func (s *Server) clearDownload() { s.down = nil }

// handleInitiateUpload serves "client initiate upload" (CCS=2).
func (s *Server) handleInitiateUpload(index uint16, sub uint8) {
	// Any half-finought transfer is discarded and restarted cleanly.
	s.clearUpload()
	s.clearDownload()

	data, err := s.dict.GetRaw(index, sub)
	if err != nil {
		s.sendAbort(index, sub, err.Code)
		return
	}
	entry, lerr := s.dict.Lookup(index, sub)
	if lerr != nil {
		s.sendAbort(index, sub, lerr.Code)
		return
	}

	if len(data) <= 4 {
		// Expedited upload.
		var d [8]byte
		d[0] = scsInitiateUpload<<5 | bitExpedited | bitSizeIndicated | expeditedN(len(data))
		d[1] = byte(index)
		d[2] = byte(index >> 8)
		d[3] = sub
		copy(d[4:], data)
		s.logf("SDO server node %d upload expedited 0x%04X:%02X len=%d type=%d",
			s.id, index, sub, len(data), entry.Type)
		s.reply(d)
		return
	}

	// Segmented upload: size indicated in bytes 4..7.
	var d [8]byte
	d[0] = scsInitiateUpload<<5 | bitSizeIndicated
	d[1] = byte(index)
	d[2] = byte(index >> 8)
	d[3] = sub
	n := uint32(len(data))
	d[4] = byte(n)
	d[5] = byte(n >> 8)
	d[6] = byte(n >> 16)
	d[7] = byte(n >> 24)
	s.up = &uploadState{index: index, sub: sub, data: data}
	s.logf("SDO server node %d upload segmented 0x%04X:%02X len=%d", s.id, index, sub, len(data))
	s.reply(d)
}

// handleUploadSegment serves one "client upload segment" (CCS=3).
func (s *Server) handleUploadSegment(f canbus.Frame, index uint16, sub uint8) {
	if s.up == nil {
		s.sendAbort(index, sub, AbortCmdSpecifier)
		return
	}
	gotToggle := f.Data[0] & bitToggle
	if gotToggle != s.up.toggle {
		// Protocol violation: abort the transfer.
		s.sendAbort(s.up.index, s.up.sub, AbortToggleBit)
		s.clearUpload()
		return
	}
	var payload []byte
	last := false
	if s.up.pos < len(s.up.data) {
		end := s.up.pos + 7
		if end >= len(s.up.data) {
			end = len(s.up.data)
			last = true
		}
		payload = s.up.data[s.up.pos:end]
	} else {
		last = true
	}

	var d [8]byte
	d[0] = scsUploadSegment << 5
	if last {
		d[0] |= bitNoMoreSegments
	}
	d[0] |= s.up.toggle
	d[0] |= segmentN(len(payload))
	copy(d[1:], payload)
	s.reply(d)

	s.up.pos += len(payload)
	s.up.toggle ^= bitToggle
	if last {
		s.logf("SDO server node %d upload segment finished 0x%04X:%02X", s.id, s.up.index, s.up.sub)
		s.clearUpload()
	}
}

// handleInitiateDownload serves "client initiate download" (CCS=1).
func (s *Server) handleInitiateDownload(f canbus.Frame, index uint16, sub uint8) {
	s.clearUpload()

	entry, err := s.dict.Lookup(index, sub)
	if err != nil {
		s.sendAbort(index, sub, err.Code)
		return
	}
	if entry.Access == od.AccessRO || entry.Access == od.AccessConst {
		s.sendAbort(index, sub, od.AbortReadOnly)
		return
	}

	expedited := f.Data[0]&bitExpedited != 0
	sizeIndicated := f.Data[0]&bitSizeIndicated != 0

	if expedited {
		n := expeditedDataBytes(f.Data[0])
		payload := make([]byte, n)
		copy(payload, f.Data[4:4+n])
		// Validate and install atomically: SetRaw performs all checks
		// before mutating the dictionary.
		if aerr := s.dict.SetRaw(index, sub, payload); aerr != nil {
			s.sendAbort(index, sub, aerr.Code)
			return
		}
		var d [8]byte
		d[0] = scsInitiateDownload << 5
		d[1] = byte(index)
		d[2] = byte(index >> 8)
		d[3] = sub
		s.logf("SDO server node %d download expedited 0x%04X:%02X len=%d", s.id, index, sub, n)
		s.reply(d)
		s.clearDownload()
		return
	}

	// Segmented download.
	size := 0
	if sizeIndicated {
		size = int(uint32(f.Data[4]) | uint32(f.Data[5])<<8 |
			uint32(f.Data[6])<<16 | uint32(f.Data[7])<<24)
	}
	s.down = &downloadState{index: index, sub: sub, size: size}
	if size > 0 {
		s.down.buf = make([]byte, 0, size)
	}

	var d [8]byte
	d[0] = scsInitiateDownload << 5
	d[1] = byte(index)
	d[2] = byte(index >> 8)
	d[3] = sub
	s.logf("SDO server node %d download segmented begin 0x%04X:%02X size=%d", s.id, index, sub, size)
	s.reply(d)
}

// handleDownloadSegment serves one "client download segment" (CCS=0).
// Segments must carry strictly alternating toggle bits. A mismatched
// toggle aborts the transfer; the client treats a lost segment ACK as a
// local timeout and restarts the whole download from initiate, which
// keeps the retry path deterministic and the buffer intact.
func (s *Server) handleDownloadSegment(f canbus.Frame) {
	if s.down == nil {
		s.sendAbort(0, 0, AbortCmdSpecifier)
		return
	}
	st := s.down
	gotToggle := f.Data[0] & bitToggle
	if gotToggle != st.toggle {
		s.sendAbort(st.index, st.sub, AbortToggleBit)
		s.clearDownload()
		return
	}
	n := segmentDataBytes(f.Data[0])
	payload := append([]byte(nil), f.Data[1:1+n]...)
	last := f.Data[0]&bitNoMoreSegments != 0
	st.buf = append(st.buf, payload...)
	s.replyACKAndFinish(st, last)
}

// replyACKAndFinish validates the final segment and, on success, sends
// the segment handshake and commits the whole transfer atomically.
func (s *Server) replyACKAndFinish(st *downloadState, last bool) {
	if !last {
		var d [8]byte
		d[0] = scsDownloadSegment<<5 | st.toggle
		s.reply(d)
		st.toggle ^= bitToggle
		return
	}
	// Last segment: validate and commit atomically BEFORE acknowledging.
	// On failure we abort and the old value stays in place; the client
	// never sees an ACK for a rejected transfer.
	if st.size > 0 && len(st.buf) != st.size {
		s.sendAbort(st.index, st.sub, od.AbortTypeMismatch)
		s.clearDownload()
		return
	}
	if aerr := s.dict.SetRaw(st.index, st.sub, st.buf); aerr != nil {
		s.sendAbort(st.index, st.sub, aerr.Code)
		s.clearDownload()
		return
	}
	var d [8]byte
	d[0] = scsDownloadSegment<<5 | st.toggle
	s.reply(d)
	s.logf("SDO server node %d download segmented commit 0x%04X:%02X len=%d",
		s.id, st.index, st.sub, len(st.buf))
	s.clearDownload()
}
