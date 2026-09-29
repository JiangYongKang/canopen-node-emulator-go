package sdo

import (
	"context"
	"errors"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
)

func (c *Client) initiateFrame(ccs byte, index uint16, sub uint8) canbus.Frame {
	var f canbus.Frame
	f.ID = 0 // set by callers
	f.DLC = 8
	f.Data[0] = ccs << 5
	f.Data[1] = byte(index)
	f.Data[2] = byte(index >> 8)
	f.Data[3] = sub
	return f
}

// Upload reads an object dictionary entry from node id. Expedited and
// segmented uploads are both supported. Protocol abort codes are returned
// as *od.AbortError so callers can distinguish the reason; local timeouts
// are retried cfg.Retries times.
func (c *Client) Upload(ctx context.Context, id uint8, index uint16, sub uint8) ([]byte, od.DataType, error) {
	ch := c.register(id)
	defer c.unregister(id)

	var lastErr error
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			c.log("SDO client upload retry #%d node %d 0x%04X:%02X (last err: %v)",
				attempt, id, index, sub, lastErr)
		}
		f := c.initiateFrame(ccsInitiateUpload, index, sub)
		f.ID = RequestCOB(id)
		f.Source = 0
		c.publish(f)

		resp, err := c.waitFor(ctx, id, ch, index, sub)
		if err != nil {
			lastErr = err
			if isAbort(err) {
				return nil, 0, err
			}
			continue // timeout: re-issue initiate (server restarts cleanly)
		}

		// In the server->client initiate-upload response bit1 is the
		// expedited flag and bit0 is the size-indicated flag.
		if resp.Data[0]&bitExpedited != 0 {
			n := expeditedDataBytes(resp.Data[0])
			out := make([]byte, n)
			copy(out, resp.Data[4:4+n])
			c.log("SDO client upload OK expedited node %d 0x%04X:%02X len=%d", id, index, sub, n)
			return out, 0, nil
		}

		size := int(uint32(resp.Data[4]) | uint32(resp.Data[5])<<8 |
			uint32(resp.Data[6])<<16 | uint32(resp.Data[7])<<24)
		data, uerr := c.collectUploadSegments(ctx, id, ch, index, sub, size)
		if uerr != nil {
			lastErr = uerr
			if isAbort(uerr) {
				return nil, 0, uerr
			}
			continue // timeout mid-transfer: restart from initiate
		}
		c.log("SDO client upload OK segmented node %d 0x%04X:%02X len=%d", id, index, sub, len(data))
		return data, 0, nil
	}
	return nil, 0, lastErr
}

func isAbort(err error) bool {
	_, ok := err.(*od.AbortError)
	return ok
}

// errRestartTransfer signals that a segmented transfer must be retried
// from "client initiate" (e.g. a segment response timed out or was lost).
var errRestartTransfer = errors.New("sdo: restart transfer from initiate")

// collectUploadSegments performs one segmented upload attempt. A lost
// segment response cannot be recovered by repeating the segment request
// (the server has already advanced the toggle), so any timeout returns
// errRestartTransfer and the caller re-issues "client initiate upload".
func (c *Client) collectUploadSegments(ctx context.Context, id uint8, ch chan canbus.Frame, index uint16, sub uint8, size int) ([]byte, error) {
	var toggle byte
	out := make([]byte, 0, size)
	for {
		req := canbus.Frame{ID: RequestCOB(id), DLC: 8, Source: 0}
		req.Data[0] = ccsUploadSegment<<5 | toggle
		c.publish(req)

		resp, err := c.waitFor(ctx, id, ch, index, sub)
		if err != nil {
			if isAbort(err) {
				return nil, err
			}
			return nil, errRestartTransfer
		}
		if resp.Data[0]&bitToggle != toggle {
			return nil, &od.AbortError{Code: AbortToggleBit, Index: index, SubIndex: sub}
		}
		n := segmentDataBytes(resp.Data[0])
		out = append(out, resp.Data[1:1+n]...)
		last := resp.Data[0]&bitNoMoreSegments != 0
		toggle ^= bitToggle
		if last {
			if size > 0 && len(out) != size {
				return nil, &od.AbortError{Code: od.AbortTypeMismatch, Index: index, SubIndex: sub}
			}
			return out, nil
		}
	}
}

// Download writes an object dictionary entry on node id. Values up to
// four bytes use expedited transfer; longer values use segmented transfer
// and are installed atomically by the server only after the last segment.
func (c *Client) Download(ctx context.Context, id uint8, index uint16, sub uint8, data []byte) error {
	ch := c.register(id)
	defer c.unregister(id)

	var lastErr error
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			c.log("SDO client download retry #%d node %d 0x%04X:%02X (last err: %v)",
				attempt, id, index, sub, lastErr)
		}
		var err error
		if len(data) <= 4 {
			err = c.downloadExpedited(ctx, id, ch, index, sub, data)
		} else {
			err = c.downloadSegmented(ctx, id, ch, index, sub, data)
		}
		if err == nil {
			c.log("SDO client download OK node %d 0x%04X:%02X len=%d", id, index, sub, len(data))
			return nil
		}
		if isAbort(err) {
			return err // protocol reason is deterministic, do not retry
		}
		lastErr = err
	}
	return lastErr
}

func (c *Client) downloadExpedited(ctx context.Context, id uint8, ch chan canbus.Frame, index uint16, sub uint8, data []byte) error {
	f := c.initiateFrame(ccsInitiateDownload, index, sub)
	f.ID = RequestCOB(id)
	f.Data[0] |= bitExpedited | bitSizeIndicated | expeditedN(len(data))
	copy(f.Data[4:], data)
	c.publish(f)
	_, err := c.waitFor(ctx, id, ch, index, sub)
	return err
}

func (c *Client) downloadSegmented(ctx context.Context, id uint8, ch chan canbus.Frame, index uint16, sub uint8, data []byte) error {
	f := c.initiateFrame(ccsInitiateDownload, index, sub)
	f.ID = RequestCOB(id)
	f.Data[0] |= bitSizeIndicated
	n := uint32(len(data))
	f.Data[4] = byte(n)
	f.Data[5] = byte(n >> 8)
	f.Data[6] = byte(n >> 16)
	f.Data[7] = byte(n >> 24)
	c.publish(f)
	if _, err := c.waitFor(ctx, id, ch, index, sub); err != nil {
		return err
	}

	var toggle byte
	for pos := 0; pos < len(data); {
		end := pos + 7
		last := false
		if end >= len(data) {
			end = len(data)
			last = true
		}
		payload := data[pos:end]
		var seg canbus.Frame
		seg.ID = RequestCOB(id)
		seg.DLC = 8
		seg.Data[0] = ccsDownloadSegment<<5 | toggle | segmentN(len(payload))
		if last {
			seg.Data[0] |= bitNoMoreSegments
		}
		copy(seg.Data[1:], payload)

		c.publish(seg)
		resp, err := c.waitFor(ctx, id, ch, index, sub)
		if err != nil {
			if isAbort(err) {
				return err
			}
			// Lost segment ACK: restart the whole segmented download from
			// initiate (server resets its state on the new initiate).
			return errRestartTransfer
		}
		if resp.Data[0]&bitToggle != toggle {
			return &od.AbortError{Code: AbortToggleBit, Index: index, SubIndex: sub}
		}
		toggle ^= bitToggle
		pos = end
	}
	return nil
}
