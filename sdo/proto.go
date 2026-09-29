package sdo

// This file contains SDO protocol constants and byte-level helpers for
// expedited and segmented upload/download (CiA 301 §7.2.4.3-6).

// Client command specifiers (CCS).
const (
	ccsDownloadSegment  = 0
	ccsInitiateDownload = 1
	ccsInitiateUpload   = 2
	ccsUploadSegment    = 3
	ccsAbort            = 4
)

// Server command specifiers (SCS).
const (
	scsUploadSegment    = 0
	scsDownloadSegment  = 1
	scsInitiateUpload   = 2
	scsInitiateDownload = 3
	scsAbort            = 4
)

// Byte-0 flag bits (CiA 301 §7.2.4.3-6):
//
//	initiate upload/download: bit1 = transfer type (1 expedited),
//	                           bit0 = size indicated;
//	upload/download segment:  bit0 = last segment (no more segments),
//	                           bit4 = toggle.
const (
	bitSizeIndicated  = 1 << 0
	bitExpedited      = 1 << 1
	bitNoMoreSegments = 1 << 0
	bitToggle         = 1 << 4
)

// RequestCOB returns the SDO request COB-ID for node id (0x600 + id).
func RequestCOB(id uint8) uint32 { return 0x600 + uint32(id) }

// ResponseCOB returns the SDO response COB-ID for node id (0x580 + id).
func ResponseCOB(id uint8) uint32 { return 0x580 + uint32(id) }

// expeditedDataBytes maps number-of-bytes-doesnt-follow n to payload size.
func expeditedDataBytes(b byte) int { return 4 - int((b>>2)&0x03) }

// expeditedN builds n for a given expedited payload length.
func expeditedN(payloadLen int) byte { return byte(4-payloadLen) << 2 }

// segmentDataBytes maps the n field of a segmented transfer to payload size.
func segmentDataBytes(b byte) int { return 7 - int((b>>1)&0x07) }

// segmentN builds n for a given segment payload length.
func segmentN(payloadLen int) byte { return byte(7-payloadLen) << 1 }

// encodeAbort builds an SDO abort frame payload.
func encodeAbort(index uint16, sub uint8, code uint32) [8]byte {
	var d [8]byte
	d[0] = scsAbort << 5
	d[1] = byte(index)
	d[2] = byte(index >> 8)
	d[3] = sub
	d[4] = byte(code)
	d[5] = byte(code >> 8)
	d[6] = byte(code >> 16)
	d[7] = byte(code >> 24)
	return d
}

// parseAbort extracts the abort code from an abort frame payload.
func parseAbort(d [8]byte) uint32 {
	return uint32(d[4]) | uint32(d[5])<<8 | uint32(d[6])<<16 | uint32(d[7])<<24
}
