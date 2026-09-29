// Package pdo implements PDO mapping and periodic transmission/reception
// over the virtual CAN bus, including fault tolerant handling of
// duplicate, reordered and stale frames.
//
// Idempotency model: each RPDO remembers the bus Seq of the last applied
// frame and applies frames in monotonic Seq order. Duplicate frames
// (same Seq) are acknowledged as seen but never written twice; older
// frames (lower Seq, "reordered" delivery) are discarded; a late frame
// after a timeout simply refreshes data again. Timeout and bus faults
// never poison previously applied data, which stays available and
// interpretable.
package pdo

import "github.com/highcumontoa/canopen-node-emulator-go/od"

// Mapping maps one byte range of a PDO frame to an OD entry.
type Mapping struct {
	Index  uint16
	Sub    uint8
	Type   od.DataType
	Offset int
	Length int // bytes
}

// PDOState is the receiver state of an RPDO.
type PDOState uint8

// Receiver states.
const (
	PDOIdle PDOState = iota
	PDOActive
	PDOStale
	PDOFaulted
)

// String renders a receiver state.
func (s PDOState) String() string {
	switch s {
	case PDOIdle:
		return "IDLE"
	case PDOActive:
		return "ACTIVE"
	case PDOStale:
		return "STALE"
	case PDOFaulted:
		return "FAULTED"
	default:
		return "UNKNOWN"
	}
}

// size returns the mapping's consumed byte count, preferring the declared
// Length and falling back to the type's natural size.
func (m Mapping) size() int {
	if m.Length > 0 {
		return m.Length
	}
	switch m.Type {
	case od.TypeUInt8, od.TypeInt8:
		return 1
	case od.TypeUInt16, od.TypeInt16:
		return 2
	case od.TypeUInt32, od.TypeInt32:
		return 4
	}
	return 0
}

// validateMappings checks that mappings fit inside an 8-byte frame and
// do not overlap.
func validateMappings(ms []Mapping) error {
	used := make([]bool, 8)
	for _, m := range ms {
		n := m.size()
		if n <= 0 {
			return &MappingError{Index: m.Index, Sub: m.Sub, Reason: "unsupported type"}
		}
		if m.Offset < 0 || m.Offset+n > 8 {
			return &MappingError{Index: m.Index, Sub: m.Sub, Reason: "mapping exceeds 8-byte frame"}
		}
		for i := m.Offset; i < m.Offset+n; i++ {
			if used[i] {
				return &MappingError{Index: m.Index, Sub: m.Sub, Reason: "mappings overlap"}
			}
			used[i] = true
		}
	}
	return nil
}

// MappingError describes an invalid PDO mapping configuration.
type MappingError struct {
	Index  uint16
	Sub    uint8
	Reason string
}

func (e *MappingError) Error() string {
	return "pdo: invalid mapping for 0x" +
		hex4(e.Index) + ":" + hex2(e.Sub) + ": " + e.Reason
}

func hex4(v uint16) string {
	const digits = "0123456789ABCDEF"
	return string([]byte{digits[v>>12&0xF], digits[v>>8&0xF], digits[v>>4&0xF], digits[v&0xF]})
}

func hex2(v uint8) string {
	const digits = "0123456789ABCDEF"
	return string([]byte{digits[v>>4&0xF], digits[v&0xF]})
}
