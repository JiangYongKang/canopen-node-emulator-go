package canopen

import "errors"

// Sentinel errors used across the emulator.
var (
	ErrNotFound       = errors.New("canopen: object not found")
	ErrNoSubIndex     = errors.New("canopen: sub-index does not exist")
	ErrAccessDenied   = errors.New("canopen: access denied")
	ErrReadOnly       = errors.New("canopen: object is read-only")
	ErrWriteOnly      = errors.New("canopen: object is write-only")
	ErrTypeMismatch   = errors.New("canopen: data type mismatch")
	ErrLengthMismatch = errors.New("canopen: data length mismatch")
	ErrInvalidNodeID  = errors.New("canopen: invalid node id")
	ErrBusOffline     = errors.New("canopen: bus is offline (bus-off)")
	ErrNoPDOMapping   = errors.New("canopen: PDO has no mapping")
	ErrPDOLength      = errors.New("canopen: PDO mapping length exceeds 8 bytes")
	ErrSDOAborted     = errors.New("canopen: SDO transfer aborted")
	ErrSDOTimeout     = errors.New("canopen: SDO timeout")
	ErrStateConflict  = errors.New("canopen: operation not allowed in current NMT state")
)
