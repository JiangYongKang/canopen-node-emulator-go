// Package od implements the object dictionary shared by the SDO and PDO
// services. All accessors are goroutine-safe; SetRaw validates before it
// mutates anything, so a rejected write never leaves a partially updated
// value.
package od

import (
	"fmt"
	"sync"
)

// DataType identifies a CANopen object dictionary data type.
type DataType uint16

// Supported data types (CiA 301 / CiA 302 subset).
const (
	TypeInvalid       DataType = 0x0000
	TypeInt8          DataType = 0x0002
	TypeInt16         DataType = 0x0003
	TypeInt32         DataType = 0x0004
	TypeUInt8         DataType = 0x0005
	TypeUInt16        DataType = 0x0006
	TypeUInt32        DataType = 0x0007
	TypeVisibleString DataType = 0x0009
	TypeDomain        DataType = 0x000F
)

// Access is the access attribute of an object.
type Access uint8

// Access attributes.
const (
	AccessRO    Access = iota // read only
	AccessWO                  // write only (attempt to read aborts 0x06010001)
	AccessRW                  // read/write
	AccessConst               // read only constant (write aborts 0x06010002)
)

// AbortCode is an SDO abort code (CiA 301 §7.2.4.13).
type AbortCode uint32

// Abort codes reused across the dictionary and SDO layers.
const (
	AbortNoObject       AbortCode = 0x06020000
	AbortNoSubindex     AbortCode = 0x06090011
	AbortUnsupportedAcc AbortCode = 0x06010000
	AbortWriteOnly      AbortCode = 0x06010001 // read on write-only
	AbortReadOnly       AbortCode = 0x06010002 // write on read-only
	AbortTypeMismatch   AbortCode = 0x06070010
	AbortTypeTooHigh    AbortCode = 0x06070012
	AbortTypeTooLow     AbortCode = 0x06070013
)

// AbortError carries a protocol-level failure reason.
type AbortError struct {
	Code     AbortCode
	Index    uint16
	SubIndex uint8
}

func (e *AbortError) Error() string {
	return fmt.Sprintf("SDO abort 0x%08X at 0x%04X:%02X (%s)",
		uint32(e.Code), e.Index, e.SubIndex, codeText(e.Code))
}

func codeText(c AbortCode) string {
	switch c {
	case AbortNoObject:
		return "object does not exist"
	case AbortNoSubindex:
		return "sub-index does not exist"
	case AbortUnsupportedAcc:
		return "unsupported access"
	case AbortWriteOnly:
		return "attempt to read a write-only object"
	case AbortReadOnly:
		return "attempt to write a read-only object"
	case AbortTypeMismatch:
		return "data type/length mismatch"
	case AbortTypeTooHigh:
		return "parameter length too high"
	case AbortTypeTooLow:
		return "parameter length too low"
	default:
		return "aborted"
	}
}

// Entry is one object dictionary entry descriptor.
type Entry struct {
	Index  uint16
	Sub    uint8
	Type   DataType
	Access Access
}

// Size returns the fixed size in bytes; variable-size types return 0.
func (e *Entry) Size() int {
	switch e.Type {
	case TypeUInt8, TypeInt8:
		return 1
	case TypeUInt16, TypeInt16:
		return 2
	case TypeUInt32, TypeInt32:
		return 4
	case TypeVisibleString, TypeDomain:
		return 0
	default:
		return -1
	}
}

// storedEntry bundles descriptor and value.
type storedEntry struct {
	entry Entry
	data  []byte
}

func key(index uint16, sub uint8) uint32 {
	return uint32(index)<<8 | uint32(sub)
}

// Dictionary is a thread-safe object dictionary.
type Dictionary struct {
	mu      sync.RWMutex
	entries map[uint32]*storedEntry
	indexes map[uint16]struct{}
}

// New returns an empty dictionary.
func New() *Dictionary {
	return &Dictionary{
		entries: make(map[uint32]*storedEntry),
		indexes: make(map[uint16]struct{}),
	}
}

// AddRaw registers an entry with an initial raw value. A fixed-size type
// whose initial value has the wrong length is a configuration error and
// panics. The dictionary keeps its own copy of the value.
func (d *Dictionary) AddRaw(index uint16, sub uint8, t DataType, a Access, value []byte) {
	e := Entry{Index: index, Sub: sub, Type: t, Access: a}
	size := e.Size()
	if size > 0 && len(value) != size {
		panic(fmt.Sprintf("od: initial value for 0x%04X:%02X has length %d, want %d",
			index, sub, len(value), size))
	}
	v := make([]byte, len(value))
	copy(v, value)

	k := key(index, sub)
	d.mu.Lock()
	d.entries[k] = &storedEntry{entry: e, data: v}
	d.indexes[index] = struct{}{}
	d.mu.Unlock()
}

// abort builds an AbortError.
func abort(code AbortCode, index uint16, sub uint8) *AbortError {
	return &AbortError{Code: code, Index: index, SubIndex: sub}
}

// Lookup returns a copy of an entry descriptor, or a protocol abort.
// A missing sub-index under an existing index returns 0x06090011, which
// is distinguishable from a missing index (0x06020000).
func (d *Dictionary) Lookup(index uint16, sub uint8) (Entry, *AbortError) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	se, ok := d.entries[key(index, sub)]
	if ok {
		return se.entry, nil
	}
	if _, idxOK := d.indexes[index]; idxOK {
		return Entry{}, abort(AbortNoSubindex, index, sub)
	}
	return Entry{}, abort(AbortNoObject, index, sub)
}

// GetRaw returns a copy of the raw value. Reading a write-only object
// aborts with 0x06010001.
func (d *Dictionary) GetRaw(index uint16, sub uint8) ([]byte, *AbortError) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	se, ok := d.entries[key(index, sub)]
	if !ok {
		if _, idxOK := d.indexes[index]; idxOK {
			return nil, abort(AbortNoSubindex, index, sub)
		}
		return nil, abort(AbortNoObject, index, sub)
	}
	if se.entry.Access == AccessWO {
		return nil, abort(AbortWriteOnly, index, sub)
	}
	out := make([]byte, len(se.data))
	copy(out, se.data)
	return out, nil
}

// SetRaw validates access and length first and only then installs a copy
// of the value; a rejected write leaves the previous value untouched.
func (d *Dictionary) SetRaw(index uint16, sub uint8, value []byte) *AbortError {
	d.mu.Lock()
	defer d.mu.Unlock()

	se, ok := d.entries[key(index, sub)]
	if !ok {
		if _, idxOK := d.indexes[index]; idxOK {
			return abort(AbortNoSubindex, index, sub)
		}
		return abort(AbortNoObject, index, sub)
	}
	if se.entry.Access == AccessRO || se.entry.Access == AccessConst {
		return abort(AbortReadOnly, index, sub)
	}
	if size := se.entry.Size(); size > 0 {
		if len(value) > size {
			return abort(AbortTypeTooHigh, index, sub)
		}
		if len(value) < size {
			return abort(AbortTypeTooLow, index, sub)
		}
	}
	v := make([]byte, len(value))
	copy(v, value)
	se.data = v
	return nil
}
