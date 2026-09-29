package canopen

import (
	"sync"
)

// ObjectType identifies the data type stored at an OD entry.
type ObjectType int

const (
	TypeU8 ObjectType = iota
	TypeU16
	TypeU32
	TypeU64
	TypeI8
	TypeI16
	TypeI32
	TypeI64
	TypeDomain // raw byte blob / visible string: variable length
)

// Size returns the fixed byte width of a scalar type; domains return 0.
func (t ObjectType) Size() int {
	switch t {
	case TypeU8, TypeI8:
		return 1
	case TypeU16, TypeI16:
		return 2
	case TypeU32, TypeI32:
		return 4
	case TypeU64, TypeI64:
		return 8
	default:
		return 0
	}
}

func (t ObjectType) String() string {
	switch t {
	case TypeU8:
		return "U8"
	case TypeU16:
		return "U16"
	case TypeU32:
		return "U32"
	case TypeU64:
		return "U64"
	case TypeI8:
		return "I8"
	case TypeI16:
		return "I16"
	case TypeI32:
		return "I32"
	case TypeI64:
		return "I64"
	case TypeDomain:
		return "DOMAIN"
	default:
		return "UNKNOWN"
	}
}

// Access is the access attribute of an entry.
type Access int

const (
	AccessRO Access = iota
	AccessWO
	AccessRW
)

func (a Access) String() string {
	switch a {
	case AccessRO:
		return "RO"
	case AccessWO:
		return "WO"
	case AccessRW:
		return "RW"
	default:
		return "?"
	}
}

func (a Access) readable() bool { return a == AccessRO || a == AccessRW }
func (a Access) writable() bool { return a == AccessWO || a == AccessRW }

// Entry describes one object dictionary entry (index/sub-index).
type Entry struct {
	Index  uint16
	Sub    uint8
	Name   string
	Type   ObjectType
	Access Access
	Size   int // bytes; 0 for scalar derives from Type, domains use initial len
}

// ObjectDict is a concurrency-safe object dictionary. All reads return
// defensive copies and all writes replace the value atomically, so a failed
// or torn transfer can never leave a partially updated entry.
type ObjectDict struct {
	mu      sync.RWMutex
	entries map[[2]uint16]*entryData
}

type entryData struct {
	meta Entry
	data []byte
}

// NewObjectDict returns an empty dictionary.
func NewObjectDict() *ObjectDict {
	return &ObjectDict{entries: make(map[[2]uint16]*entryData)}
}

func key(index uint16, sub uint8) [2]uint16 { return [2]uint16{uint16(index), uint16(sub)} }

// Add registers an entry with its initial raw value. For fixed-size scalars
// the initial value may be nil (zero-filled) and must otherwise match the
// type width.
func (od *ObjectDict) Add(e Entry, initial []byte) error {
	if e.Type != TypeDomain {
		if e.Size == 0 {
			e.Size = e.Type.Size()
		}
		if e.Size != e.Type.Size() {
			return ErrTypeMismatch
		}
	}
	if len(initial) != 0 && e.Type != TypeDomain && len(initial) != e.Size {
		return ErrLengthMismatch
	}
	data := make([]byte, e.Size)
	if e.Type == TypeDomain {
		data = append([]byte(nil), initial...)
		e.Size = len(data)
	} else if len(initial) == e.Size {
		copy(data, initial)
	}

	od.mu.Lock()
	defer od.mu.Unlock()
	k := key(e.Index, e.Sub)
	if _, exists := od.entries[k]; exists {
		return ErrAccessDenied
	}
	od.entries[k] = &entryData{meta: e, data: data}
	return nil
}

// Has reports whether an entry exists.
func (od *ObjectDict) Has(index uint16, sub uint8) bool {
	od.mu.RLock()
	_, ok := od.entries[key(index, sub)]
	od.mu.RUnlock()
	return ok
}

// Meta returns a copy of the entry descriptor.
func (od *ObjectDict) Meta(index uint16, sub uint8) (Entry, bool) {
	od.mu.RLock()
	d, ok := od.entries[key(index, sub)]
	od.mu.RUnlock()
	if !ok {
		return Entry{}, false
	}
	return d.meta, true
}

// ReadRaw returns a copy of the entry value.
func (od *ObjectDict) ReadRaw(index uint16, sub uint8) ([]byte, error) {
	od.mu.RLock()
	defer od.mu.RUnlock()
	d, ok := od.entries[key(index, sub)]
	if !ok {
		return nil, ErrNotFound
	}
	if !d.meta.Access.readable() {
		return nil, ErrWriteOnly
	}
	out := make([]byte, len(d.data))
	copy(out, d.data)
	return out, nil
}

// WriteRaw atomically replaces the entry value after checking existence,
// write access and (for scalars) exact length. The dictionary is only touched
// after every check passes, so a rejected write leaves no partial state.
func (od *ObjectDict) WriteRaw(index uint16, sub uint8, data []byte) error {
	od.mu.Lock()
	defer od.mu.Unlock()
	d, ok := od.entries[key(index, sub)]
	if !ok {
		return ErrNotFound
	}
	if !d.meta.Access.writable() {
		return ErrReadOnly
	}
	if d.meta.Type != TypeDomain && len(data) != d.meta.Size {
		return ErrLengthMismatch
	}
	buf := append([]byte(nil), data...)
	d.data = buf
	if d.meta.Type == TypeDomain {
		d.meta.Size = len(buf)
	}
	return nil
}

// Entries returns descriptors of all entries (unspecified order).
func (od *ObjectDict) Entries() []Entry {
	od.mu.RLock()
	out := make([]Entry, 0, len(od.entries))
	for _, d := range od.entries {
		out = append(out, d.meta)
	}
	od.mu.RUnlock()
	return out
}
