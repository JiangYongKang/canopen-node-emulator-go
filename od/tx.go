package od

// Tx is a staged, validated multi-object write. Nothing touches the
// dictionary until Commit succeeds; Commit validates every staged value
// first and installs them only when all checks pass, so a multi-entry
// update (e.g. one PDO frame) is atomic: either all entries are written
// or none are.
type Tx struct {
	d      *Dictionary
	values map[uint32]txValue
}

type txValue struct {
	index uint16
	sub   uint8
	data  []byte
}

// Begin starts a transaction.
func (d *Dictionary) Begin() *Tx {
	return &Tx{d: d, values: make(map[uint32]txValue)}
}

// Set stages a write.
func (t *Tx) Set(index uint16, sub uint8, value []byte) *Tx {
	v := make([]byte, len(value))
	copy(v, value)
	t.values[key(index, sub)] = txValue{index: index, sub: sub, data: v}
	return t
}

// Commit validates all staged values against the live dictionary and
// then installs them. The first error aborts the whole transaction.
func (t *Tx) Commit() *AbortError {
	d := t.d
	d.mu.Lock()
	defer d.mu.Unlock()

	// Validation phase.
	type prepared struct {
		k    uint32
		data []byte
	}
	ready := make([]prepared, 0, len(t.values))
	for _, v := range t.values {
		se, ok := d.entries[key(v.index, v.sub)]
		if !ok {
			if _, idxOK := d.indexes[v.index]; idxOK {
				return abort(AbortNoSubindex, v.index, v.sub)
			}
			return abort(AbortNoObject, v.index, v.sub)
		}
		if se.entry.Access == AccessRO || se.entry.Access == AccessConst {
			return abort(AbortReadOnly, v.index, v.sub)
		}
		if size := se.entry.Size(); size > 0 {
			if len(v.data) > size {
				return abort(AbortTypeTooHigh, v.index, v.sub)
			}
			if len(v.data) < size {
				return abort(AbortTypeTooLow, v.index, v.sub)
			}
		}
		ready = append(ready, prepared{k: key(v.index, v.sub), data: v.data})
	}
	// Install phase: cannot fail after validation.
	for _, p := range ready {
		se := d.entries[p.k]
		se.data = append([]byte(nil), p.data...)
	}
	return nil
}
