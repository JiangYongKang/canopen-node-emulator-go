package canopen

import (
	"bytes"
	"errors"
	"sync"
	"testing"
)

func TestODBasicAndErrors(t *testing.T) {
	od := NewObjectDict()
	if err := od.Add(Entry{Index: 0x2000, Sub: 0, Type: TypeU16, Access: AccessRW}, nil); err != nil {
		t.Fatal(err)
	}
	if err := od.Add(Entry{Index: 0x2001, Sub: 0, Type: TypeU32, Access: AccessRO}, []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	// bad width on add
	if err := od.Add(Entry{Index: 0x2002, Type: TypeU8}, []byte{1, 2}); !errors.Is(err, ErrLengthMismatch) {
		t.Fatalf("want length mismatch, got %v", err)
	}
	// read back
	v, err := od.ReadRaw(0x2001, 0)
	if err != nil || !bytes.Equal(v, []byte{1, 2, 3, 4}) {
		t.Fatalf("read = %v %v", v, err)
	}
	// read missing
	if _, err := od.ReadRaw(0x9999, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	// write read-only
	if err := od.WriteRaw(0x2001, 0, []byte{9, 9, 9, 9}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("want read-only, got %v", err)
	}
	// value must be untouched
	v, _ = od.ReadRaw(0x2001, 0)
	if !bytes.Equal(v, []byte{1, 2, 3, 4}) {
		t.Fatal("read-only entry changed after rejected write")
	}
	// wrong length write
	if err := od.WriteRaw(0x2000, 0, []byte{1}); !errors.Is(err, ErrLengthMismatch) {
		t.Fatalf("want length mismatch, got %v", err)
	}
	// good write + defensive copy
	if err := od.WriteRaw(0x2000, 0, []byte{0x34, 0x12}); err != nil {
		t.Fatal(err)
	}
	v, _ = od.ReadRaw(0x2000, 0)
	if !bytes.Equal(v, []byte{0x34, 0x12}) {
		t.Fatalf("written value = % x", v)
	}
}

func TestODConcurrent(t *testing.T) {
	od := NewObjectDict()
	_ = od.Add(Entry{Index: 0x2100, Type: TypeU32, Access: AccessRW}, nil)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = od.WriteRaw(0x2100, 0, []byte{byte(i), byte(j), 0, 0})
				v, _ := od.ReadRaw(0x2100, 0)
				if len(v) != 4 {
					t.Errorf("torn read len %d", len(v))
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
