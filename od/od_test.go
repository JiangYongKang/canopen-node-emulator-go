package od

import (
	"errors"
	"sync"
	"testing"
)

func TestAbortReasonsAreDistinguishable(t *testing.T) {
	d := New()
	d.AddRaw(0x2000, 0, TypeUInt16, AccessRW, []byte{0, 0})
	d.AddRaw(0x2001, 1, TypeUInt8, AccessRO, []byte{7})
	d.AddRaw(0x2002, 0, TypeUInt8, AccessWO, []byte{9})

	cases := []struct {
		name       string
		index      uint16
		sub        uint8
		want       AbortCode
		wantString string
	}{
		{"missing index", 0x6000, 0, AbortNoObject, "0x06020000"},
		{"missing subindex", 0x2001, 7, AbortNoSubindex, "0x06090011"},
		{"write read-only", 0x2001, 1, AbortReadOnly, "0x06010002"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := d.SetRaw(tc.index, tc.sub, []byte{1})
			if err == nil || err.Code != tc.want {
				t.Fatalf("got %v want %s", err, tc.wantString)
			}
			t.Logf("judgement: %s -> %v", tc.name, err)
		})
	}

	// Reading a write-only object gives the write-only code.
	if _, err := d.GetRaw(0x2002, 0); err == nil || err.Code != AbortWriteOnly {
		t.Fatalf("read of WO object: %v", err)
	}
}

func TestLengthMismatchReasons(t *testing.T) {
	d := New()
	d.AddRaw(0x2000, 0, TypeUInt32, AccessRW, []byte{0, 0, 0, 0})

	if err := d.SetRaw(0x2000, 0, []byte{1, 2, 3, 4, 5}); err == nil || err.Code != AbortTypeTooHigh {
		t.Fatalf("too long: %v", err)
	}
	if err := d.SetRaw(0x2000, 0, []byte{1, 2}); err == nil || err.Code != AbortTypeTooLow {
		t.Fatalf("too short: %v", err)
	}
	v, err := d.GetRaw(0x2000, 0)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "\x00\x00\x00\x00" {
		t.Fatalf("rejected writes must leave the old value, got % X", v)
	}
	t.Logf("length failures distinguishable and no partial/old-value corruption: % X", v)
}

func TestSetRawIsAtomicAndIsolatesValue(t *testing.T) {
	d := New()
	d.AddRaw(0x2000, 0, TypeVisibleString, AccessRW, []byte("abc"))

	buf := []byte("xyz")
	if err := d.SetRaw(0x2000, 0, buf); err != nil {
		t.Fatal(err)
	}
	buf[0] = 'Q' // mutating caller memory must not affect the dictionary
	got, _ := d.GetRaw(0x2000, 0)
	if string(got) != "xyz" {
		t.Fatalf("dictionary aliases caller slice: %q", got)
	}
}

func TestTxCommitAtomicity(t *testing.T) {
	d := New()
	d.AddRaw(0x2000, 0, TypeUInt16, AccessRW, []byte{0xAA, 0xBB})
	d.AddRaw(0x2001, 0, TypeUInt8, AccessRO, []byte{1})

	// First object writable, second read-only: whole transaction must abort.
	err := d.Begin().
		Set(0x2000, 0, []byte{0x11, 0x22}).
		Set(0x2001, 0, []byte{3}).
		Commit()
	if err == nil || err.Code != AbortReadOnly {
		t.Fatalf("want read-only abort, got %v", err)
	}
	got, _ := d.GetRaw(0x2000, 0)
	if string(got) != "\xAA\xBB" {
		t.Fatalf("aborted transaction left partial write: % X", got)
	}

	// Successful transaction installs all values.
	err = d.Begin().Set(0x2000, 0, []byte{0x33, 0x44}).Commit()
	if err != nil {
		t.Fatal(err)
	}
	got, _ = d.GetRaw(0x2000, 0)
	if string(got) != "\x33\x44" {
		t.Fatalf("commit did not install: % X", got)
	}
	t.Logf("atomic transaction: rejected frame rolled back, accepted frame installed % X", got)
}

func TestConcurrentDictionaryAccess(t *testing.T) {
	d := New()
	d.AddRaw(0x2000, 0, TypeUInt32, AccessRW, make([]byte, 4))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if err := d.SetRaw(0x2000, 0, []byte{1, 2, 3, 4}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				v, err := d.GetRaw(0x2000, 0)
				if err != nil {
					t.Error(err)
					return
				}
				if len(v) != 4 {
					t.Errorf("torn read: % X", v)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestAbortErrorIsError(t *testing.T) {
	var e error = &AbortError{Code: AbortNoObject, Index: 1, SubIndex: 2}
	if !errors.As(e, new(*AbortError)) {
		t.Fatal("AbortError does not implement error")
	}
}
