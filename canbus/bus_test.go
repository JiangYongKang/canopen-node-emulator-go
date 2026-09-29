package canbus

import (
	"sync"
	"testing"
)

func TestBusDeliversToAllSubscribers(t *testing.T) {
	b := NewBus()
	_, c1 := b.Subscribe(4)
	_, c2 := b.Subscribe(4)

	b.Publish(Frame{ID: 0x123, DLC: 1, Data: [8]byte{0xAB}})

	f1 := <-c1
	f2 := <-c2
	if f1.ID != 0x123 || f1.Data[0] != 0xAB {
		t.Fatalf("sub1 got %+v", f1)
	}
	if f2.ID != 0x123 {
		t.Fatalf("sub2 got %+v", f2)
	}
	if f1.Seq == 0 || f1.Seq != f2.Seq {
		t.Fatalf("seq not assigned consistently: %d %d", f1.Seq, f2.Seq)
	}
	t.Logf("frame delivered ID=0x%X data=% X seq=%d", f1.ID, f1.Data[:1], f1.Seq)
}

func TestBusSequenceMonotonic(t *testing.T) {
	b := NewBus()
	_, ch := b.Subscribe(16)
	for i := uint64(1); i <= 10; i++ {
		b.Publish(Frame{ID: 0x100})
	}
	var last uint64
	for i := 0; i < 10; i++ {
		f := <-ch
		if f.Seq <= last {
			t.Fatalf("seq not strictly increasing: %d after %d", f.Seq, last)
		}
		last = f.Seq
	}
	t.Logf("seqs strictly increasing, last=%d", last)
}

func TestBusHookCanMutateAndDrop(t *testing.T) {
	b := NewBus()
	_, ch := b.Subscribe(4)

	b.AddHook(func(f Frame) Frame {
		f.Data[0]++
		return f
	})
	b.Publish(Frame{ID: 0x100, Data: [8]byte{1}})
	f := <-ch
	if f.Data[0] != 2 {
		t.Fatalf("hook mutation not applied: %d", f.Data[0])
	}

	b.AddHook(func(f Frame) Frame { f.ID = Drop; return f })
	b.Publish(Frame{ID: 0x100})
	select {
	case f := <-ch:
		t.Fatalf("dropped frame was delivered: %+v", f)
	default:
	}
	if b.Dropped() == 0 {
		t.Fatal("dropped counter not incremented")
	}
	t.Logf("hooks: mutated then dropped; dropped=%d", b.Dropped())
}

func TestBusFaultSwallowsFrames(t *testing.T) {
	b := NewBus()
	_, ch := b.Subscribe(2)

	b.SetFaulted(true)
	if !b.Faulted() {
		t.Fatal("faulted flag not set")
	}
	b.Publish(Frame{ID: 0x100})
	select {
	case f := <-ch:
		t.Fatalf("frame delivered while faulted: %+v", f)
	default:
	}
	if b.Frames() != 1 {
		t.Fatalf("faulted frame should still be accepted/counted, got %d", b.Frames())
	}

	b.SetFaulted(false)
	b.Publish(Frame{ID: 0x101})
	f := <-ch
	if f.ID != 0x101 {
		t.Fatalf("delivery did not resume: %+v", f)
	}
	t.Logf("fault swallowed frame (accepted=%d), delivery resumed with ID=0x%X", b.Frames(), f.ID)
}

func TestBusSlowConsumerDoesNotBlock(t *testing.T) {
	b := NewBus()
	_, slow := b.Subscribe(1)
	_, fast := b.Subscribe(8)
	for i := 0; i < 5; i++ {
		b.Publish(Frame{ID: 0x200, Data: [8]byte{byte(i)}})
	}
	gotFast := 0
	for {
		select {
		case <-fast:
			gotFast++
		default:
			goto done
		}
	}
done:
	if gotFast != 5 {
		t.Fatalf("fast consumer lost frames: %d/5 (publish must not block)", gotFast)
	}
	// Drain slow channel; count dropped copies must be > 0.
	drainCount := 0
	for {
		select {
		case <-slow:
			drainCount++
		default:
			goto done2
		}
	}
done2:
	if b.Dropped() == 0 {
		t.Fatalf("slow subscriber backpressure should drop copies, dropped=%d", b.Dropped())
	}
	t.Logf("fast got=%d slow kept=%d dropped copies=%d", gotFast, drainCount, b.Dropped())
}

func TestBusUnsubscribeCloses(t *testing.T) {
	b := NewBus()
	id, ch := b.Subscribe(2)
	b.Unsubscribe(id)
	if _, ok := <-ch; ok {
		t.Fatal("channel not closed after unsubscribe")
	}
	// Publishing after unsubscribe must not panic.
	b.Publish(Frame{ID: 0x1})
	t.Log("unsubscribe closed channel and publish remains safe")
}

func TestBusConcurrentPublishRace(t *testing.T) {
	b := NewBus()
	_, ch := b.Subscribe(1024)
	var wg sync.WaitGroup
	const writers = 16
	const each = 200
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id byte) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				b.Publish(Frame{ID: 0x300 | uint32(id), Data: [8]byte{byte(i)}})
			}
		}(byte(w))
	}
	wg.Wait()
	received := 0
loop:
	for {
		select {
		case <-ch:
			received++
		default:
			break loop
		}
	}
	if want := writers * each; b.Frames() != uint64(want) {
		t.Fatalf("accepted=%d want %d", b.Frames(), want)
	}
	t.Logf("concurrent publishes accepted=%d, subscriber received=%d (rest dropped by backpressure)",
		b.Frames(), received)
}
