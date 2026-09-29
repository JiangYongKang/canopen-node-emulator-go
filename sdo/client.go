package sdo

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/highcumontoa/canopen-node-emulator-go/canbus"
	"github.com/highcumontoa/canopen-node-emulator-go/od"
)

// Client performs SDO transfers against one or more servers. A single
// shared subscription is demultiplexed by response COB-ID; in-flight
// transfers are keyed by node id and serialized through per-call mutexes.
type Client struct {
	bus *canbus.Bus
	cfg Config
	log func(string, ...any)

	subID uint64

	mu      sync.Mutex
	pending map[uint8]chan canbus.Frame
}

// ErrTransfer is returned for local transport problems (timeout/cancel).
var (
	ErrTimeout = errors.New("sdo: response timeout")
	ErrAborted = errors.New("sdo: transfer aborted by server")
)

// NewClient creates an SDO client using the virtual bus.
func NewClient(bus *canbus.Bus, cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg = DefaultConfig()
	}
	c := &Client{
		bus:     bus,
		cfg:     cfg,
		log:     log.Printf,
		pending: make(map[uint8]chan canbus.Frame),
	}
	return c
}

// SetLogger overrides the logger.
func (c *Client) SetLogger(f func(string, ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	c.log = f
}

// Run starts frame demultiplexing. Must be called before transfers.
func (c *Client) Run(ctx context.Context) error {
	id, ch := c.bus.Subscribe(256)
	c.mu.Lock()
	c.subID = id
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		if c.subID == id {
			c.subID = 0
		}
		c.mu.Unlock()
		c.bus.Unsubscribe(id)
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case f, ok := <-ch:
			if !ok {
				return nil
			}
			if f.ID < 0x580 || f.ID > 0x5FF {
				continue
			}
			node := uint8(f.ID - 0x580)
			c.mu.Lock()
			wait, ok := c.pending[node]
			c.mu.Unlock()
			if !ok {
				continue
			}
			select {
			case wait <- f:
			default: // server retry while client isn't waiting: keep first
			}
		}
	}
}

// Stop releases the subscription held by Run.
func (c *Client) Stop() {
	c.mu.Lock()
	id := c.subID
	c.subID = 0
	c.mu.Unlock()
	if id != 0 {
		c.bus.Unsubscribe(id)
	}
}

func (c *Client) register(node uint8) chan canbus.Frame {
	ch := make(chan canbus.Frame, 4)
	c.mu.Lock()
	c.pending[node] = ch
	c.mu.Unlock()
	return ch
}

func (c *Client) unregister(node uint8) {
	c.mu.Lock()
	delete(c.pending, node)
	c.mu.Unlock()
}

func (c *Client) publish(f canbus.Frame) { c.bus.Publish(f) }

// waitFor waits for the next response addressed to node, checking abort.
func (c *Client) waitFor(ctx context.Context, node uint8, ch chan canbus.Frame, index uint16, sub uint8) (canbus.Frame, error) {
	timeout := c.cfg.SegmentTimeout
	if timeout <= 0 {
		timeout = c.cfg.Timeout
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return canbus.Frame{}, ctx.Err()
	case <-t.C:
		return canbus.Frame{}, fmt.Errorf("%w: node %d 0x%04X:%02X", ErrTimeout, node, index, sub)
	case f := <-ch:
		if f.Data[0]>>5 == scsAbort {
			code := od.AbortCode(parseAbort(f.Data))
			return f, &od.AbortError{Code: code, Index: index, SubIndex: sub}
		}
		return f, nil
	}
}
