package canopen

import (
	"fmt"
	"io"
	"sync"
)

// Logger is a small leveled, goroutine-safe logger used to print key frames,
// state transitions and the reason behind every decision in tests.
// It is safe for concurrent use. A nil *Logger is a no-op.
type Logger struct {
	mu      sync.Mutex
	w       io.Writer
	prefix  string
	enabled bool
	debug   bool
}

// NewLogger creates a logger writing to w. A nil writer yields a disabled
// logger.
func NewLogger(w io.Writer, prefix string) *Logger {
	return &Logger{w: w, prefix: prefix, enabled: w != nil, debug: true}
}

// Enabled reports whether info logging is on.
func (l *Logger) Enabled() bool { return l != nil && l.enabled }

// SetEnabled turns info logging on/off.
func (l *Logger) SetEnabled(b bool) {
	l.mu.Lock()
	l.enabled = b
	l.mu.Unlock()
}

// SetDebug turns debug logging on/off.
func (l *Logger) SetDebug(b bool) {
	l.mu.Lock()
	l.debug = b
	l.mu.Unlock()
}

func (l *Logger) write(level, msg string) {
	if l == nil || l.w == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, "%s [%s] %s\n", l.prefix, level, msg)
}

// Infof logs an informational line.
func (l *Logger) Infof(format string, args ...any) {
	if l != nil && l.enabled {
		l.write("INFO", fmt.Sprintf(format, args...))
	}
}

// Debugf logs a diagnostic line.
func (l *Logger) Debugf(format string, args ...any) {
	if l != nil && l.enabled && l.debug {
		l.write("DBG ", fmt.Sprintf(format, args...))
	}
}

// Framef logs a CAN frame with a direction tag ("TX"/"RX"/"DROP" ...) and an
// explanation of why it was accepted, rejected or altered.
func (l *Logger) Framef(dir string, f Frame, note string) {
	if l == nil || !l.enabled || !l.debug {
		return
	}
	l.write("FRM ", fmt.Sprintf("%s id=%#04x dlc=%d data=%s | %s",
		dir, f.CANID, f.DLC, hexBytes(f.Data[:f.DLC]), note))
}

func hexBytes(b []byte) string {
	if len(b) == 0 {
		return "-"
	}
	s := ""
	for _, x := range b {
		s += fmt.Sprintf("%02x ", x)
	}
	return s[:len(s)-1]
}
