package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/shankalpaLamichhane/corvus/pkg/event"
)

// Log manages multiple segments
type Log struct {
	dir           string
	segments      []*Segment
	activeSegment *Segment
	maxSegSize    int64
	mu            sync.RWMutex
}

// Setup initializes the log by loading existing segments
func (l *Log) setup() error {

	if len(l.segments) == 0 {
		// Create first segment directly
		path := filepath.Join(l.dir, "00000000000000000000.log")
		seg, err := NewSegment(path, 0, l.maxSegSize)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
		l.activeSegment = seg
		return nil
	}

	files, err := filepath.Glob(filepath.Join(l.dir, "*.log"))
	if err != nil {
		return err
	}

	sort.Strings(files)

	for _, file := range files {
		var baseOffset int64
		_, err := fmt.Sscanf(filepath.Base(file), "%020d.log", &baseOffset)
		if err != nil {
			return err
		}
		seg, err := NewSegment(file, baseOffset, l.maxSegSize)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
	}

	// If the directory was empty, start the very first segment
	if len(l.segments) == 0 {
		return l.roll()
	}

	// Set the most recent segment as the active segment for writing
	l.activeSegment = l.segments[len(l.segments)-1]
	return nil
}

// NewLog creates a new commit log
func NewLog(dir string, maxSegmentSize int64) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	log := &Log{
		dir:        dir,
		maxSegSize: maxSegmentSize,
	}

	if err := log.setup(); err != nil {
		return nil, err
	}
	return log, nil
}

// IsFull checks if the segment has reached its maximum byte size
func (s *Segment) IsFull() bool {
	info, err := s.file.Stat()
	if err != nil {
		return false
	}
	return info.Size() >= s.maxSize
}

// Append finds the active segment and writes to it
func (l *Log) Append(e *event.Event) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// check for rollover
	if l.activeSegment.IsFull() {
		if err := l.roll(); err != nil {
			return 0, err
		}
	}
	return l.activeSegment.Append(e)
}

// Roll freezes the current segment and starts a new one
func (l *Log) roll() error {
	newBase := l.activeSegment.nextOffset
	path := filepath.Join(l.dir, fmt.Sprintf("%020d.log", newBase))

	newSeg, err := NewSegment(path, newBase, l.maxSegSize)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, newSeg)
	l.activeSegment = newSeg
	return nil
}

// Read finds the correct segment based on offset and reads the event
func (l *Log) Read(offset int64) (*event.Event, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var target *Segment
	// In memory binary search for the segment
	for _, seg := range l.segments {
		if offset >= seg.baseOffset && offset < seg.nextOffset {
			target = seg
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("offset %d not found", offset)
	}
	return target.ReadAt(offset)
}
