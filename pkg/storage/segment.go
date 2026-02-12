package storage

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/shankalpaLamichhane/corvus/pkg/event"

	"google.golang.org/protobuf/proto"
)

// Segment stores events in single file
type Segment struct {
	baseOffset int64    // First offset in this segment
	nextOffset int64    // Next offset to assign
	file       *os.File // the data file
	// writer     *bufio.Writer
	// path       string
	maxSize int64 // Maximum bytes of the segment file before creating new segment
	mu      sync.Mutex
}

// NewSegment creates or opens a segment file
func NewSegment(path string, baseOffset int64, maxSize int64) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	s := &Segment{
		baseOffset: baseOffset,
		nextOffset: baseOffset,
		file:       file,
		maxSize:    maxSize,
	}
	// Recovery: If file has data, we must find where the next offset sarts
	if info, _ := file.Stat(); err == nil && info.Size() > 0 {
		s.recoverNextOffset()
	}
	return s, nil
}

// Append writes the event and returns the physical byte position
func (s *Segment) Append(e *event.Event) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e.Offset = s.nextOffset
	data, err := proto.Marshal(e)
	if err != nil {
		return 0, err
	}

	// Get current position of the index
	pos, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	// Write size (4 bytes) + data
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(data)))

	if _, err := s.file.Write(buf); err != nil {
		return 0, err
	}
	if _, err := s.file.Write(data); err != nil {
		return 0, err
	}

	s.nextOffset++
	return pos, nil
}

// ReadAt jumps directly to a byte position and reads the event
func (s *Segment) ReadAt(pos int64) (*event.Event, error) {
	// Read the 4-byte length
	lenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(lenBuf, pos); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)

	// Read the event data
	dataBuf := make([]byte, length)
	if _, err := s.file.ReadAt(dataBuf, pos+4); err != nil {
		return nil, err
	}

	// Unmarshal the event
	var e event.Event
	if err := proto.Unmarshal(dataBuf, &e); err != nil {
		return nil, err
	}

	return &e, nil
}

// recoverNextOffset scans the segment file to find the next offset
func (s *Segment) recoverNextOffset() {
	s.file.Seek(0, io.SeekStart)
	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(s.file, lenBuf); err != nil {
			break
		}
		length := binary.BigEndian.Uint32(lenBuf)

		// skip the data to find the next length prefix
		s.file.Seek(int64(length), io.SeekCurrent)
		s.nextOffset++
	}
}
