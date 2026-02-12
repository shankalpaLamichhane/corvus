package storage

import (
	"os"
	"testing"

	"github.com/shankalpaLamichhane/corvus/pkg/event"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSegmentAppendRead(t *testing.T) {
	// Create a new segment
	segmentPath := "test_segment.log"
	defer os.Remove(segmentPath)

	segment, err := NewSegment(segmentPath, 0, 1024*1024)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}

	// Append an event
	originalEvent := &event.Event{
		Topic:     "test-topic",
		Value:     []byte("test-value"),
		Timestamp: timestamppb.Now(),
		Headers:   map[string]string{"key": "value"},
	}

	pos, err := segment.Append(originalEvent)
	if err != nil {
		t.Fatalf("Failed to append event: %v", err)
	}

	// Read back the event
	readEvent, err := segment.ReadAt(pos)
	if err != nil {
		t.Fatalf("Failed to read event: %v", err)
	}

	// Verify the event
	if readEvent.Topic != originalEvent.Topic {
		t.Errorf("Topic mismatch: got %s, want %s", readEvent.Topic, originalEvent.Topic)
	}
	if string(readEvent.Value) != string(originalEvent.Value) {
		t.Errorf("Value mismatch: got %s, want %s", string(readEvent.Value), string(originalEvent.Value))
	}
	if val, ok := readEvent.Headers["key"]; !ok || val != "value" {
		t.Errorf("Headers mismatch: got %v, want %v", readEvent.Headers, originalEvent.Headers)
	}
}
