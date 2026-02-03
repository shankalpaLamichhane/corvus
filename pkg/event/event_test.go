package event

import (
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEventEncodeDecode(t *testing.T) {
	originalEvent := &Event{
		Offset:    1337,
		Topic:     "corvus-logs",
		Value:     []byte("log data here"),
		Timestamp: timestamppb.Now(),
		Headers:   map[string]string{"env": "production"},
	}

	// Encode the event
	encodedData, err := originalEvent.Encode()
	if err != nil {
		t.Fatalf("Failed to encode event: %v", err)
	}

	// Decode the event
	decodedEvent, err := Decode(encodedData)
	if err != nil {
		t.Fatalf("Failed to decode event: %v", err)
	}

	if decodedEvent.Offset != originalEvent.Offset {
		t.Errorf("Offset mismatch: got %d, want %d", decodedEvent.Offset, originalEvent.Offset)
	}
}
