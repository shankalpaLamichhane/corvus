package event

import(
    "google.golang.org/protobuf/proto"
)

// Encode serializes the event to bytes using Protobuf
func (e *Event) Encode() ([]byte, error){
    return proto.Marshal(e)
}

// Decode deserializes bytes back to an Event
func Decode(data []byte) (*Event, error){
    var e Event
    err := proto.Unmarshal(data, &e)
    return &e, err
}