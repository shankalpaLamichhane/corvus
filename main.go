package main

import (
	"fmt"

	"github.com/shankalpaLamichhane/corvus/pkg/event"
	"github.com/shankalpaLamichhane/corvus/pkg/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	log, _ := storage.NewLog("./testlog", 1024*1024)
	e := &event.Event{
		Topic: "test-topic",
		Value: []byte("Hello, Corvus!"),
		Headers: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
		Timestamp: timestamppb.Now(),
	}
	offset, err := log.Append(e)
	fmt.Println("Appended event at offset:", offset, "Error:", err)

	read, err := log.Read(offset)
	if err != nil {
		fmt.Println("Read failed:", err)
		return
	}
	fmt.Printf("Read: topic=%s value=%s\n", read.Topic, string(read.Value))
}
 