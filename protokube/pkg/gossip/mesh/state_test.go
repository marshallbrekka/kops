package mesh

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
)

// Ensure we don't get into any deadlocks, and that there isn't concurrent access to
// the records map
func TestKVStateWrapperLocks(t *testing.T) {
	waitGroup := &sync.WaitGroup{}
	target := newKVStateWrapper(1)

	// Start all the goroutines first and have them block on this channel, then
	// close the channel to have them all start
	start := make(chan interface{})

	// Try and encode and merge against the target.
	tryChanges := func(i uint64) {
		<-start
		state := newKVStateWrapper(i)
		target.Encode()
		target.Merge(state)
		target.Encode()
		waitGroup.Done()
	}

	for i := uint64(0); i < 1000; i++ {
		waitGroup.Add(1)
		go tryChanges(i)
	}

	// Start the goroutines
	close(start)

	done := make(chan struct{}, 1)
	timeout := time.After(time.Second * 10)
	go func() {
		waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
		if target.Records["foo"].Version != 999 {
			t.Errorf("Expected final version for foo to be 999, but got %d", target.Records["foo"].Version)
		}
	case <-timeout:
		t.Error("Failed to complete all merges after 10 seconds")
	}
}

func TestKVStateWrapperEncodeDecode(t *testing.T) {
	state := newKVStateWrapper(2)
	encoded := state.Encode()
	decoded, err := DecodeKVState(encoded[0])
	if err != nil {
		t.Fatalf("Error decoding KVStateWrapper: %s", err)
	}
	if !reflect.DeepEqual(state, decoded) {
		t.Errorf("Failed to decode KVStateWrapper\nExpected: %v\nActual: %v\n", state, decoded)
	}

	// Test that the result of marshaling KVState is the same as marshalling KVStateWrapper
	data, err := proto.Marshal(&state.KVState)
	if err != nil {
		t.Fatalf("Error marshaling KVState: %s", err)
	}

	if !reflect.DeepEqual(encoded[0], data) {
		t.Errorf("Expected %X\nActual %X\n", encoded[0], data)
	}
}

func newKVStateWrapper(version uint64) *KVStateWrapper {
	return &KVStateWrapper{
		KVState: KVState{
			Records: map[string]*KVStateRecord{
				"foo": &KVStateRecord{
					Data:    []byte("data"),
					Version: version,
				},
			},
		},
	}
}
