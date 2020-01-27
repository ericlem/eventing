/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go"

	"knative.dev/eventing/test/test_images/recordevents/requests"
)

const evBlockSize = 100

type eventBlock struct {
	firstIndex      int
	firstOffsetFree int
	firstValid      int
	evInfo          [evBlockSize]requests.EventInfo
}

type eventStore struct {
	evBlocks     []*eventBlock
	evBlocksLock sync.Mutex
}

func newEventStore() *eventStore {
	es := &eventStore{}
	es.evBlocks = []*eventBlock{&eventBlock{}}

	es.evBlocks[0].firstIndex = 1
	es.evBlocks[0].firstOffsetFree = 0
	es.evBlocks[0].firstValid = 0
	return es
}

func (es *eventStore) checkAppendBlock() {
	if es.evBlocks[len(es.evBlocks)-1].firstOffsetFree == evBlockSize {
		newEVBlock := &eventBlock{
			firstIndex: es.evBlocks[len(es.evBlocks)-1].firstIndex + evBlockSize,
		}
		es.evBlocks = append(es.evBlocks, newEVBlock)
	}
}

func (es *eventStore) StoreEvent(event cloudevents.Event) {
	var evErrorString string
	evBytes, evError := event.MarshalJSON()
	if evError != nil {
		evErrorString = evError.Error()
		if evErrorString == "" {
			evErrorString = "Unknown Error"
		}
	}
	es.evBlocksLock.Lock()

	evBlock := es.evBlocks[len(es.evBlocks)-1]
	if evBlock.firstOffsetFree < evBlockSize {
		evBlock.evInfo[evBlock.firstOffsetFree] = requests.EventInfo{
			ValidationError: evErrorString,
			EventJSON:       evBytes,
		}

		evBlock.firstOffsetFree++
	}

	// We always keep at least one free entry
	es.checkAppendBlock()

	es.evBlocksLock.Unlock()
}
func (es *eventStore) MaxSeen() int {
	es.evBlocksLock.Lock()
	maxBlock := es.evBlocks[len(es.evBlocks)-1]
	maxSeen := maxBlock.firstIndex + (maxBlock.firstOffsetFree - 1)
	es.evBlocksLock.Unlock()
	if maxSeen == 0 {
		maxSeen = -1
	}
	return maxSeen
}
func (es *eventStore) MinAvail() int {
	minAvail := -1
	es.evBlocksLock.Lock()
	minBlock := es.evBlocks[0]
	if minBlock.firstOffsetFree > minBlock.firstValid {
		minAvail = minBlock.firstIndex + (minBlock.firstValid)
	}
	es.evBlocksLock.Unlock()
	return minAvail
}
func (es *eventStore) GetEventInfo(seq int) (requests.EventInfo, error) {
	var evInfo requests.EventInfo
	found := false

	es.evBlocksLock.Lock()
	for _, block := range es.evBlocks {
		if seq < block.firstIndex+block.firstValid {
			break
		}
		if seq < block.firstIndex+block.firstOffsetFree {
			found = true
			evInfo = block.evInfo[seq-block.firstIndex]
			break
		}
	}
	es.evBlocksLock.Unlock()
	if !found {
		return evInfo, fmt.Errorf("Invalid sequence number %d", seq)
	}
	return evInfo, nil
}
