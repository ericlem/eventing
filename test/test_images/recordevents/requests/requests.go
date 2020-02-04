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

package requests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	cloudevents "github.com/cloudevents/sdk-go"
)

type MinMaxResponse struct {
	MinAvail int
	MaxSeen  int
}

type EventInfo struct {
	ValidationError string
	Event           *cloudevents.Event
	HTTPHeaders     map[string][]string
}

const GetMinMaxPath = "/minmax"
const GetEntryPath = "/entry/"

func GetMinMax(host string, port int) (minRet int, maxRet int, errRet error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d%s", host, port, GetMinMaxPath))
	if err != nil {
		return -1, -1, fmt.Errorf("http get error: %s", err)
	}
	defer resp.Body.Close()
	bodyContents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, -1, fmt.Errorf("Error reading response body %w", err)
	}
	if resp.StatusCode != 200 {
		return -1, -1, fmt.Errorf("Error %d reading response", resp.StatusCode)
	}
	minMaxResponse := MinMaxResponse{}
	err = json.Unmarshal(bodyContents, &minMaxResponse)
	if err != nil {
		return -1, -1, fmt.Errorf("Error unmarshalling response %w", err)
	}
	if minMaxResponse.MinAvail == 0 || minMaxResponse.MaxSeen == 0 {
		return -1, -1, fmt.Errorf("Invalid decoded json: %+v", minMaxResponse)
	}

	return minMaxResponse.MinAvail, minMaxResponse.MaxSeen, nil
}

func GetEntry(host string, port int, seqno int) (EventInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d%s/%d", host, port, GetEntryPath, seqno))
	if err != nil {
		return EventInfo{}, fmt.Errorf("http get err %s", err)
	}
	defer resp.Body.Close()
	bodyContents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return EventInfo{}, fmt.Errorf("Error reading response body %w", err)
	}
	if resp.StatusCode != 200 {
		return EventInfo{}, fmt.Errorf("Error %d reading response", resp.StatusCode)
	}
	entryResponse := EventInfo{}
	err = json.Unmarshal(bodyContents, &entryResponse)
	if err != nil {
		return EventInfo{}, fmt.Errorf("Error unmarshalling response %w", err)
	}
	if len(entryResponse.ValidationError) == 0 && entryResponse.Event == nil {
		return EventInfo{}, fmt.Errorf("Invalid decoded json: %+v", entryResponse)
	}

	return entryResponse, nil
}
