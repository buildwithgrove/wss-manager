package relay

import (
	"encoding/json"
	"fmt"
)

type (
	Relay struct {
		ID      *ID             `json:"id,omitempty"`
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method,omitempty"`
		Result  json.RawMessage `json:"result,omitempty"`
		Params  json.RawMessage `json:"params,omitempty"`
		Error   *RelayError     `json:"error,omitempty"`
	}

	RelayError struct {
		Code    int    `json:"code,omitempty"`
		Message string `json:"message,omitempty"`
	}

	SubscriptionEventParams struct {
		Result       json.RawMessage `json:"result"`
		Subscription string          `json:"subscription"`
	}

	ID struct {
		string string
		number int

		isNumber bool
	}
)

func (r *Relay) IsError() bool {
	return r.Error != nil
}

func IDFromString(id string) *ID {
	return &ID{string: id, isNumber: false}
}

func IDFromInt(id int) *ID {
	return &ID{number: id, isNumber: true}
}

func (i *ID) String() string {
	if i.isNumber {
		return ""
	}
	return i.string
}

func (i *ID) UnmarshalJSON(data []byte) error {
	var intID int
	if err := json.Unmarshal(data, &intID); err == nil {
		i.number = intID
		i.isNumber = true
		return nil
	}

	var stringID string
	if err := json.Unmarshal(data, &stringID); err == nil {
		i.string = stringID
		return nil
	}

	return fmt.Errorf("error unmarshalling ID: %s", string(data))
}

func (i *ID) MarshalJSON() ([]byte, error) {
	if i.isNumber {
		return json.Marshal(i.number)
	} else if i.string != "" {
		return json.Marshal(i.string)
	}
	return json.Marshal(nil)
}
