package relay

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestID_IDFromString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *ID
	}{
		{
			name:     "should create ID from string",
			input:    "abc123",
			expected: &ID{string: "abc123", isNumber: false},
		},
		{
			name:     "should handle empty string",
			input:    "",
			expected: &ID{string: "", isNumber: false},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := IDFromString(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestID_IDFromInt(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected *ID
	}{
		{
			name:     "should create ID from int",
			input:    123,
			expected: &ID{number: 123, isNumber: true},
		},
		{
			name:     "should handle zero value",
			input:    0,
			expected: &ID{number: 0, isNumber: true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := IDFromInt(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestID_String(t *testing.T) {
	tests := []struct {
		name     string
		id       *ID
		expected string
	}{
		{
			name:     "should return string for string ID",
			id:       &ID{string: "abc123", isNumber: false},
			expected: "abc123",
		},
		{
			name:     "should return empty string for number ID",
			id:       &ID{number: 123, isNumber: true},
			expected: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.id.String()
			assert.Equal(t, test.expected, result)
		})
	}
}

func TestID_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		expected    ID
		expectError bool
	}{
		{
			name:        "should unmarshal string ID",
			input:       []byte(`"abc123"`),
			expected:    ID{string: "abc123", isNumber: false},
			expectError: false,
		},
		{
			name:        "should unmarshal integer ID",
			input:       []byte(`123`),
			expected:    ID{number: 123, isNumber: true},
			expectError: false,
		},
		{
			name:        "should return error for invalid JSON",
			input:       []byte(`:`),
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var id ID
			err := id.UnmarshalJSON(test.input)
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.expected, id)
			}
		})
	}
}

func TestID_MarshalJSON(t *testing.T) {
	tests := []struct {
		name        string
		id          *ID
		expected    []byte
		expectError bool
	}{
		{
			name:     "should marshal string ID",
			id:       &ID{string: "abc123", isNumber: false},
			expected: []byte(`"abc123"`),
		},
		{
			name:     "should marshal integer ID",
			id:       &ID{number: 123, isNumber: true},
			expected: []byte(`123`),
		},
		{
			name:     "should marshal nil for empty ID",
			id:       &ID{},
			expected: []byte(`null`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.id.MarshalJSON()
			if test.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.JSONEq(t, string(test.expected), string(result))
			}
		})
	}
}

func TestRelay_IsError(t *testing.T) {
	tests := []struct {
		name     string
		relay    Relay
		expected bool
	}{
		{
			name:     "should return true when error is not nil",
			relay:    Relay{Error: &RelayError{Code: 1, Message: "error"}},
			expected: true,
		},
		{
			name:     "should return false when error is nil",
			relay:    Relay{Error: nil},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.relay.IsError()
			assert.Equal(t, test.expected, result)
		})
	}
}
