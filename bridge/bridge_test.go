package bridge

import (
	"testing"
	"time"

	"github.com/pokt-foundation/utils-go/logger"
	mock "github.com/stretchr/testify/mock"
)

func Test_Bridge_Run(t *testing.T) {
	tests := []struct {
		name           string
		clientPayload  []byte
		gatewayPayload []byte
	}{
		{
			name:           "should forward message from client to gateway and receive response",
			clientPayload:  []byte(`{"jsonrpc": "2.0", "id": 0, "method": "eth_gasPrice"}`),
			gatewayPayload: []byte(`{"jsonrpc":"2.0","id":0,"result":"0x337d04a3b"}`),
		},
		// TODO - add test cases for intercepting `eth_subscription` message replies, matching the ID from the send step and adding to the subscriptions map for this bridge
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clientConn := newMockWsConnection(t, test.clientPayload)
			gatewayConn := newMockWsConnection(t, test.gatewayPayload)
			logger := logger.New()

			clientConn.On("ReadMessage").Return(1, test.clientPayload, nil)
			gatewayConn.On("WriteMessage", 1, test.clientPayload).Return(nil)

			gatewayConn.On("ReadMessage").Return(1, test.gatewayPayload, nil)
			clientConn.On("WriteMessage", 1, test.gatewayPayload).Return(nil)

			bridge := NewBuilder(logger).NewBridge("appID", "chainAlias", clientConn, gatewayConn)

			// Start the bridge
			go bridge.Run()

			// Wait for a short duration to allow goroutines to run
			<-time.After(5 * time.Millisecond)

			// Assert that ReadMessage and WriteMessage were called with the correct payloads
			clientConn.AssertCalled(t, "ReadMessage")
			gatewayConn.AssertCalled(t, "WriteMessage", 1, test.clientPayload)

			gatewayConn.AssertCalled(t, "ReadMessage")
			clientConn.AssertCalled(t, "WriteMessage", 1, test.gatewayPayload)

			clientConn.AssertExpectations(t)
			gatewayConn.AssertExpectations(t)
		})
	}
}

// WebSocket connection mock

type mockWsConnection struct {
	mock.Mock
	readPayload []byte
}

func (m *mockWsConnection) ReadMessage() (int, []byte, error) {
	args := m.Called()
	return args.Int(0), m.readPayload, args.Error(2)
}

func (m *mockWsConnection) WriteMessage(messageType int, data []byte) error {
	args := m.Called(messageType, data)
	return args.Error(0)
}

func (m *mockWsConnection) Close() error {
	args := m.Called()
	return args.Error(0)
}

func newMockWsConnection(t interface {
	mock.TestingT
	Cleanup(func())
}, readPayload []byte) *mockWsConnection {
	mock := &mockWsConnection{
		readPayload: readPayload,
	}
	mock.Mock.Test(t)
	t.Cleanup(func() { mock.AssertExpectations(t) })
	return mock
}
