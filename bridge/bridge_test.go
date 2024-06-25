package bridge

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	exporterMocks "github.com/pokt-foundation/portal-middleware/metrics/exporter/mocks"
	"github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/stretchr/testify/require"
)

const (
	ethGasPriceBody                    = `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice"}`
	ethBlockNumberBody                 = `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`
	ethSubNewHeadsBody                 = `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`
	ethSubNewPendingTransactionsBody   = `{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions"]}`
	ethUnsubNewPendingTransactionsBody = `{"jsonrpc":"2.0","id":1,"method":"eth_unsubscribe","params":["0x7f4e1826ba9d3c5012acef9876543210"]}`
)

var capturedMessages struct {
	sync.Mutex
	clientRequests   map[clientReq]struct{}
	gatewayResponses map[gatewayResp]struct{}
}

type (
	testWSConnection struct {
		*websocket.Conn
	}

	clientReq   string
	gatewayResp string
)

func newTestBridge(clientConn, gatewayConn *websocket.Conn, gatewayURL string, maxReconnectionAttempts int) *Bridge {
	msgChan := make(chan websockets.Message)
	stopChan := make(chan error)

	log := logger.New().With("component", "bridge")

	b := &Bridge{
		gatewayURL:              gatewayURL,
		headers:                 http.Header{},
		maxReconnectionAttempts: maxReconnectionAttempts,

		msgChan:  msgChan,
		stopChan: make(chan error),

		subscriptions: make(map[websockets.SubscriptionID]*websockets.Subscription),
		mu:            sync.RWMutex{},

		metrics: exporterMocks.Exporter{},
		log:     log,
	}

	b.gatewayConn = websockets.NewConnection(websockets.ConnConfig{
		Conn:          gatewayConn,
		Source:        websockets.SourceBackend,
		ReconnectFunc: b.reconnectToGateway,
		MsgChan:       msgChan,
		StopChan:      stopChan,
		Log:           log.With("conn", "gateway"),
	})
	b.clientConn = websockets.NewConnection(websockets.ConnConfig{
		Conn:     clientConn,
		Source:   websockets.SourceClient,
		MsgChan:  msgChan,
		StopChan: stopChan,
		Log:      log.With("conn", "client"),
	})

	return b
}

func Test_NewBridge(t *testing.T) {
	tests := []struct {
		name                   string
		config                 Config
		expectedError          bool
		expectedGatewayConnNil bool
	}{
		{
			name: "should create a new Bridge instance successfully",
			config: Config{
				ClientConn:              &websocket.Conn{},
				GatewayURL:              "ws://localhost:8080",
				Headers:                 http.Header{},
				MaxReconnectionAttempts: 5,
				MetricExporter:          exporterMocks.Exporter{},
				Log:                     logger.New(),
			},
			expectedError:          false,
			expectedGatewayConnNil: false,
		},
		{
			name: "should return error when failing to connect to gateway",
			config: Config{
				ClientConn:              &websocket.Conn{},
				GatewayURL:              "ws://invalid-url",
				Headers:                 http.Header{},
				MaxReconnectionAttempts: 5,
				MetricExporter:          exporterMocks.Exporter{},
				Log:                     logger.New(),
			},
			expectedError:          true,
			expectedGatewayConnNil: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Create a mock server for successful connection test
			if !test.expectedError {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error("Error during connection upgradation:", err)
						return
					}
					conn.Close()
				}))
				defer server.Close()

				test.config.GatewayURL = "ws" + strings.TrimPrefix(server.URL, "http")
			}

			// Create a mock client connection
			clientServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
				conn.Close()
			}))
			defer clientServer.Close()

			clientURL := "ws" + strings.TrimPrefix(clientServer.URL, "http")
			clientConn, _, err := websocket.DefaultDialer.Dial(clientURL, nil)
			c.NoError(err)
			test.config.ClientConn = clientConn

			bridge, err := NewBridge(test.config)

			if test.expectedError {
				c.Error(err)
				c.Nil(bridge)
			} else {
				c.NoError(err)
				c.NotNil(bridge)
				c.NotNil(bridge.clientConn)
				c.Equal(test.config.GatewayURL, bridge.gatewayURL)
				c.Equal(test.config.Headers, bridge.headers)
				c.Equal(test.config.MaxReconnectionAttempts, bridge.maxReconnectionAttempts)
				c.NotNil(bridge.stopChan)
				c.NotNil(bridge.subscriptions)
				if test.expectedGatewayConnNil {
					c.Nil(bridge.gatewayConn)
				} else {
					c.NotNil(bridge.gatewayConn)
				}
			}
		})
	}
}

func Test_Bridge_Run(t *testing.T) {
	tests := []struct {
		name             string
		wsReqs           map[clientReq]gatewayResp
		subscription     map[clientReq]*websockets.Subscription
		unsubscription   map[clientReq]*websockets.SubscriptionID
		expectedSubsByID map[websockets.SubscriptionID]*websockets.Subscription
		expectedStopErr  string
	}{
		{
			name: "should forward message from client to gateway and receive response",
			wsReqs: map[clientReq]gatewayResp{
				ethGasPriceBody:    `{"jsonrpc":"2.0","id":1,"result":"0x337d04a3b"}`,
				ethBlockNumberBody: `{"jsonrpc":"2.0","id":1,"result":"0x12c1b21"}`,
			},
		},
		{
			name: "should add new subscription to bridge map for an eth_subscribe request",
			wsReqs: map[clientReq]gatewayResp{
				ethSubNewHeadsBody: `{"jsonrpc":"2.0","id":1,"result":"0x62013741778a9ba131fec673e84f0916"}`,
			},
			subscription: map[clientReq]*websockets.Subscription{
				ethSubNewHeadsBody: {
					ID:          "0x62013741778a9ba131fec673e84f0916",
					RequestBody: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`),
				},
			},
			expectedSubsByID: map[websockets.SubscriptionID]*websockets.Subscription{
				"0x62013741778a9ba131fec673e84f0916": {
					ID:          "0x62013741778a9ba131fec673e84f0916",
					RequestBody: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`),
				},
			},
		},
		{
			name: "should remove existing subscription from bridge map for an eth_unsubscribe request",
			wsReqs: map[clientReq]gatewayResp{
				ethSubNewHeadsBody:                 `{"jsonrpc":"2.0","id":1,"result":"0x62013741778a9ba131fec673e84f0916"}`,
				ethSubNewPendingTransactionsBody:   `{"jsonrpc":"2.0","id":1,"result":"0x7f4e1826ba9d3c5012acef9876543210"}`,
				ethUnsubNewPendingTransactionsBody: `{"jsonrpc":"2.0","id":1,"result":true}`,
			},
			subscription: map[clientReq]*websockets.Subscription{
				ethSubNewHeadsBody: {
					ID:          "0x62013741778a9ba131fec673e84f0916",
					RequestBody: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`),
				},
				ethSubNewPendingTransactionsBody: {
					ID:          "0x7f4e1826ba9d3c5012acef9876543210",
					RequestBody: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newPendingTransactions"]}`),
				},
			},
			unsubscription: map[clientReq]*websockets.SubscriptionID{
				ethUnsubNewPendingTransactionsBody: subIDPointer("0x7f4e1826ba9d3c5012acef9876543210"),
			},
			expectedSubsByID: map[websockets.SubscriptionID]*websockets.Subscription{
				"0x62013741778a9ba131fec673e84f0916": {
					ID:          "0x62013741778a9ba131fec673e84f0916",
					RequestBody: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`),
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Reset captured messages before each test
			capturedMessages.clientRequests = make(map[clientReq]struct{})
			capturedMessages.gatewayResponses = make(map[gatewayResp]struct{})

			clientConn := testClientWSConn(t, test.wsReqs)
			gatewayConn, gatewayURL := testGatewayWSConn(t, test.wsReqs, test.subscription, test.unsubscription)

			bridge := newTestBridge(clientConn.Conn, gatewayConn.Conn, gatewayURL, 10)

			// Start the bridge
			go bridge.Run()

			clientConn.sendWSRequests(t, test.wsReqs)

			// Wait for a short duration to allow goroutines to run
			<-time.After(1000 * time.Millisecond)

			// Assert that the client sent the expected requests and the gateway received the expected responses
			capturedMessages.Lock()

			for clientReq := range test.wsReqs {
				_, exists := capturedMessages.clientRequests[clientReq]
				c.True(exists, "Gateway did not receive expected request: %s", clientReq)
			}
			for _, gatewayResp := range test.wsReqs {
				_, exists := capturedMessages.gatewayResponses[gatewayResp]
				c.True(exists, "Client did not receive expected response: %s", gatewayResp)
			}

			capturedMessages.Unlock()

			if test.expectedSubsByID != nil {
				c.Equal(test.expectedSubsByID, bridge.subscriptions)
			}
		})
	}
}

func Test_reconnectToGateway(t *testing.T) {
	tests := []struct {
		name                    string
		maxReconnectionAttempts int
		existingSubscriptions   map[websockets.SubscriptionID]*websockets.Subscription
		expectedError           error
	}{
		{
			name:                    "should reconnect to gateway successfully",
			maxReconnectionAttempts: 3,
			expectedError:           nil,
		},
		{
			name:                    "should reconnect and re-establish subscriptions",
			maxReconnectionAttempts: 3,
			existingSubscriptions: map[websockets.SubscriptionID]*websockets.Subscription{
				"0x1": {
					ID:          "0x1",
					RequestBody: []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`),
				},
			},
			expectedError: nil,
		},
		{
			name:                    "should fail to reconnect to gateway after max attempts",
			maxReconnectionAttempts: 3,
			expectedError:           fmt.Errorf("failed to reconnect to gateway after 3 attempts"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Create a mock server for client connection
			clientServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				_, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
			}))
			defer clientServer.Close()

			clientURL := "ws" + strings.TrimPrefix(clientServer.URL, "http")
			clientConn, _, err := websocket.DefaultDialer.Dial(clientURL, nil)
			c.NoError(err)

			// Create a mock server for gateway connection
			gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				_, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
			}))
			defer gatewayServer.Close()

			gatewayURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http")

			gatewayConn, err := connectGateway(gatewayURL, http.Header{})
			c.NoError(err)
			bridge := newTestBridge(clientConn, gatewayConn, gatewayURL, test.maxReconnectionAttempts)

			bridge.mu.Lock()
			bridge.subscriptions = test.existingSubscriptions
			bridge.mu.Unlock()

			// Shut down the gateway server to simulate connection failure
			if test.expectedError != nil {
				gatewayServer.Close()
			}

			newGatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
				conn.Close()
			}))
			defer newGatewayServer.Close()
			newGatewayServer.URL = gatewayURL

			if test.expectedError != nil {
				newGatewayServer.Close()
			}

			err = bridge.reconnectToGateway()
			if test.expectedError != nil {
				c.Error(err)
			} else {
				c.NoError(err)
			}

			<-time.After(100 * time.Millisecond)

			if test.existingSubscriptions != nil {
				bridge.mu.Lock()
				c.Equal(test.existingSubscriptions, bridge.subscriptions)
				bridge.mu.Unlock()
			}
		})
	}
}

func subIDPointer(subID websockets.SubscriptionID) *websockets.SubscriptionID {
	return &subID
}

func testClientWSConn(t *testing.T, wsReqs map[clientReq]gatewayResp) testWSConnection {
	clientSocketHandler := func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error("Error during connection upgradation:", err)
			return
		}

		for req := range wsReqs {
			err := conn.WriteMessage(websocket.TextMessage, []byte(req))
			if err != nil {
				t.Error("Error reading response:", err)
				return
			}
		}

		go func() {
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					t.Error("Error reading response:", err)
					return
				}

				capturedMessages.Lock()
				capturedMessages.gatewayResponses[gatewayResp(message)] = struct{}{}
				capturedMessages.Unlock()
			}
		}()
	}

	s := httptest.NewServer(http.HandlerFunc(clientSocketHandler))

	wsURL := "ws" + strings.TrimPrefix(s.URL, "http")

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Error(err)
	}

	return testWSConnection{conn}
}

func testGatewayWSConn(t *testing.T, wsReqs map[clientReq]gatewayResp, subscriptions map[clientReq]*websockets.Subscription, unsubscriptions map[clientReq]*websockets.SubscriptionID) (testWSConnection, string) {
	gatewaySocketHandler := func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error("Error during connection upgradation:", err)
			return
		}

		go func() {
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					t.Error("Error reading message:", err)
					return
				}

				var clientMsg websockets.ClientMessage
				if err := json.Unmarshal(message, &clientMsg); err != nil {
					t.Error("Error unmarshalling message:", err)
					return
				}

				messageReq := clientReq(clientMsg.Message)

				if response, ok := wsReqs[messageReq]; ok {
					gatewayMessage := websockets.GatewayMessage{
						Message: []byte(response),
					}

					if subscriptions != nil {
						if subscription, ok := subscriptions[messageReq]; ok {
							gatewayMessage.Subscription = subscription
						}
					}
					if unsubscriptions != nil {
						if unsubscription, ok := unsubscriptions[messageReq]; ok {
							gatewayMessage.Unsubscription = unsubscription
						}
					}

					gatewayResponse, err := json.Marshal(gatewayMessage)
					if err != nil {
						t.Error("Error marshalling response:", err)
						return
					}

					if err := conn.WriteMessage(websocket.TextMessage, []byte(gatewayResponse)); err != nil {
						t.Error("Error sending response:", err)
					}
				}

				capturedMessages.Lock()
				capturedMessages.clientRequests[messageReq] = struct{}{}
				capturedMessages.Unlock()
			}
		}()
	}

	s := httptest.NewServer(http.HandlerFunc(gatewaySocketHandler))

	wsURL := "ws" + strings.TrimPrefix(s.URL, "http")

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Error(err)
	}

	return testWSConnection{conn}, wsURL
}

func (tc testWSConnection) sendWSRequests(t *testing.T, wsReqs map[clientReq]gatewayResp) {
	for req := range wsReqs {
		<-time.After(100 * time.Millisecond)
		if err := tc.WriteMessage(websocket.TextMessage, []byte(req)); err != nil {
			t.Fatalf("failed to send message: %v", err)
		} else {
			t.Logf("Message sent: %s", req) // Log each message sent
		}
	}
}
