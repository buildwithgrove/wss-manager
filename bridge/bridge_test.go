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

func newTestBridge(clientConn, gatewayConn *websocket.Conn, gatewayURL string, log *logger.Logger) *Bridge {
	return &Bridge{
		clientConn:              clientConn,
		gatewayConn:             gatewayConn,
		gatewayURL:              gatewayURL,
		metrics:                 exporterMocks.Exporter{},
		maxReconnectionAttempts: 10,

		stopChan:       make(chan error),
		pausePingLoop:  make(chan struct{}),
		resumePingLoop: make(chan struct{}),
		wsLock:         sync.Mutex{},

		subscriptions: make(map[websockets.SubscriptionID]*websockets.Subscription),
		subsLock:      sync.RWMutex{},

		log: log,
	}
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
				c.Equal(test.config.Log, bridge.log)
				c.NotNil(bridge.stopChan)
				c.NotNil(bridge.pausePingLoop)
				c.NotNil(bridge.resumePingLoop)
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
		closeBridgeTest  bool
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
		{
			name:            "should call closeBridge and send stop error",
			closeBridgeTest: true,
			expectedStopErr: "test error: closeBridge",
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

			bridge := newTestBridge(clientConn.Conn, gatewayConn.Conn, gatewayURL, logger.New())

			// Start the bridge
			go bridge.Run()

			clientConn.sendWSRequests(t, test.wsReqs)

			// Wait for a short duration to allow goroutines to run
			<-time.After(500 * time.Millisecond)

			if test.closeBridgeTest {
				customStopChan := make(chan error, 1)
				bridge.stopChan = customStopChan
				go bridge.closeBridge("test error", fmt.Errorf("closeBridge"))
				stopErr := <-customStopChan
				c.EqualError(stopErr, test.expectedStopErr)
				return
			}

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

func Test_cleanup(t *testing.T) {
	tests := []struct {
		name          string
		expectedError bool
	}{
		{
			name:          "should cleanup without errors",
			expectedError: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Create a mock server for client connection
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

			// Create a mock server for gateway connection
			gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
				conn.Close()
			}))
			defer gatewayServer.Close()

			gatewayURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http")
			gatewayConn, _, err := websocket.DefaultDialer.Dial(gatewayURL, nil)
			c.NoError(err)

			bridge := &Bridge{
				clientConn:  clientConn,
				gatewayConn: gatewayConn,
				metrics:     exporterMocks.Exporter{},
				log:         logger.New(),
				wsLock:      sync.Mutex{},
			}

			err = bridge.cleanup(fmt.Errorf("test error"))

			if test.expectedError {
				c.Error(err)
			} else {
				c.NoError(err)
			}
		})
	}
}

func Test_handleClientError(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		expectedClose bool
	}{
		{
			name:          "should log info when client websocket closes normally",
			err:           &websocket.CloseError{Code: websocket.CloseGoingAway},
			expectedClose: false,
		},
		{
			name:          "should log info when client websocket closes abnormally",
			err:           &websocket.CloseError{Code: websocket.CloseAbnormalClosure},
			expectedClose: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Create a mock server for client connection
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

			// Create a mock server for gateway connection
			gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
				conn.Close()
			}))
			defer gatewayServer.Close()

			gatewayURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http")
			gatewayConn, _, err := websocket.DefaultDialer.Dial(gatewayURL, nil)
			c.NoError(err)

			bridge := &Bridge{
				clientConn:  clientConn,
				gatewayConn: gatewayConn,
				metrics:     exporterMocks.Exporter{},
				log:         logger.New(),
				wsLock:      sync.Mutex{},
				stopChan:    make(chan error, 1),
			}

			customStopChan := make(chan error, 1)
			bridge.stopChan = customStopChan

			// Call handleClientError
			bridge.handleClientError(test.err)

			stopErr := <-customStopChan
			expectedErrStr := fmt.Sprintf("error reading from client websocket: %s", test.err.Error())
			c.EqualError(stopErr, expectedErrStr)
		})
	}
}

func Test_handleGatewayError(t *testing.T) {
	tests := []struct {
		name                string
		err                 error
		expectedReconnect   bool
		expectedCloseBridge bool
	}{
		{
			name:                "should not attempt to reconnect on unexpected close error",
			err:                 &websocket.CloseError{Code: websocket.CloseGoingAway},
			expectedReconnect:   false,
			expectedCloseBridge: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Create a mock server for client connection
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

			// Create a mock server for gateway connection
			gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
				conn.Close()
			}))
			defer gatewayServer.Close()

			gatewayURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http")
			gatewayConn, _, err := websocket.DefaultDialer.Dial(gatewayURL, nil)
			c.NoError(err)

			bridge := &Bridge{
				clientConn:  clientConn,
				gatewayConn: gatewayConn,
				metrics:     exporterMocks.Exporter{},
				log:         logger.New(),
				wsLock:      sync.Mutex{},
				stopChan:    make(chan error, 1),
			}

			customStopChan := make(chan error, 1)
			bridge.stopChan = customStopChan

			// Call handleGatewayError
			reconnectedToGateway := bridge.handleGatewayError(test.err)

			stopErr := <-customStopChan
			expectedErrStr := fmt.Sprintf("error reading from gateway websocket: %s", test.err.Error())
			c.EqualError(stopErr, expectedErrStr)

			// Check if reconnect was attempted
			c.Equal(test.expectedReconnect, reconnectedToGateway)

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

			// Create a mock server for gateway connection
			gatewayServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("Error during connection upgradation:", err)
					return
				}
				conn.Close()
			}))
			defer gatewayServer.Close()

			gatewayURL := "ws" + strings.TrimPrefix(gatewayServer.URL, "http")

			bridge := &Bridge{
				clientConn:              clientConn,
				gatewayURL:              gatewayURL,
				metrics:                 exporterMocks.Exporter{},
				headers:                 http.Header{},
				maxReconnectionAttempts: test.maxReconnectionAttempts,
				stopChan:                make(chan error),
				pausePingLoop:           make(chan struct{}),
				resumePingLoop:          make(chan struct{}),
				wsLock:                  sync.Mutex{},
				subscriptions:           test.existingSubscriptions,
				subsLock:                sync.RWMutex{},
				log:                     logger.New(),
			}

			// Simulate initial connection
			gatewayConn, err := bridge.connectGateway()
			c.NoError(err)
			bridge.gatewayConn = gatewayConn

			go bridge.clientPingLoop()
			go bridge.gatewayPingLoop()

			// Shut down the gateway server to simulate connection failure
			if test.expectedError != nil {
				gatewayServer.Close()
			}

			err = bridge.reconnectToGateway()
			if test.expectedError != nil {
				c.Error(err)
			} else {
				c.NoError(err)
			}

			if test.existingSubscriptions != nil {
				c.Equal(test.existingSubscriptions, bridge.subscriptions)
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
