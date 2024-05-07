package bridge

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
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

func Test_Bridge_Run(t *testing.T) {
	tests := []struct {
		name             string
		wsReqs           map[clientReq]gatewayResp
		subscription     map[clientReq]*websockets.Subscription
		unsubscription   map[clientReq]*websockets.SubscriptionID
		expectedSubsByID map[websockets.SubscriptionID]*websockets.Subscription
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
				ethSubNewHeadsBody: &websockets.Subscription{
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

			bridge := newTestBridge(clientConn.Conn, gatewayConn.Conn, gatewayURL, logger.New())

			// Start the bridge
			go bridge.Run()

			clientConn.sendWSRequests(t, test.wsReqs)

			// Wait for a short duration to allow goroutines to run
			<-time.After(500 * time.Millisecond)

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
