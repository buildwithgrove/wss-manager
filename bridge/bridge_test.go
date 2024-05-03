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

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/utils-go/logger"
	relayPkg "github.com/pokt-foundation/wss-manager/relay"
	subPkg "github.com/pokt-foundation/wss-manager/subscription"
	"github.com/stretchr/testify/require"
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

func newTestBridge(clientConn, gatewayConn wsConnection, gatewayURL string, log *logger.Logger) *Bridge {
	return &Bridge{
		id:               uuid.New().String(),
		clientConn:       clientConn,
		gatewayURL:       gatewayURL,
		gatewayConn:      gatewayConn,
		gatewayMsgChan:   make(chan wsMessage, 100_000),
		doneChan:         make(chan struct{}),
		subsByCurrentID:  make(map[subPkg.SubscriptionID]*subPkg.Subscription),
		subsByOriginalID: make(map[subPkg.SubscriptionID]*subPkg.Subscription),
		pendingSubs:      make(map[string]subPkg.PendingSubscribe),
		pendingUnsubs:    make(map[string]subPkg.PendingUnsubscribe),
		pendingResubs:    make(map[string]subPkg.SubscriptionID),
		mu:               sync.RWMutex{},
		log:              log,
	}
}

func Test_Bridge_Run(t *testing.T) {
	tests := []struct {
		name                     string
		wsReqs                   map[clientReq]gatewayResp
		expectedSubsByCurrentID  map[subPkg.SubscriptionID]*subPkg.Subscription
		expectedSubsByOriginalID map[subPkg.SubscriptionID]*subPkg.Subscription
	}{
		{
			name: "should forward message from client to gateway and receive response",
			wsReqs: map[clientReq]gatewayResp{
				`{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice"}`:    `{"jsonrpc":"2.0","id":1,"result":"0x337d04a3b"}`,
				`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`: `{"jsonrpc":"2.0","id":1,"result":"0x12c1b21"}`,
			},
		},
		{
			name: "should add new subscription to bridge maps for an eth_subscribe request",
			wsReqs: map[clientReq]gatewayResp{
				`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`: `{"jsonrpc":"2.0","id":1,"result":"0x62013741778a9ba131fec673e84f0916"}`,
			},
			expectedSubsByCurrentID: map[subPkg.SubscriptionID]*subPkg.Subscription{
				"0x62013741778a9ba131fec673e84f0916": subPkg.NewSubscription(
					"0x62013741778a9ba131fec673e84f0916",
					[]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`),
				),
			},
			expectedSubsByOriginalID: map[subPkg.SubscriptionID]*subPkg.Subscription{
				"0x62013741778a9ba131fec673e84f0916": subPkg.NewSubscription(
					"0x62013741778a9ba131fec673e84f0916",
					[]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`),
				),
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
			gatewayConn, gatewayURL := testGatewayWSConn(t, test.wsReqs)

			bridge := newTestBridge(clientConn, gatewayConn, gatewayURL, logger.New())

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

			if test.expectedSubsByCurrentID != nil {
				c.Equal(test.expectedSubsByCurrentID, bridge.subsByCurrentID)
			}
			if test.expectedSubsByOriginalID != nil {
				c.Equal(test.expectedSubsByOriginalID, bridge.subsByOriginalID)
			}
		})
	}
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

				// Match the behaviour of temp Relay ID when handling subscription request
				var relay relayPkg.Relay
				err = json.Unmarshal(message, &relay)
				if err != nil {
					t.Error("Error unmarshalling message:", err)
					return
				}
				message = []byte(fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"result":%s}`, relay.Result))

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

func testGatewayWSConn(t *testing.T, wsReqs map[clientReq]gatewayResp) (testWSConnection, string) {
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

				// Match the behaviour of temp Relay ID when handling subscription request
				var relay relayPkg.Relay
				err = json.Unmarshal(message, &relay)
				if err != nil {
					t.Error("Error unmarshalling message:", err)
					return
				}
				if relay.Method == "eth_subscribe" {
					message = []byte(
						fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"%s","params":%s}`, relay.Method, relay.Params),
					)
				}

				if response, ok := wsReqs[clientReq(message)]; ok {
					// Match the behaviour of temp Relay ID when handling subscription request
					var responseRelay relayPkg.Relay
					err = json.Unmarshal([]byte(response), &responseRelay)
					if err != nil {
						t.Error("Error unmarshalling message:", err)
						return
					}
					if relay.Method == "eth_subscribe" {
						str := `{"jsonrpc":"2.0","id":"%s","result":%s}`
						response = gatewayResp(fmt.Sprintf(str, relay.ID, responseRelay.Result))
					}

					if err := conn.WriteMessage(websocket.TextMessage, []byte(response)); err != nil {
						t.Error("Error sending response:", err)
					}
				}

				capturedMessages.Lock()
				capturedMessages.clientRequests[clientReq(message)] = struct{}{}
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
		if err := tc.WriteMessage(websocket.TextMessage, []byte(req)); err != nil {
			t.Fatalf("failed to send message: %v", err)
		}
	}
}
