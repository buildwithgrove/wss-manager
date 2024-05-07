package bridge

import (
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
		expectedSubsByID map[websockets.SubscriptionID]*websockets.Subscription
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
			defer clientConn.Close()
			gatewayConn, gatewayURL := testGatewayWSConn(t, test.wsReqs)
			defer gatewayConn.Close()

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

				if response, ok := wsReqs[clientReq(message)]; ok {
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
