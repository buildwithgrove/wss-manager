package router

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/stretchr/testify/require"
)

var capturedMessages struct {
	sync.Mutex
	clientRequests map[clientReq]struct{}
	// gatewayResponses map[gatewayResp]struct{}
}

type (
	clientReq   string
	gatewayResp string
)

func Test_websocketHandler(t *testing.T) {
	tests := []struct {
		name   string
		app    types.PortalAppID
		wsReqs map[clientReq]gatewayResp
		err    error
	}{
		{
			name: "should connect without error when app ID provided",
			app:  "1a2b3c4d",
			err:  nil,
		},
		{
			name: "should connect and send and receive messages through the bridge to the gateway",
			app:  "1a2b3c4d",
			wsReqs: map[clientReq]gatewayResp{
				`{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice"}`:    `{"jsonrpc":"2.0","id":1,"result":"0x337d04a3b"}`,
				`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`: `{"jsonrpc":"2.0","id":1,"result":"0x12c1b21"}`,
			},
			err: nil,
		},
		{
			name: "should fail to connect when app ID is not provided",
			app:  "",
			err:  errors.New("websocket: bad handshake"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Reset captured messages before each test
			capturedMessages.clientRequests = make(map[clientReq]struct{})

			testGatewayURL := testGatewayWSConn(t, test.wsReqs)
			config := Config{
				Logger: logger.New(),
				GatewayURLFunc: func(chain types.ChainAlias, appID types.PortalAppID) string {
					return testGatewayURL
				},
			}
			router := newAPIRouter(config)

			// Setup mux & handlers
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/{app}", router.websocketHandler)
			ts := httptest.NewServer(mux)

			// Convert http test server URL to ws URL
			wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + fmt.Sprintf("/v1/%s", test.app)

			// Dial WebSocket server
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{})
			c.Equal(test.err, err)

			for clientReq, gatewayResp := range test.wsReqs {
				// Send message through the client
				err := conn.WriteMessage(websocket.TextMessage, []byte(clientReq))
				c.NoError(err)

				// Capture the response from the gateway
				_, message, err := conn.ReadMessage()
				c.NoError(err)
				c.Equal(string(gatewayResp), string(message))

				// Assert that the client sent the expected requests and the gateway received the expected responses
				capturedMessages.Lock()
				_, exists := capturedMessages.clientRequests[clientReq]
				c.True(exists, "Gateway did not receive expected request: %s", clientReq)
				capturedMessages.Unlock()
			}
		})
	}
}

func testGatewayWSConn(t *testing.T, wsReqs map[clientReq]gatewayResp) string {
	gatewaySocketHandler := func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("Failed to upgrade to WebSocket: %v", err)
		}

		go func() {
			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					t.Errorf("Error reading message: %v", err)
					return
				}

				capturedMessages.Lock()
				capturedMessages.clientRequests[clientReq(message)] = struct{}{}
				capturedMessages.Unlock()

				if response, ok := wsReqs[clientReq(message)]; ok {
					if err := conn.WriteMessage(websocket.TextMessage, []byte(response)); err != nil {
						t.Error("Error sending response:", err)
						return
					}
				}
			}
		}()
	}

	s := httptest.NewServer(http.HandlerFunc(gatewaySocketHandler))

	// Parse the original URL
	u, err := url.Parse(s.URL)
	if err != nil {
		t.Fatalf("Failed to parse httptest server URL: %v", err)
	}

	wsURL := strings.Replace(u.String(), "http", "ws", 1)

	return wsURL
}
