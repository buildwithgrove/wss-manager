package router

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/stretchr/testify/require"
)

var capturedMessages struct {
	sync.Mutex
	clientRequests map[clientReq]struct{}
}

type (
	clientReq   string
	gatewayResp string
)

func Test_requestHandler(t *testing.T) {
	tests := []struct {
		name          string
		app           types.PortalAppID
		requests      map[clientReq]gatewayResp
		websocketsReq bool
		err           error
	}{
		{
			name:          "should connect without error when app ID provided",
			app:           "1a2b3c4d",
			websocketsReq: true,
			err:           nil,
		},
		{
			name: "should establish a websocket connection and send and receive messages through the bridge to the gateway",
			app:  "1a2b3c4d",
			requests: map[clientReq]gatewayResp{
				`{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice"}`:    `{"jsonrpc":"2.0","id":1,"result":"0x337d04a3b"}`,
				`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`: `{"jsonrpc":"2.0","id":1,"result":"0x12c1b21"}`,
			},
			websocketsReq: true,
			err:           nil,
		},
		{
			name: "should proxy a regular HTTP request and return the gateway response to the client",
			app:  "1a2b3c4d",
			requests: map[clientReq]gatewayResp{
				`{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice"}`: `{"jsonrpc":"2.0","id":1,"result":"0x337d04a3b"}`,
			},
			websocketsReq: false,
			err:           nil,
		},
		{
			name:          "should fail to connect when app ID is not provided",
			app:           "",
			websocketsReq: true,
			err:           errors.New("websocket: bad handshake"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			// Reset captured messages before each test
			capturedMessages.clientRequests = make(map[clientReq]struct{})

			testHTTPGatewayURL := testGatewayHTTPServer(t, test.requests)
			testWSSGatewayURL := testGatewayWSConn(t, test.requests)
			config := Config{
				Logger: logger.New(),
				GatewayURLFunc: func(scheme string, chain types.ChainAlias, path string) string {
					if strings.Contains(scheme, "http") {
						return testHTTPGatewayURL
					}
					return testWSSGatewayURL
				},
			}
			router := newAPIRouter(config)

			// Setup mux & handlers
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/{app}", router.requestHandler)
			ts := httptest.NewServer(mux)

			if test.websocketsReq {
				// Send a WebSocket connection handshake and multiple messages

				// Convert http test server URL to ws URL
				wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + fmt.Sprintf("/v1/%s", test.app)

				// Dial WebSocket server
				conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{})
				c.Equal(test.err, err)

				for clientReq, gatewayResp := range test.requests {
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
			} else {

				// Send a regular HTTP request
				httpURL := ts.URL + fmt.Sprintf("/v1/%s", test.app)
				for clientReq, expectedResp := range test.requests {
					reqBody := strings.NewReader(string(clientReq))

					req, err := http.NewRequest("POST", httpURL, reqBody)
					c.NoError(err)

					resp, err := http.DefaultClient.Do(req)
					c.NoError(err)
					c.Equal(test.err, err)

					// Read response body
					respBody, err := io.ReadAll(resp.Body)
					c.NoError(err)
					resp.Body.Close()

					// Compare the response body with the expected response
					c.Equal(string(expectedResp), string(respBody))

					// Assert that the client sent the expected requests and the gateway received the expected responses
					capturedMessages.Lock()
					_, exists := capturedMessages.clientRequests[clientReq]
					c.True(exists, "Gateway did not receive expected request: %s", clientReq)
					capturedMessages.Unlock()
				}

			}
		})
	}
}

// testGatewayHTTPServer creates a test HTTP server that mocks the gateway regular relay handling
func testGatewayHTTPServer(t *testing.T, requests map[clientReq]gatewayResp) string {
	httpHandler := func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		err := json.NewDecoder(r.Body).Decode(&raw)
		if err != nil {
			t.Error("Failed to decode request:", err)
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		req := clientReq(raw)

		capturedMessages.Lock()
		capturedMessages.clientRequests[req] = struct{}{}
		capturedMessages.Unlock()

		response, exists := requests[req]
		if !exists {
			http.Error(w, "Not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, err = w.Write([]byte(response))
		if err != nil {
			t.Error("Failed to write response:", err)
		}
	}

	s := httptest.NewServer(http.HandlerFunc(httpHandler))

	return s.URL
}

// testGatewayWSConn creates a test WebSocket server that mocks the gateway websockets handling
func testGatewayWSConn(t *testing.T, requests map[clientReq]gatewayResp) string {
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

				var clientMsg websockets.ClientMessage
				if err := json.Unmarshal(message, &clientMsg); err != nil {
					t.Error("Error unmarshalling message:", err)
					return
				}

				messageReq := clientReq(clientMsg.Message)

				capturedMessages.Lock()
				capturedMessages.clientRequests[messageReq] = struct{}{}
				capturedMessages.Unlock()

				if response, ok := requests[messageReq]; ok {
					gatewayMessage := websockets.GatewayMessage{
						Message: []byte(response),
					}

					gatewayResponse, err := json.Marshal(gatewayMessage)
					if err != nil {
						t.Error("Error marshalling response:", err)
						return
					}

					if err := conn.WriteMessage(websocket.TextMessage, []byte(gatewayResponse)); err != nil {
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
