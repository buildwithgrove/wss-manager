package router

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

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

func Test_Start(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "should start server without error",
			config: Config{
				Port:   "8080",
				Logger: logger.New(),
				GatewayURLFunc: func(scheme string, chain types.ChainAlias, path string) string {
					return "http://localhost:8080"
				},
			},
			wantErr: false,
		},
		{
			name: "should return error when server fails to start",
			config: Config{
				Port:   "invalid_port",
				Logger: logger.New(),
				GatewayURLFunc: func(scheme string, chain types.ChainAlias, path string) string {
					return "http://localhost:8080"
				},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				errCh <- Start(ctx, test.config)
			}()

			select {
			case err := <-errCh:
				if test.wantErr {
					c.Error(err)
				} else {
					c.NoError(err)
				}
			case <-time.After(2 * time.Second):
				if test.wantErr {
					c.Fail("expected error but got none")
				} else {
					cancel()
				}
			}
		})
	}
}

func Test_methodCheckMiddleware(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "should allow GET requests",
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantBody:   "ok",
		},
		{
			name:       "should reject non-GET requests",
			method:     http.MethodPost,
			wantStatus: http.StatusMethodNotAllowed,
			wantBody:   "method not allowed: only GET requests are allowed\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			handler := methodCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, err := w.Write([]byte("ok"))
				c.NoError(err)
			})

			req := httptest.NewRequest(test.method, "/healthz", nil)
			w := httptest.NewRecorder()

			handler(w, req)

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			c.NoError(err)
			resp.Body.Close()

			c.Equal(test.wantStatus, resp.StatusCode)
			c.Equal(test.wantBody, string(body))
		})
	}
}

func Test_corsMiddleware(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		origin     string
		wantStatus int
		wantHeader map[string]string
	}{
		{
			name:       "should handle CORS preflight request",
			method:     http.MethodOptions,
			origin:     "http://example.com",
			wantStatus: http.StatusOK,
			wantHeader: map[string]string{
				"Access-Control-Allow-Origin":  "http://example.com",
				"Access-Control-Allow-Methods": "GET, POST, PUT",
				"Access-Control-Allow-Headers": "Content-Type, solana-client",
			},
		},
		{
			name:       "should handle CORS actual request",
			method:     http.MethodGet,
			origin:     "http://example.com",
			wantStatus: http.StatusOK,
			wantHeader: map[string]string{
				"Access-Control-Allow-Origin": "http://example.com",
			},
		},
		{
			name:       "should return error for invalid method",
			method:     "INVALID",
			origin:     "http://example.com",
			wantStatus: http.StatusMethodNotAllowed,
			wantHeader: map[string]string{
				"Access-Control-Allow-Origin": "http://example.com",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			handler := corsMiddleware(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "INVALID" {
					http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
					return
				}
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(test.method, "/test", nil)
			req.Header.Set("Origin", test.origin)
			w := httptest.NewRecorder()

			handler(w, req)

			resp := w.Result()
			c.Equal(test.wantStatus, resp.StatusCode)

			for key, value := range test.wantHeader {
				c.Equal(value, resp.Header.Get(key))
			}
		})
	}
}

func Test_handleHealthz(t *testing.T) {
	tests := []struct {
		name       string
		imageTag   string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "should return health check response",
			imageTag:   "v1.0.0",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok","imageTag":"v1.0.0"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			config := Config{
				Logger: logger.New(),
			}
			router := newAPIRouter(config)
			router.imageTag = test.imageTag

			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			w := httptest.NewRecorder()

			router.handleHealthz(w, req)

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			c.NoError(err)
			resp.Body.Close()

			c.Equal(test.wantStatus, resp.StatusCode)
			c.JSONEq(test.wantBody, string(body))
		})
	}
}

func Test_requestHandler(t *testing.T) {
	tests := []struct {
		name          string
		app           types.PortalAppID
		requests      map[clientReq]gatewayResp
		websocketsReq bool
		err           error
		authHeader    string
		badURL        bool
		origin        string
		wantHeader    map[string]string
		wantStatus    int
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
		{
			name:          "should forward Authorization header if set",
			app:           "1a2b3c4d",
			websocketsReq: true,
			err:           nil,
			authHeader:    "Bearer testtoken",
		},
		{
			name:          "should handle CORS preflight request",
			app:           "1a2b3c4d",
			websocketsReq: false,
			err:           nil,
			origin:        "http://example.com",
			wantHeader: map[string]string{
				"Access-Control-Allow-Origin":  "http://example.com",
				"Access-Control-Allow-Methods": "GET, POST, PUT",
				"Access-Control-Allow-Headers": "Content-Type, solana-client",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:          "should return error for invalid method",
			app:           "1a2b3c4d",
			websocketsReq: false,
			err:           nil,
			origin:        "http://example.com",
			wantHeader: map[string]string{
				"Access-Control-Allow-Origin": "http://example.com",
			},
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:          "should return error for bad URL when making a regular HTTP request",
			app:           "1a2b3c4d",
			websocketsReq: false,
			err:           errors.New("parse \"http://bad url\": invalid character \" \" in host name"),
			badURL:        true,
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
					if test.badURL {
						return "http://bad url"
					}
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
				headers := http.Header{}
				if test.authHeader != "" {
					headers.Set("Authorization", test.authHeader)
				}
				conn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
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
					if test.authHeader != "" {
						req.Header.Set("Authorization", test.authHeader)
					}
					if test.origin != "" {
						req.Header.Set("Origin", test.origin)
					}

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

					// Check CORS headers if applicable
					if test.origin != "" {
						for key, value := range test.wantHeader {
							c.Equal(value, resp.Header.Get(key))
						}
					}

					// Check status code if applicable
					if test.wantStatus != 0 {
						c.Equal(test.wantStatus, resp.StatusCode)
					}
				}
			}
		})
	}
}

func Test_websocketHandler_Errors(t *testing.T) {
	tests := []struct {
		name       string
		req        *http.Request
		setupMocks func(*wsRouter)
		wantStatus int
		wantBody   string
	}{
		{
			name: "should return error when upgrading connection fails",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
				r.Header.Set("Connection", "Upgrade")
				r.Header.Set("Upgrade", "websocket")
				r.Header.Set("Sec-WebSocket-Version", "13")
				r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
				return r
			}(),
			setupMocks: func(wr *wsRouter) {
				wr.gatewayURLFunc = func(scheme string, chain types.ChainAlias, path string) string {
					return "ws://localhost:8080"
				}
			},
			wantStatus: http.StatusInternalServerError,
			wantBody:   "Internal Server Error\nerror upgrading connection: hijack not supported",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			config := Config{
				Logger: logger.New(),
			}
			router := newAPIRouter(config)
			test.setupMocks(router)

			w := &hijackableResponseRecorder{httptest.NewRecorder()}
			router.websocketHandler(w, test.req, "chain")

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			c.NoError(err)
			resp.Body.Close()

			c.Equal(test.wantStatus, resp.StatusCode)
			c.Equal(test.wantBody, string(body))
		})
	}
}

func Test_writeHandshakeErrorResponse(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		message    string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "should write handshake error response",
			statusCode: http.StatusBadRequest,
			message:    "bad request error",
			wantStatus: http.StatusBadRequest,
			wantBody:   "bad request error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			config := Config{
				Logger: logger.New(),
			}
			router := newAPIRouter(config)

			w := httptest.NewRecorder()
			router.writeHandshakeErrorResponse(w, test.statusCode, test.message)

			resp := w.Result()
			body, err := io.ReadAll(resp.Body)
			c.NoError(err)
			resp.Body.Close()

			c.Equal(test.wantStatus, resp.StatusCode)
			c.Equal(test.wantBody, string(body))
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

type hijackableResponseRecorder struct {
	*httptest.ResponseRecorder
}

func (h *hijackableResponseRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("hijack not supported")
}
