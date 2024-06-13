package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/pokt-foundation/utils-go/client"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/bridge"
)

type (
	wsRouter struct {
		mux                     *http.ServeMux
		http                    *client.Client
		logger                  *logger.Logger
		gatewayURLFunc          GatewayURLFunc
		maxReconnectionAttempts int
		wsAuthKey               string
		imageTag                string
	}

	Config struct {
		GatewayURLFunc          GatewayURLFunc
		MaxReconnectionAttempts int
		WSAuthKey               string
		ImageTag                string
		Port                    string
		Logger                  *logger.Logger
	}

	GatewayURLFunc func(scheme string, chain types.ChainAlias, path string) string
)

// Start starts the API server on the specified port
func Start(ctx context.Context, config Config) error {
	router := newAPIRouter(config)

	server := &http.Server{
		Addr:           fmt.Sprintf(":%s", config.Port),
		Handler:        router.mux,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	router.logger.Info(fmt.Sprintf("WSS Manager is starting on port %s", config.Port))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

// methodCheckMiddleware ensures that only GET requests are allowed for the wrapped handler
func methodCheckMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed: only GET requests are allowed", http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}

// newAPIRouter creates a new APIRouter instance
func newAPIRouter(config Config) *wsRouter {
	wr := &wsRouter{
		mux:                     http.NewServeMux(),
		http:                    newHTTPClient(),
		gatewayURLFunc:          config.GatewayURLFunc,
		maxReconnectionAttempts: config.MaxReconnectionAttempts,
		wsAuthKey:               config.WSAuthKey,
		imageTag:                config.ImageTag,
		logger:                  config.Logger,
	}

	// GET /healthz - handleHealthz returns a simple health check response
	wr.mux.HandleFunc("GET /healthz", methodCheckMiddleware(wr.handleHealthz))

	// GET /v1/{app} - handles requests sent to the WSS Manager
	// `wss` requests are upgraded to a WebSocket connection
	// `https` requests are proxied to the Gateway
	wr.mux.HandleFunc("/v1/{app}", wr.requestHandler)

	return wr
}

// newHTTPClient creates a new HTTP client with the same transport config as Gateway
// This client is used to proxy requests to the Gateway
func newHTTPClient() *client.Client {
	return client.NewCustomClientWithOptions(client.CustomClientOpts{
		Transport: &http.Transport{
			MaxConnsPerHost:     100,
			MaxIdleConnsPerHost: 100,
			MaxIdleConns:        10_000,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
				DualStack: true,
			}).DialContext,
		},
		Timeout: 10 * time.Second,
		Retries: 3,
	})
}

// * /healthz - handleHealthz returns a simple health check response
func (wr *wsRouter) handleHealthz(w http.ResponseWriter, r *http.Request) {
	responseBytes, err := json.Marshal(struct {
		Status   string `json:"status"`
		ImageTag string `json:"imageTag"`
	}{
		Status:   "ok",
		ImageTag: wr.imageTag,
	})
	if err != nil {
		wr.logger.Error("error marshalling health check response", slog.String("error", err.Error()))
		return
	}

	_, err = w.Write(responseBytes)
	if err != nil {
		wr.logger.Error("error writing health check response", slog.String("error", err.Error()))
		return
	}
}

// isWebSocketRequest checks if the request is a WebSocket connection by checking the `Upgrade` and `Connection` headers
func isWebSocketRequest(req *http.Request) bool {
	upgradeHeader := strings.ToLower(req.Header.Get("Upgrade"))
	connectionHeader := strings.ToLower(req.Header.Get("Connection"))

	return upgradeHeader == "websocket" && strings.Contains(connectionHeader, "upgrade")
}

// getScheme returns the scheme of the request based on the presence of a TLS connection
func getScheme(scheme string, req *http.Request) string {
	if req.TLS != nil {
		scheme += "s"
	}
	return scheme
}

// GET /v1/{app} - handles requests sent to the WSS Manager
func (wr *wsRouter) requestHandler(w http.ResponseWriter, req *http.Request) {
	chainDomain := types.ChainDomain(req.Host)
	chain := chainDomain.GetAlias()

	if isWebSocketRequest(req) {
		wr.websocketHandler(w, req, chain)
	} else {
		wr.httpHandler(w, req, chain)
	}
}

// httpHandler handles HTTP requests by proxying them to the Gateway and returning the response to the user
func (wr *wsRouter) httpHandler(w http.ResponseWriter, req *http.Request, chain types.ChainAlias) {
	scheme := getScheme("http", req)
	url := wr.gatewayURLFunc(scheme, chain, req.URL.Path)

	ctx, cancel := context.WithTimeout(req.Context(), 10*time.Second)
	defer cancel()

	proxyReq, err := http.NewRequestWithContext(ctx, req.Method, url, req.Body)
	if err != nil {
		wr.logger.Error("error creating proxy request", slog.String("error", err.Error()))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	proxyReq.Header = req.Header

	resp, err := wr.http.Client.Do(proxyReq)
	if err != nil {
		wr.logger.Error("error making proxy request", slog.String("error", err.Error()))
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		w.Header()[k] = vv
	}

	w.WriteHeader(resp.StatusCode)

	_, err = io.Copy(w, resp.Body)
	if err != nil {
		wr.logger.Error("error writing response to client", slog.String("error", err.Error()))
	}
}

// websocketHandler handles WebSocket connections by upgrading the connection to a WebSocket connection and creating a bridge
func (wr *wsRouter) websocketHandler(w http.ResponseWriter, req *http.Request, chain types.ChainAlias) {
	// add the `-ws` suffix to the chain to get the WebSocket chain alias
	chain += "-ws"

	// upgrade the client connection to a websocket connection
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true }, // allow all origins
	}
	clientConn, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		errString := fmt.Sprintf("error upgrading connection: %s", err.Error())
		wr.logger.Error(errString)
		wr.writeHandshakeErrorResponse(w, http.StatusBadRequest, errString)
		return
	}

	// forward auth header (if set) & WS auth key header
	headers := http.Header{}
	if req.Header.Get("Authorization") != "" {
		headers.Set("Authorization", req.Header.Get("Authorization"))
	}
	headers.Set(websockets.AuthHeader, wr.wsAuthKey)

	// create a new bridge, which includes creating a new gateway connection
	bridge, err := bridge.NewBridge(bridge.Config{
		ClientConn:              clientConn,
		GatewayURL:              wr.gatewayURLFunc("ws", chain, req.URL.Path),
		Headers:                 headers,
		MaxReconnectionAttempts: wr.maxReconnectionAttempts,
		Log:                     wr.logger,
	})
	if err != nil {
		wr.logger.Error(fmt.Sprintf("error creating bridge: %s", err.Error()))

		// if gateway connection fails, close the connection with the client and send the reason for the closure
		closeMsg := websocket.FormatCloseMessage(websocket.CloseInternalServerErr, err.Error())
		if writeErr := clientConn.WriteMessage(websocket.CloseMessage, closeMsg); writeErr != nil {
			wr.logger.Error(fmt.Sprintf("error writing close message to client: %s", writeErr.Error()))
		}
		clientConn.Close()

		return
	}

	// run the bridge between client websocket and gateway websocket
	go bridge.Run()
}

// writeHandshakeErrorResponse writes a standard HTTP error response to the client.
func (wr *wsRouter) writeHandshakeErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	wr.logger.Error("HTTP error response sent to client", slog.String("error", message))

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(statusCode)

	_, err := w.Write([]byte(message))
	if err != nil {
		wr.logger.Error("error writing HTTP error response to client", slog.String("error", err.Error()))
	}
}
