package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/health"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/bridge"
)

type (
	wsRouter struct {
		mux                     *http.ServeMux
		logger                  *logger.Logger
		gatewayURLFunc          GatewayURLFunc
		maxReconnectionAttempts int
		imageTag                string
		tls                     bool
	}

	Config struct {
		GatewayURLFunc          GatewayURLFunc
		MaxReconnectionAttempts int
		ImageTag                string
		Port                    string
		TLS                     bool
		Logger                  *logger.Logger
	}

	GatewayURLFunc func(scheme string, chain types.ChainAlias, path string) string
)

// Start starts the API server on the specified port
func Start(ctx context.Context, config Config) error {
	defer func() {
		if r := recover(); r != nil {
			config.Logger.Error(fmt.Sprintf("router panicked: %v", r))
		}
	}()

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

// corsMiddleware handles CORS for the wrapped handler
func corsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, solana-client")

		if r.Method == "OPTIONS" {
			// Handle preflight request, which is necessary for CORS to work.
			w.WriteHeader(http.StatusOK)
			return
		}

		// Call the next handler
		next(w, r)
	}
}

// newAPIRouter creates a new APIRouter instance
func newAPIRouter(config Config) *wsRouter {
	wr := &wsRouter{
		mux:                     http.NewServeMux(),
		gatewayURLFunc:          config.GatewayURLFunc,
		maxReconnectionAttempts: config.MaxReconnectionAttempts,
		tls:                     config.TLS,
		imageTag:                config.ImageTag,
		logger:                  config.Logger,
	}

	// GET /healthz - handleHealthz returns a simple health check response
	wr.mux.HandleFunc("GET /healthz", wr.handleHealthz)

	// GET /v1/{app} - handles requests sent to the WSS Manager
	// `wss` requests are upgraded to a WebSocket connection
	// `https` requests are proxied to the Gateway
	wr.mux.HandleFunc("/v1/{app}", corsMiddleware(wr.requestHandler))

	return wr
}

// * /healthz - handleHealthz returns a simple health check response, as well as the health check response from the Gateway.
// The Gateway does not have a public IP so querying the /healthz endpoint from outside must go through WSS Manager.
func (wr *wsRouter) handleHealthz(w http.ResponseWriter, r *http.Request) {
	var gatewayHealthCheckJSON health.HealthCheckJSON

	resp, err := http.Get(wr.gatewayURLFunc("https", "mainnet", "/healthz"))
	if err != nil {
		gatewayHealthCheckJSON = health.HealthCheckJSON{Status: "request error"}
	} else {
		err := json.NewDecoder(resp.Body).Decode(&gatewayHealthCheckJSON)
		if err != nil {
			wr.logger.Error("error unmarshalling gateway health check", slog.String("error", err.Error()))
			gatewayHealthCheckJSON = health.HealthCheckJSON{Status: "unmarshal error"}
		}
		defer resp.Body.Close()
	}

	responseBytes, err := json.Marshal(struct {
		WSSManagerHealth health.HealthCheckJSON `json:"wssManagerHealth"`
		GatewayHealth    health.HealthCheckJSON `json:"gatewayHealth"`
	}{
		WSSManagerHealth: health.HealthCheckJSON{
			Status:   "ok",
			ImageTag: wr.imageTag,
		},
		GatewayHealth: gatewayHealthCheckJSON,
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
	scheme := "http"
	if wr.tls {
		scheme += "s"
	}

	gatewayURL, err := url.Parse(wr.gatewayURLFunc(scheme, chain, req.URL.Path))
	if err != nil {
		wr.logger.Error("error parsing gateway URL", slog.String("error", err.Error()))
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	httputil.NewSingleHostReverseProxy(gatewayURL).ServeHTTP(w, req)
}

// websocketHandler handles WebSocket connections by upgrading the connection to a WebSocket connection and creating a bridge
func (wr *wsRouter) websocketHandler(w http.ResponseWriter, req *http.Request, chain types.ChainAlias) {
	scheme := "ws"
	if wr.tls {
		scheme += "s"
	}

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

	// forward auth header if set
	headers := http.Header{}
	if req.Header.Get("Authorization") != "" {
		headers.Set("Authorization", req.Header.Get("Authorization"))
	}

	// create a new bridge, which includes creating a new gateway connection
	bridge, err := bridge.NewBridge(bridge.Config{
		ClientConn:              clientConn,
		GatewayURL:              wr.gatewayURLFunc(scheme, chain, req.URL.Path),
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
