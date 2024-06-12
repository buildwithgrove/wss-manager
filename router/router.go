package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/bridge"
)

type (
	wsRouter struct {
		mux                     *http.ServeMux
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

	GatewayURLFunc func(chain types.ChainAlias, appID types.PortalAppID) string
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
		gatewayURLFunc:          config.GatewayURLFunc,
		maxReconnectionAttempts: config.MaxReconnectionAttempts,
		wsAuthKey:               config.WSAuthKey,
		imageTag:                config.ImageTag,
		logger:                  config.Logger,
	}

	// GET /healthz - handleHealthz returns a simple health check response
	wr.mux.HandleFunc("GET /healthz", methodCheckMiddleware(wr.handleHealthz))

	// GET /v1/{app} - establishes a websocket connection to the WSS Manager
	wr.mux.HandleFunc("GET /v1/{app}", methodCheckMiddleware(wr.websocketHandler))

	return wr
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

// GET /v1/{app} - handles requests sent to the WSS Manager
func (wr *wsRouter) websocketHandler(w http.ResponseWriter, req *http.Request) {
	// parse the portal app from the request path to ensure it is present
	appID := types.PortalAppID(req.PathValue("app"))
	if appID == "" {
		errString := "app must be present"
		wr.logger.Error(errString)
		wr.writeHandshakeErrorResponse(w, http.StatusBadRequest, errString)
		return
	}

	// parse the chain from the request host to ensure it is present
	chainDomain := types.ChainDomain(req.Host)
	chain := chainDomain.GetAlias()
	if chain == "" {
		errString := "chain must be present"
		wr.logger.Error(errString)
		wr.writeHandshakeErrorResponse(w, http.StatusBadRequest, errString)
		return
	}

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
		GatewayURL:              wr.gatewayURLFunc(chain, appID),
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
