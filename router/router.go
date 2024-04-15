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
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/bridge"
	"github.com/pokt-foundation/wss-manager/relay"
)

type (
	wsRouter struct {
		mux           *http.ServeMux
		bridge        *bridge.Builder
		logger        *logger.Logger
		gatewayDomain string
		imageTag      string
	}

	Config struct {
		GatewayDomain string
		ImageTag      string
		Port          string
		Logger        *logger.Logger
	}
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
		mux:           http.NewServeMux(),
		bridge:        bridge.NewBuilder(config.Logger),
		gatewayDomain: config.GatewayDomain,
		imageTag:      config.ImageTag,
		logger:        config.Logger,
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
	app := types.PortalAppID(req.PathValue("app"))
	if app == "" {
		errString := "app must be present"
		wr.logger.Error(errString)
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), errString)
		return
	}

	// parse the chain from the request host to ensure it is present
	chainDomain := types.ChainDomain(req.Host)
	chain := chainDomain.GetAlias()
	if chain == "" {
		errString := "chain must be present"
		wr.logger.Error(errString)
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), errString)
		return
	}

	// upgrade the client connection to a websocket connection
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true }, // allow all origins
	}
	clientWS, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		errString := fmt.Sprintf("error upgrading connection: %s", err.Error())
		wr.logger.Error(errString)
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), errString)
		return
	}

	// build the gateway URL
	gatewayURL := wr.buildGatewayURL(chain)

	// create a new bridge, which includes creating a new gateway connection
	bridge, err := wr.bridge.NewBridge(app, chain, clientWS, gatewayURL, req)
	if err != nil {
		errString := fmt.Sprintf("error creating bridge: %s", err.Error())
		wr.logger.Error(errString)
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), errString)
		return
	}

	// run the bridge between client websocket and gateway websocket
	go bridge.Run()
}

// writeRequestProcessingError writes a request processing error response to the client in the JSON-RPC expected format
func (wr *wsRouter) writeRequestProcessingError(w http.ResponseWriter, relayID relay.ID, message string) {
	bytes, err := json.Marshal(relay.RelayResponse{
		JSONRPC: "2.0",
		ID:      relayID,
		Error: relay.RelayErrorResponse{
			Code:    -32603,
			Message: fmt.Sprintf("error processing the request: %s", message),
		},
	})
	if err != nil {
		wr.logger.Error("error marshalling request processing error response", slog.String("error", err.Error()))
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK) // gateway should not return 50x/40x errors: always 200 as per JSON-RPC expected behavior

	_, err = w.Write(bytes)
	if err != nil {
		wr.logger.Error("error writing request processing error response", slog.String("error", err.Error()))
	}
}

func (wr *wsRouter) buildGatewayURL(chain types.ChainAlias) string {

	// TEMP - fix
	return "ws://eth-mainnet.localhost:3000/ws/ea7f9165/eth-mainnet"

	// return fmt.Sprintf("wss://%s.%s", chain, wr.gatewayDomain)
}
