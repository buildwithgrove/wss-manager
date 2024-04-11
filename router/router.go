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
	ws "github.com/pokt-foundation/portal-middleware/net/websocket"
	"github.com/pokt-foundation/portal-middleware/relay"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/bridge"
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

// newAPIRouter creates a new APIRouter instance
func newAPIRouter(config Config) *wsRouter {
	wr := &wsRouter{
		mux:      http.NewServeMux(),
		logger:   config.Logger,
		imageTag: config.ImageTag,
		bridge:   bridge.NewBuilder(config.Logger),
	}

	// GET /healthz - handleHealthz returns a simple health check response
	wr.mux.HandleFunc("GET /healthz", wr.handleHealthz)

	// GET /v1/{app} - establishes a websocket connection to the WSS Manager
	wr.mux.HandleFunc("GET /v1/{app}", wr.websocketHandler)

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
	app := types.PortalAppID(req.PathValue("app"))
	if app == "" {
		wr.logger.Error("error parsing app", slog.String("error", "Error parsing app"))
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), "Error parsing app")
		return
	}

	chainDomain := types.ChainDomain(req.Host)
	chain := chainDomain.GetAlias()
	if chain == "" {
		wr.logger.Error("error parsing host", slog.String("error", "Error parsing host"))
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), "Error parsing host")
		return
	}

	gatewayURL := wr.buildGatewayURL(chain)
	gatewayWS, err := ws.Connect(gatewayURL)
	if err != nil {
		wr.logger.Error("error establishing connection to gateway", slog.String("error", err.Error()))
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), "Error establishing connection")
		return
	}

	// Allow all origins here. user-security plugin has applied origin whitelisting
	var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	clientWS, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		wr.logger.Error("error upgrading connection", slog.String("error", err.Error()))
		wr.writeRequestProcessingError(w, relay.IDFromString("0"), "Error processing request")
		return
	}

	wr.bridge.NewBridge(app, chain, clientWS, gatewayWS).Run()
}

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
	// Portal V2 should not return 50x/40x errors: always 200, as per Json-RPC expected behavior
	w.WriteHeader(http.StatusOK)

	_, err = w.Write(bytes)
	if err != nil {
		wr.logger.Error("error writing request processing error response", slog.String("error", err.Error()))
	}
}

func (wr *wsRouter) buildGatewayURL(chain types.ChainAlias) string {
	return fmt.Sprintf("wss://%s.%s", chain, wr.gatewayDomain)
}
