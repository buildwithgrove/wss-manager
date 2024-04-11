package router

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	ws "github.com/pokt-foundation/portal-middleware/net/websocket"
	"github.com/pokt-foundation/portal-middleware/relay"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/bridge"
)

const (
	defaultPort = "8080"
	// TODO - move to env
	gatewayURL = "wss://gateway.grove.city/v1/ws"
)

var (
	websocketPathRE = regexp.MustCompile(`^/ws/([0-9a-fA-F]{24}|[0-9a-fA-F]{8})/([0-9a-zA-Z\-]+)`)
)

type (
	apiRouter struct {
		mux      *http.ServeMux
		bridge   bridge.Builder
		logger   *logger.Logger
		imageTag string
	}

	Config struct {
		Logger   *logger.Logger
		ImageTag string
	}
)

// Start starts the API server on the specified port
func Start(ctx context.Context, config Config) error {
	router := newAPIRouter(config)

	server := &http.Server{
		Addr:           fmt.Sprintf(":%s", defaultPort),
		Handler:        router.mux,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   10 * time.Second,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	router.logger.Info(fmt.Sprintf("request reporter API is starting on port %s", defaultPort))
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

// newAPIRouter creates a new APIRouter instance
func newAPIRouter(config Config) *apiRouter {
	router := &apiRouter{
		mux:      http.NewServeMux(),
		logger:   config.Logger,
		imageTag: config.ImageTag,
	}

	router.setupRoutes()

	return router
}

func (ar *apiRouter) setupRoutes() {
	// GET /healthz - handleHealthz returns a simple health check response
	ar.mux.HandleFunc("GET /healthz", ar.handleHealthz)

	// * /
	ar.mux.HandleFunc("* /", ar.websocketHandler)
}

// * /healthz - handleHealthz returns a simple health check response
func (ar *apiRouter) handleHealthz(w http.ResponseWriter, r *http.Request) {
	responseBytes, err := json.Marshal(struct {
		Status   string `json:"status"`
		ImageTag string `json:"imageTag"`
	}{
		Status:   "ok",
		ImageTag: ar.imageTag,
	})
	if err != nil {
		ar.logger.Error("error marshalling health check response", slog.String("error", err.Error()))
		return
	}

	_, err = w.Write(responseBytes)
	if err != nil {
		ar.logger.Error("error writing health check response", slog.String("error", err.Error()))
		return
	}
}

// GET /
func (ar *apiRouter) websocketHandler(w http.ResponseWriter, req *http.Request) {
	matches := websocketPathRE.FindStringSubmatch(req.URL.Path)

	// First match is the enture req.URL.Path
	if len(matches) != 3 {
		return
	}

	app := types.PortalAppID(matches[1])
	chain := types.ChainAlias(matches[2])

	gatewayWS, err := ws.Connect(gatewayURL)
	if err != nil {
		ar.writeRequestProcessingError(w, relay.IDFromString("0"), "Error establishing connection")
		return
	}

	// Allow all origins here. user-security plugin has applied origin whitelisting
	var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

	clientWS, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		ar.writeRequestProcessingError(w, relay.IDFromString("0"), "Error processing request")
		return
	}

	ar.bridge.NewBridge(app, chain, clientWS, gatewayWS).Run()
}

func (ar *apiRouter) writeRequestProcessingError(w http.ResponseWriter, relayID relay.ID, message string) {
	bytes, err := json.Marshal(relay.RelayResponse{
		JSONRPC: "2.0",
		ID:      relayID,
		Error: relay.RelayErrorResponse{
			Code:    -32603,
			Message: fmt.Sprintf("error processing the request: %s", message),
		},
	})
	if err != nil {
		ar.logger.Error("error marshalling request processing error response", slog.String("error", err.Error()))
	}

	w.Header().Set("Content-Type", "application/json")
	// Portal V2 should not return 50x/40x errors: always 200, as per Json-RPC expected behavior
	w.WriteHeader(http.StatusOK)

	_, err = w.Write(bytes)
	if err != nil {
		ar.logger.Error("error writing request processing error response", slog.String("error", err.Error()))
	}
}
