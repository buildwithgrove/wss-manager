package bridge

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/pokt-foundation/portal-middleware/relay"
	"github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/pokt-foundation/utils-go/logger"
)

const (
	writeWait     = 10 * time.Second    // Time allowed to write a message to the peer.
	pongWait      = 30 * time.Second    // Time allowed to read the next pong message from the peer.
	pingPeriod    = (pongWait * 9) / 10 // Send pings to peer with this period. Must be less than pongWait.
	backoffFactor = 2                   // Factor by which the gateway reconnection interval increases
	maxBackoff    = 10 * time.Second    // Maximum backoff interval for gateway reconnection
)

type (
	// Bridge routes data between clients and the Gateway.
	// One bridge represents exactly one WebSocket connection between a Client and the Gateway.
	// It will be connected to a Bridge in the Gateway between the Gateway and a WebSocket node.
	//
	// eg. full data flow: Client <-> WSS Manager <-> Gateway <-> WebSocket Node
	//
	// In the case of a Gateway disconnection, the Bridge will attempt a reconnection to the
	// Gateway, including re-establishing any subscriptions. The Client will not be aware of this
	// reconnection logic as their side of the bridge will remain connected at all times.
	Bridge struct {
		gatewayURL              string
		headers                 http.Header
		maxReconnectionAttempts int

		clientConn  *websockets.Connection
		gatewayConn *websockets.Connection
		msgChan     <-chan websockets.Message
		stopChan    chan error

		subscriptions map[websockets.SubscriptionID]*websockets.Subscription

		mu sync.RWMutex

		log *slog.Logger
	}

	Config struct {
		ClientConn              *websocket.Conn
		GatewayURL              string
		Headers                 http.Header
		MaxReconnectionAttempts int
		Log                     *logger.Logger
	}
)

// NewBridge creates a new Bridge instance and a new connection to the Gateway from the Gateway URL
func NewBridge(config Config) (*Bridge, error) {
	gatewayConn, err := connectGateway(config.GatewayURL, config.Headers)
	if err != nil {
		return nil, fmt.Errorf("error establishing connection to node: %s", err.Error())
	}

	msgChan := make(chan websockets.Message)
	stopChan := make(chan error)

	log := config.Log.With("component", "bridge")

	b := &Bridge{
		gatewayURL:              config.GatewayURL,
		headers:                 config.Headers,
		maxReconnectionAttempts: config.MaxReconnectionAttempts,

		msgChan:  msgChan,
		stopChan: stopChan,

		subscriptions: make(map[websockets.SubscriptionID]*websockets.Subscription),
		mu:            sync.RWMutex{},

		log: log,
	}

	b.gatewayConn = websockets.NewConnection(websockets.ConnConfig{
		Conn:          gatewayConn,
		Source:        websockets.SourceBackend,
		ReconnectFunc: b.reconnectToGateway,
		MsgChan:       msgChan,
		StopChan:      stopChan,
		Log:           log.With("conn", "gateway"),
	})
	b.clientConn = websockets.NewConnection(websockets.ConnConfig{
		Conn:     config.ClientConn,
		Source:   websockets.SourceClient,
		MsgChan:  msgChan,
		StopChan: stopChan,
		Log:      log.With("conn", "client"),
	})

	return b, nil
}

// connectGateway connects to the gateway and returns the websocket connection.
func connectGateway(gatewayURL string, headers http.Header) (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(gatewayURL, headers)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

/* ---------- Public method - Run Bridge ---------- */

// Run starts the bridge and establishes a bidirectional communication between the client and server
func (b *Bridge) Run() {
	defer func() {
		if r := recover(); r != nil {
			b.log.Error(fmt.Sprintf("bridge panicked: %v", r))
		}
	}()

	// Start goroutine to read messages from message channel
	go b.messageLoop()

	b.log.Info("bridge operation started successfully")

	// If close signal is received, stop the bridge and close both connections
	<-b.stopChan
}

/* ---------- Private methods - Message loop ---------- */

// messageLoop reads from the message channel and handles messages from the node and WSS Manager
func (b *Bridge) messageLoop() {
	for {
		select {
		case <-b.stopChan:
			return

		case msg := <-b.msgChan:
			switch msg.Source {
			// If the message is from the Client connection, send it to the Gateway
			case websockets.SourceClient:
				b.handleClientMessage(msg)
			// If the message is from the Gateway, send it to the Client
			case websockets.SourceBackend:
				b.handleGatewayMessage(msg)
			}
		}
	}
}

// handleClientMessage processes a message from the Client and sends it to the Gateway
func (b *Bridge) handleClientMessage(msg websockets.Message) {
	clientMsg := websockets.ClientMessage{Message: msg.Data}
	clientMsgBytes, err := json.Marshal(clientMsg)
	if err != nil {
		b.log.Error("error marshalling client message:", slog.String("error", err.Error()))
		return
	}

	err = b.gatewayConn.WriteMessage(msg.MessageType, clientMsgBytes)
	if err != nil {
		// An error writing means the connection is broken and the bridge should be stopped
		b.log.Error("error writing to gateway websocket", slog.String("error", err.Error()))
		b.stopChan <- fmt.Errorf("error writing to gateway websocket: %w", err)
		return
	}
}

// handleGatewayMessage processes a message from the Gateway and sends it to the Client
func (b *Bridge) handleGatewayMessage(msg websockets.Message) {
	// Check if the message is a subscription event or a response to a pending subscribe or unsubscribe request
	processedMsg, err := b.processGatewayResponse(msg.Data)
	if err != nil {
		b.log.Error("error processing gateway response:", slog.String("error", err.Error()))
		return
	}

	// If the message is a resubscribe confirmation from the gateway do not forward it to the client
	if processedMsg == nil {
		return
	}

	err = b.clientConn.WriteMessage(msg.MessageType, processedMsg)
	if err != nil {
		// An error writing means the connection is broken and the bridge should be stopped
		b.log.Error("error writing to client websocket", slog.String("error", err.Error()))
		b.stopChan <- fmt.Errorf("error writing to client websocket: %w", err)
		return
	}
}

// processGatewayResponse checks if a gateway response is an active subscription event,
// or a response to an 'eth_subscribe', 'eth_unsubscribe' or resubscription request.
func (b *Bridge) processGatewayResponse(message []byte) ([]byte, error) {
	var gatewayMsg websockets.GatewayMessage
	if err := json.Unmarshal(message, &gatewayMsg); err != nil {
		return nil, fmt.Errorf("error unmarshalling gateway response: %w", err)
	}

	// Check if the message is a subscription event
	if gatewayMsg.IsSubscriptionEvent() {
		if err := b.handleSubscribeEvent(gatewayMsg); err != nil {
			return nil, fmt.Errorf("error handling subscription event: %w", err)
		}
	}

	return gatewayMsg.Message, nil
}

func (b *Bridge) handleSubscribeEvent(gatewayMsg websockets.GatewayMessage) error {
	subEventType := gatewayMsg.SubscriptionEventType()

	if !subEventType.IsValid() {
		return fmt.Errorf("invalid subscription event type: %s", subEventType)
	}

	switch subEventType {
	// If response is a subscription confirmation save the subscription to the subscriptions map
	case websockets.SubTypeSubscribe:
		subscription := gatewayMsg.Subscription

		b.mu.Lock()
		b.subscriptions[subscription.ID] = subscription
		b.mu.Unlock()

	// If response is a unsubscription confirmation remove the subscription from the subscriptions map
	case websockets.SubTypeUnsubscribe:
		unsubID := gatewayMsg.Unsubscription

		b.mu.Lock()
		delete(b.subscriptions, *unsubID)
		b.mu.Unlock()
	}

	return nil
}

/* ---------- Private methods - Gateway Reconnection Handling ---------- */

// reconnectToGateway reconnects to the gateway in case of connection drop with incremental backoff.
func (b *Bridge) reconnectToGateway() error {
	var backoffInterval = 500 * time.Millisecond // Initial backoff interval

	for attempt := 1; attempt <= b.maxReconnectionAttempts; attempt++ {
		b.log.Info("attempting to reconnect to gateway", slog.Int("attempt", attempt))
		gatewayConn, err := connectGateway(b.gatewayURL, b.headers)
		if err != nil {
			b.log.Error("failed to reconnect to gateway", slog.String("error", err.Error()), slog.Int("attempt", attempt))

			if attempt == b.maxReconnectionAttempts {
				b.log.Error("max reconnect attempts reached", slog.Int("maxReconnectionAttempts", b.maxReconnectionAttempts))
				return err
			}

			b.log.Info("retrying to connect after backoff interval")
			<-time.After(backoffInterval)

			// Increase the backoff interval for the next attempt
			backoffInterval *= backoffFactor
			if backoffInterval > maxBackoff {
				backoffInterval = maxBackoff
			}

			continue
		}

		b.mu.Lock()
		defer b.mu.Unlock()

		b.gatewayConn.Set(gatewayConn)

		b.log.Info("Successfully reconnected to gateway")

		if len(b.subscriptions) > 0 {
			b.log.Info(fmt.Sprintf("resuming %d subscriptions", len(b.subscriptions)))
			b.resumeSubscriptions()
		}

		return nil
	}

	return fmt.Errorf("failed to reconnect to gateway after %d attempts", b.maxReconnectionAttempts)
}

func (b *Bridge) resumeSubscriptions() {
	for _, sub := range b.subscriptions {
		var relay relay.JsonRpcRelay
		if err := json.Unmarshal(sub.RequestBody, &relay); err != nil {
			b.log.Error("error unmarshalling original subscription request body:", slog.String("error", err.Error()))
			continue
		}

		subReqBody, err := json.Marshal(relay)
		if err != nil {
			b.log.Error("error marshalling original subscription request body:", slog.String("error", err.Error()))
			continue
		}

		clientMsg := websockets.ClientMessage{Message: subReqBody, ResubscribeID: sub.ID}

		clientMsgBytes, err := json.Marshal(clientMsg)
		if err != nil {
			b.log.Error("error marshalling original subscription request body:", slog.String("error", err.Error()))
			continue
		}

		err = b.gatewayConn.WriteMessage(websocket.TextMessage, clientMsgBytes)
		if err != nil {
			b.log.Error("failed to resume subscription", slog.String("error", err.Error()))
			continue
		}

		b.log.Info("resumed subscription", slog.String("subscription", string(sub.ID)))
	}
}
