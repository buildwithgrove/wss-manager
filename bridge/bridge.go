package bridge

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/pokt-foundation/portal-middleware/relay"
	"github.com/pokt-foundation/portal-middleware/websockets"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/metrics"
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
		clientConn  *websockets.Connection
		gatewayConn *websockets.Connection
		msgChan     <-chan websockets.Message
		stopChan    chan error

		subscriptions map[websockets.SubscriptionID]*websockets.Subscription

		mu sync.RWMutex

		metrics *metrics.MetricExporter
		log     *slog.Logger
	}
	Config struct {
		ClientConn              *websocket.Conn
		GatewayURL              string
		MetricExporter          *metrics.MetricExporter
		Headers                 http.Header
		MaxReconnectionAttempts int
		Log                     *logger.Logger
	}
)

// NewBridge creates a new Bridge instance and a new connection to the Gateway from the Gateway URL
func NewBridge(config Config) (*Bridge, error) {
	gatewayConn, _, err := websocket.DefaultDialer.Dial(config.GatewayURL, config.Headers)
	if err != nil {
		return nil, fmt.Errorf("error establishing connection to gateway URL %s Error: %s", config.GatewayURL, err.Error())
	}

	msgChan := make(chan websockets.Message)
	stopChan := make(chan error)

	log := config.Log.With("component", "bridge")

	b := &Bridge{
		msgChan:       msgChan,
		stopChan:      stopChan,
		metrics:       config.MetricExporter,
		subscriptions: make(map[websockets.SubscriptionID]*websockets.Subscription),
		mu:            sync.RWMutex{},
		log:           log,
	}

	// Establish both connections and start reading from them
	b.gatewayConn = websockets.NewConnection(websockets.ConnConfig{
		Conn:   gatewayConn,
		Source: websockets.SourceBackend,
		// Only the gateway conn will attempt to reconnect
		ReconnectConfig: &websockets.ReconnectConfig{
			GatewayURL:              config.GatewayURL,
			Headers:                 config.Headers,
			MaxReconnectionAttempts: config.MaxReconnectionAttempts,
			SubsFunc:                b.resumeSubscriptions,
		},
		MsgChan:  msgChan,
		StopChan: stopChan,
		Log:      log.With("conn", "gateway"),
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

/* ---------- Public method - Run Bridge ---------- */

// Run starts the bridge and establishes a bidirectional communication between the client and server
func (b *Bridge) Run() {
	defer func() {
		// When bridge is closed, remove all subscriptions from the metrics gauge
		b.metrics.AddSubscribe(float64(-len(b.subscriptions)))

		// handle recover to avoid panic
		if r := recover(); r != nil {
			b.metrics.IncPanicRecovered("bridge", fmt.Sprintf("%v", r))
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

/* ---------- Private methods - Message handling ---------- */

// handleClientMessage processes a message from the Client and sends it to the Gateway
func (b *Bridge) handleClientMessage(msg websockets.Message) {
	b.metrics.IncClientRelayAttempt()

	clientMsg := websockets.ClientMessage{Message: msg.Data}
	clientMsgBytes, err := json.Marshal(clientMsg)
	if err != nil {
		errMsg := fmt.Sprintf("error marshalling client message: %s", err.Error())
		b.log.Error(errMsg)
		b.metrics.IncClientRelayError(errMsg, metrics.LabelErrorMarshal)
		if err := b.clientConn.WriteMessage(websocket.TextMessage, []byte(errMsg)); err != nil {
			b.log.Error("error writing error message to client websocket", slog.String("error", err.Error()))
		}
		return
	}

	err = b.gatewayConn.WriteMessage(msg.MessageType, clientMsgBytes)
	if err != nil {
		errMsg := fmt.Sprintf("error writing to gateway websocket: %s", err.Error())
		b.log.Error(errMsg)
		b.metrics.IncGatewayRelayError(errMsg, metrics.LabelErrorWrite)
		if err := b.clientConn.WriteMessage(websocket.TextMessage, []byte(errMsg)); err != nil {
			b.log.Error("error writing error message to client websocket", slog.String("error", err.Error()))
		}

		// An error writing means the connection is broken and the bridge should be stopped
		b.stopChan <- fmt.Errorf(errMsg)
		return
	}

	b.metrics.IncClientRelaySuccess()
}

// handleGatewayMessage processes a message from the Gateway and sends it to the Client
func (b *Bridge) handleGatewayMessage(msg websockets.Message) {
	b.metrics.IncGatewayRelayAttempt()

	// Check if the message is a subscription event or a response to a pending subscribe or unsubscribe request
	processedMsg, err := b.processGatewayResponse(msg.Data)
	if err != nil {
		errMsg := fmt.Sprintf("error processing gateway response: %s", err.Error())
		b.log.Error(errMsg)
		b.metrics.IncGatewayRelayError(errMsg, metrics.LabelErrorProcess)
		if err := b.gatewayConn.WriteMessage(websocket.TextMessage, []byte(errMsg)); err != nil {
			b.log.Error("error writing to error message to gateway websocket", slog.String("error", err.Error()))
		}
		return
	}

	// If the message is a resubscribe confirmation from the gateway do not forward it to the client
	if processedMsg == nil {
		return
	}

	err = b.clientConn.WriteMessage(msg.MessageType, processedMsg)
	if err != nil {
		errMsg := fmt.Sprintf("error writing to client websocket: %s", err.Error())
		b.log.Error(errMsg)
		b.metrics.IncClientRelayError(errMsg, metrics.LabelErrorWrite)
		if err := b.gatewayConn.WriteMessage(websocket.TextMessage, []byte(errMsg)); err != nil {
			b.log.Error("error writing error message to gateway websocket", slog.String("error", err.Error()))
		}

		// An error writing means the connection is broken and the bridge should be stopped
		b.stopChan <- fmt.Errorf(errMsg)
		return
	}

	b.metrics.IncGatewayRelaySuccess()
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

/* ---------- Private methods - Subscription Handling ---------- */

func (b *Bridge) handleSubscribeEvent(gatewayMsg websockets.GatewayMessage) error {
	subEventType := gatewayMsg.SubscriptionEventType()

	if !subEventType.IsValid() {
		errMsg := fmt.Sprintf("invalid subscription event type: %s", subEventType)
		b.metrics.IncSubscribeError(errMsg)
		return fmt.Errorf(errMsg)
	}

	switch subEventType {
	// If response is a subscription confirmation save the subscription to the subscriptions map
	case websockets.SubTypeSubscribe:
		subscription := gatewayMsg.Subscription

		b.mu.Lock()
		b.subscriptions[subscription.ID] = subscription
		b.mu.Unlock()

		b.metrics.AddSubscribe(1)

	// If response is a unsubscription confirmation remove the subscription from the subscriptions map
	case websockets.SubTypeUnsubscribe:
		unsubID := gatewayMsg.Unsubscription

		b.mu.Lock()
		delete(b.subscriptions, *unsubID)
		b.mu.Unlock()

		b.metrics.AddSubscribe(-1)
	}

	return nil
}

func (b *Bridge) resumeSubscriptions() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	b.log.Info("resuming subscriptions")
	b.metrics.IncReconnect()

	for _, sub := range b.subscriptions {
		var relay relay.JsonRpcRelay
		if err := json.Unmarshal(sub.RequestBody, &relay); err != nil {
			b.log.Error("error unmarshalling original subscription request body:", slog.String("error", err.Error()))
			b.metrics.IncResubscribeError(err.Error(), metrics.LabelErrorMarshal)
			continue
		}

		subReqBody, err := json.Marshal(relay)
		if err != nil {
			b.log.Error("error marshalling original subscription request body:", slog.String("error", err.Error()))
			b.metrics.IncResubscribeError(err.Error(), metrics.LabelErrorMarshal)
			continue
		}

		clientMsg := websockets.ClientMessage{Message: subReqBody, ResubscribeID: sub.ID}

		clientMsgBytes, err := json.Marshal(clientMsg)
		if err != nil {
			b.log.Error("error marshalling original subscription request body:", slog.String("error", err.Error()))
			b.metrics.IncResubscribeError(err.Error(), metrics.LabelErrorMarshal)
			continue
		}

		err = b.gatewayConn.WriteMessage(websocket.TextMessage, clientMsgBytes)
		if err != nil {
			b.log.Error("failed to resume subscription", slog.String("error", err.Error()))
			b.metrics.IncResubscribeError(err.Error(), metrics.LabelErrorWrite)
			continue
		}

		b.log.Info("resumed subscription", slog.String("subscription", string(sub.ID)))
		b.metrics.IncResubscribeSuccess(string(sub.ID))
	}
}
