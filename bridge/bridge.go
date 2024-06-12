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
		clientConn              *websocket.Conn
		gatewayConn             *websocket.Conn
		gatewayURL              string
		headers                 http.Header
		maxReconnectionAttempts int

		stopChan       chan error
		pausePingLoop  chan struct{}
		resumePingLoop chan struct{}
		wsLock         sync.Mutex

		subscriptions map[websockets.SubscriptionID]*websockets.Subscription
		subsLock      sync.RWMutex

		log *logger.Logger
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
	b := &Bridge{
		clientConn:              config.ClientConn,
		gatewayURL:              config.GatewayURL,
		headers:                 config.Headers,
		maxReconnectionAttempts: config.MaxReconnectionAttempts,

		stopChan:       make(chan error),
		pausePingLoop:  make(chan struct{}),
		resumePingLoop: make(chan struct{}),
		wsLock:         sync.Mutex{},

		subscriptions: make(map[websockets.SubscriptionID]*websockets.Subscription),
		subsLock:      sync.RWMutex{},

		log: config.Log,
	}

	gatewayConn, err := b.connectGateway()
	if err != nil {
		return nil, fmt.Errorf("error establishing connection to gateway: %s", err.Error())
	}

	b.gatewayConn = gatewayConn

	return b, nil
}

/* ---------- Public method - Run Bridge ---------- */

// Run starts the bridge and establishes a bidirectional communication between the client and server
func (b *Bridge) Run() {
	// Start goroutine to read from client and write to gateway
	go b.clientLoop()

	// Start goroutine to read from gateway and write to client
	// This is also where the gateway reconnection logic is implemented
	go b.gatewayLoop()

	// Start goroutines to ping/pong the client and gateway
	go b.clientPingLoop()
	go b.gatewayPingLoop()

	b.log.Info("bridge operation started successfully")

	// If close signal is received, stop the bridge and close both connections
	stopErr := <-b.stopChan
	if err := b.cleanup(stopErr); err != nil {
		b.log.Error("error cleaning up bridge:", slog.String("error", err.Error()))
	}
}

/* ---------- Private methods - Handle Config ---------- */

// connectGateway connects to the gateway and returns the websocket connection.
func (b *Bridge) connectGateway() (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(b.gatewayURL, b.headers)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

/* ---------- Private methods - Handle Shutdown ---------- */

// cleanup closes the client and gateway connections
func (b *Bridge) cleanup(err error) error {
	b.wsLock.Lock()
	defer b.wsLock.Unlock()

	closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, err.Error())

	// Close the client connection with the gateway and send a reason for the closure
	if err := b.clientConn.WriteMessage(websocket.CloseMessage, closeMsg); err != nil {
		b.log.Error("error writing close message to client connection:", slog.String("error", err.Error()))
	}
	if err := b.clientConn.Close(); err != nil {
		b.log.Error("error closing client connection:", slog.String("error", err.Error()))
	}

	// Close the gateway connection with the client and send a reason for the closure
	if err := b.gatewayConn.WriteMessage(websocket.CloseMessage, closeMsg); err != nil {
		b.log.Error("error writing close message to gateway connection:", slog.String("error", err.Error()))
	}
	if err := b.gatewayConn.Close(); err != nil {
		b.log.Error("error closing gateway connection:", slog.String("error", err.Error()))
	}

	b.log.Info("bridge operation stopped successfully")
	return nil
}

// closeBridge logs the error and sends it to the stopChan to stop the bridge
func (b *Bridge) closeBridge(errStr string, err error) {
	b.wsLock.Lock()
	defer b.wsLock.Unlock()

	b.log.Error(errStr, slog.String("error", err.Error()))

	b.stopChan <- fmt.Errorf("%s: %w", errStr, err)
	close(b.stopChan)
}

/* ---------- Private methods - WebSocket loop methods ---------- */

// clientLoop reads from the client connection and writes to the gateway connection
func (b *Bridge) clientLoop() {
	for {
		select {
		case <-b.stopChan:
			return

		default:
			messageType, msg, err := b.clientConn.ReadMessage()
			if err != nil {
				b.handleClientError(err)
				return
			}

			clientMsg := websockets.ClientMessage{Message: msg}
			clientMsgBytes, err := json.Marshal(clientMsg)
			if err != nil {
				b.log.Error("error marshalling client message:", slog.String("error", err.Error()))
				continue
			}

			b.wsLock.Lock()
			err = b.gatewayConn.WriteMessage(messageType, clientMsgBytes)
			if err != nil {
				b.wsLock.Unlock()
				// An error writing means the connection is broken and the bridge should be stopped
				b.closeBridge("error writing to gateway websocket", err)
				return
			}
			b.wsLock.Unlock()
		}
	}
}

func (b *Bridge) handleClientError(err error) {
	if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
		b.log.Info("client websocket closed") // don't log error when client closes
		return
	}

	// If the error is not a close error it is net error; this handles the case where the
	// client connection is closed due to the gateway reconnection failing
	if _, ok := err.(*websocket.CloseError); !ok {
		return
	}

	b.closeBridge("error reading from client websocket", err)
}

// gatewayReadLoop reads from the gateway connection and writes to the gatewayMsgChan
// This is also where the gateway reconnection logic is implemented
func (b *Bridge) gatewayLoop() {
	for {
		select {
		case <-b.stopChan:
			return

		default:
			messageType, message, err := b.gatewayConn.ReadMessage()
			if err != nil {
				if reconnectedToGateway := b.handleGatewayError(err); reconnectedToGateway {
					continue // Resume gateway read loop if reconnection was successful
				} else {
					return
				}
			}

			// Check if the message is a subscription event or a response to a pending subscribe or unsubscribe request
			processedMsg, err := b.processGatewayResponse(message)
			if err != nil {
				b.log.Error("error processing gateway response:", slog.String("error", err.Error()))
				continue
			}

			// If the message is a resubscribe confirmation from the gateway do not forward it to the client
			if processedMsg == nil {
				continue
			}

			b.wsLock.Lock()
			err = b.clientConn.WriteMessage(messageType, processedMsg)
			if err != nil {
				b.wsLock.Unlock()
				// An error writing means the connection is broken and the bridge should be stopped
				b.closeBridge("error writing to client websocket", err)
				return
			}
			b.wsLock.Unlock()
		}
	}
}

func (b *Bridge) handleGatewayError(err error) bool {
	// If the gateway connection is closed, attempt to reconnect
	if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
		b.log.Info("gateway websocket closed unexpectedly, attempting to reconnect")

		if reconnectErr := b.reconnectToGateway(); reconnectErr == nil {
			return true // Resume gateway read loop if reconnection was successful
		}

		// If the gateway reconnection failed, stop the bridge operation
		b.closeBridge("gateway connection lost", err)
		return false
	}

	// If the error is not a close error it is net error; this handles the case where the
	// gateway connection is closed in response to the client connection being closed
	if _, ok := err.(*websocket.CloseError); !ok {
		return false
	}

	// If the gateway connection is closed, stop the bridge
	b.closeBridge("error reading from gateway websocket", err)
	return false
}

// clientPingLoop sends keep-alive ping messages to the connection and handles pong messages
func (b *Bridge) clientPingLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
	}()

	// Set initial read deadline
	if err := b.clientConn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
		b.log.Error("failed to set initial read deadline:", slog.String("error", err.Error()))
	}
	// Extend read deadline on pong response
	b.clientConn.SetPongHandler(func(string) error {
		if err := b.clientConn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			b.log.Error("failed to set pong handler read deadline:", slog.String("error", err.Error()))
		}
		return nil
	})

	for {
		select {
		case <-b.stopChan:
			return

		case <-ticker.C:
			b.wsLock.Lock()
			if err := b.clientConn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				b.wsLock.Unlock()
				b.closeBridge("failed to send ping to client", err)
				return
			}
			b.wsLock.Unlock()
		}
	}
}

// gatewayPingLoop sends keep-alive ping messages to the connection and handles pong messages
func (b *Bridge) gatewayPingLoop() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
	}()

	initPingLoop := func() {
		b.wsLock.Lock()
		// Set initial read deadline
		if err := b.gatewayConn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
			b.log.Error("failed to set initial read deadline:", slog.String("error", err.Error()))
		}
		// Extend read deadline on pong response
		b.gatewayConn.SetPongHandler(func(string) error {
			if err := b.gatewayConn.SetReadDeadline(time.Now().Add(pongWait)); err != nil {
				b.log.Error("failed to set pong handler read deadline:", slog.String("error", err.Error()))
			}

			return nil
		})
		b.wsLock.Unlock()
	}

	initPingLoop()
	paused := false

	for {
		select {
		case <-b.stopChan:
			return

		case <-b.pausePingLoop:
			paused = true

		case <-b.resumePingLoop:
			paused = false
			initPingLoop()

		case <-ticker.C:
			if paused {
				continue

			}

			b.wsLock.Lock()
			if err := b.gatewayConn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				b.wsLock.Unlock()
				b.closeBridge("failed to send ping to gateway", err)
				return
			}
			b.wsLock.Unlock()
		}
	}
}

/* ---------- Private methods - Gateway Response Handling ---------- */

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

		b.subsLock.Lock()
		b.subscriptions[subscription.ID] = subscription
		b.subsLock.Unlock()

	// If response is a unsubscription confirmation remove the subscription from the subscriptions map
	case websockets.SubTypeUnsubscribe:
		unsubID := gatewayMsg.Unsubscription

		b.subsLock.Lock()
		delete(b.subscriptions, *unsubID)
		b.subsLock.Unlock()
	}

	return nil
}

/* ---------- Private methods - Gateway Reconnection Handling ---------- */

// reconnectToGateway reconnects to the gateway in case of connection drop with incremental backoff.
func (b *Bridge) reconnectToGateway() error {
	var backoffInterval = 500 * time.Millisecond // Initial backoff interval

	b.pausePingLoop <- struct{}{}

	for attempt := 1; attempt <= b.maxReconnectionAttempts; attempt++ {
		b.log.Info("attempting to reconnect to gateway", slog.Int("attempt", attempt))
		gatewayConn, err := b.connectGateway()
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

		b.wsLock.Lock()
		b.gatewayConn = gatewayConn
		b.wsLock.Unlock()

		b.log.Info("Successfully reconnected to gateway")

		if len(b.subscriptions) > 0 {
			b.log.Info(fmt.Sprintf("resuming %d subscriptions", len(b.subscriptions)))
			b.resumeSubscriptions()
		}

		b.resumePingLoop <- struct{}{}

		return nil
	}

	return fmt.Errorf("failed to reconnect to gateway after %d attempts", b.maxReconnectionAttempts)
}

func (b *Bridge) resumeSubscriptions() {
	b.subsLock.Lock()
	defer b.subsLock.Unlock()

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
