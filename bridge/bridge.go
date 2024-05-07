package bridge

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/pokt-foundation/utils-go/logger"
	relayPkg "github.com/pokt-foundation/wss-manager/relay"
	subPkg "github.com/pokt-foundation/wss-manager/subscription"
)

const (
	ethSubscribe   = "eth_subscribe"
	ethUnsubscribe = "eth_unsubscribe"

	uuidLen = 36

	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second
	// Time allowed to read the next pong message from the peer.
	pongWait = 30 * time.Second
	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10
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
		maxReconnectionAttempts int
		stopChan                chan error
		pausePingLoop           chan struct{}
		resumePingLoop          chan struct{}
		wsLock                  sync.Mutex

		subsByCurrentID  map[subPkg.SubscriptionID]*subPkg.Subscription
		subsByOriginalID map[subPkg.SubscriptionID]*subPkg.Subscription
		pendingSubs      map[string]subPkg.PendingSubscribe
		pendingUnsubs    map[string]subPkg.PendingUnsubscribe
		pendingResubs    map[string]subPkg.SubscriptionID
		subsLock         sync.RWMutex

		log *logger.Logger
	}

	Config struct {
		ClientConn              *websocket.Conn
		GatewayURL              string
		MaxReconnectionAttempts int
		Log                     *logger.Logger
	}
)

// NewBridge creates a new Bridge instance and a new connection to the Gateway from the Gateway URL
func NewBridge(config Config) (*Bridge, error) {
	b := &Bridge{
		clientConn:              config.ClientConn,
		gatewayURL:              config.GatewayURL,
		maxReconnectionAttempts: config.MaxReconnectionAttempts,
		stopChan:                make(chan error),
		pausePingLoop:           make(chan struct{}),
		resumePingLoop:          make(chan struct{}),
		wsLock:                  sync.Mutex{},

		subsByCurrentID:  make(map[subPkg.SubscriptionID]*subPkg.Subscription),
		subsByOriginalID: make(map[subPkg.SubscriptionID]*subPkg.Subscription),
		pendingSubs:      make(map[string]subPkg.PendingSubscribe),
		pendingUnsubs:    make(map[string]subPkg.PendingUnsubscribe),
		pendingResubs:    make(map[string]subPkg.SubscriptionID),
		subsLock:         sync.RWMutex{},

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
	go b.clientPingLoop()

	// Start goroutine to read from gateway and write to client
	// This is also where the gateway reconnection logic is implemented
	go b.gatewayLoop()
	go b.gatewayPingLoop()

	b.log.Info("bridge operation started successfully")

	// If close signal is received, stop the bridge and close both connections
	stopErr := <-b.stopChan
	if err := b.cleanup(stopErr); err != nil {
		b.log.Error("error cleaning up bridge:", slog.String("error", err.Error()))
	}
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
				return
			}

			// Check if the message is a subscribe or unsubscribe request
			processedMsg, err := b.processClientRequest(msg)
			if err != nil {
				b.log.Error("error processing client request:", slog.String("error", err.Error()))
				continue
			}

			b.wsLock.Lock()
			err = b.gatewayConn.WriteMessage(messageType, processedMsg)
			if err != nil {
				b.wsLock.Unlock()

				b.closeBridge("error writing to gateway websocket", err)
				return
			}
			b.wsLock.Unlock()
		}
	}
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
				// If the gateway connection is closed, attempt to reconnect
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
					b.log.Info("gateway websocket closed unexpectedly, attempting to reconnect")

					if reconnectErr := b.reconnectToGateway(); reconnectErr == nil {
						continue // Resume gateway read loop if reconnection was successful
					}

					// If the gateway reconnection failed, stop the bridge operation
					b.closeBridge("gateway connection lost", err)
					return
				}

				// If the error is not a close error it is net error; this handles the case where the
				// gateway connection is closed in response to the client connection being closed
				if _, ok := err.(*websocket.CloseError); !ok {
					return
				}

				// If the gateway connection is closed, stop the bridge
				b.closeBridge("error reading from gateway websocket", err)
				return
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

				b.closeBridge("error writing to client websocket", err)
				return
			}
			b.wsLock.Unlock()
		}
	}
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

/* ---------- Private methods - Client Request Handling ---------- */

// processClientRequest checks if a client request is either an 'eth_subscribe' or 'eth_unsubscribe' method.
func (b *Bridge) processClientRequest(message []byte) ([]byte, error) {
	var relay relayPkg.Relay
	if err := json.Unmarshal(message, &relay); err != nil {
		return nil, fmt.Errorf("error unmarshalling client request: %w", err)
	}

	switch relay.Method {
	// If valid 'eth_subscribe' method save the subscription to pending subs map
	case ethSubscribe:
		return b.handleSubscribeRequest(relay, message)
	// If valid 'eth_unsubscribe' method save the subscription to pending unsubs map
	case ethUnsubscribe:
		return b.handleUnsubscribeRequest(relay)
	// If neither just return the unmodified message
	default:
		return message, nil
	}
}

func (b *Bridge) handleSubscribeRequest(relay relayPkg.Relay, requestBody []byte) ([]byte, error) {
	b.log.Info("received eth_subscribe request from client")

	// Generate a temporary relay ID for the pending subscribe request
	tempRelayID := uuid.New().String()

	// Store the pending subscription with the temporary relay ID
	b.subsLock.Lock()
	b.pendingSubs[tempRelayID] = subPkg.PendingSubscribe{
		OriginalRelayID: relay.ID,
		RequestBody:     requestBody,
	}
	b.subsLock.Unlock()

	// Replace the original relay ID with the temporary one in the message
	relay.ID = relayPkg.IDFromString(tempRelayID)

	// Marshal the modified relay message
	modifiedMessage, err := json.Marshal(relay)
	if err != nil {
		b.log.Error("error marshalling subscribe relay:", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error marshalling subscribe relay: %w", err)
	}

	return modifiedMessage, nil
}

func (b *Bridge) handleUnsubscribeRequest(relay relayPkg.Relay) ([]byte, error) {
	b.log.Info("received eth_unsubscribe request from client")

	var params []string
	if err := json.Unmarshal(relay.Params, &params); err != nil {
		return nil, fmt.Errorf("error unmarshalling unsubscribe request: %w", err)
	}

	// Get current sub ID from subsByOriginalID map
	b.subsLock.RLock()
	subscription, ok := b.subsByOriginalID[subPkg.SubscriptionID(params[0])]
	b.subsLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("subscription not found")
	}

	// Replace params slice with current sub ID
	currentSubIDParams := []subPkg.SubscriptionID{subscription.CurrentSubID()}
	paramsJSON, err := json.Marshal(currentSubIDParams)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay params: %w", err)
	}
	relay.Params = json.RawMessage(paramsJSON)

	// Generate a temporary relay ID for the pending unsubscribe request
	tempRelayID := uuid.New().String()

	// Store the pending subscription with the temporary relay ID
	b.subsLock.Lock()
	b.pendingUnsubs[tempRelayID] = subPkg.PendingUnsubscribe{
		OriginalRelayID: relay.ID,
		OriginalSubID:   subscription.OriginalSubID(),
	}
	b.subsLock.Unlock()

	// Replace the original relay ID with the temporary one in the message
	relay.ID = relayPkg.IDFromString(tempRelayID)

	// Marshal the modified relay message
	modifiedMessage, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	return modifiedMessage, nil
}

/* ---------- Private methods - Gateway Response Handling ---------- */

// processGatewayResponse checks if a gateway response is an active subscription event,
// or a response to an 'eth_subscribe', 'eth_unsubscribe' or resubscription request.
func (b *Bridge) processGatewayResponse(message []byte) ([]byte, error) {
	var relay relayPkg.Relay
	if err := json.Unmarshal(message, &relay); err != nil {
		return nil, fmt.Errorf("error unmarshalling gateway response: %w", err)
	}

	// If the relay is a subscription event ensure it contains the original subscription ID
	if params, ok := b.isSubscriptionEvent(relay); ok {
		return b.handleSubscriptionEvent(params, relay)
	}

	// If a response to a subscribe or unsubscribe request the ID will be a temp UUID string
	if tempRelayID := relay.ID.String(); tempRelayID != "" && len(tempRelayID) == uuidLen {
		// If response is a subscription confirmation save the subscription to the subscriptions map
		b.subsLock.RLock()
		if pendingSub, ok := b.pendingSubs[tempRelayID]; ok {
			b.subsLock.RUnlock()
			return b.handleSubscribeResponse(relay, pendingSub, tempRelayID)
		}
		b.subsLock.RUnlock()

		// If response is a unsubscription confirmation remove the subscription from the subscriptions map
		b.subsLock.RLock()
		if pendingUnsub, ok := b.pendingUnsubs[tempRelayID]; ok {
			b.subsLock.RUnlock()
			return b.handleUnsubscribeResponse(relay, pendingUnsub, tempRelayID)
		}
		b.subsLock.RUnlock()

		// If response is a resubscribe confirmation update the current sub ID
		b.subsLock.RLock()
		if pendingResub, ok := b.pendingResubs[tempRelayID]; ok {
			b.subsLock.RUnlock()
			err := b.handleResubscribeResponse(relay, pendingResub, tempRelayID)
			if err != nil {
				return nil, fmt.Errorf("error handling resubscribe response: %w", err)
			}
			// If resubscribe response is successful return nil to signal to handler not to send response to client
			return nil, nil
		}
		b.subsLock.RUnlock()
	}

	// If none of the above return the unmodified message
	return message, nil
}

func (b *Bridge) isSubscriptionEvent(relay relayPkg.Relay) (relayPkg.SubscriptionEventParams, bool) {
	var params relayPkg.SubscriptionEventParams
	if err := json.Unmarshal(relay.Params, &params); err == nil {
		return params, true
	}

	return params, false
}

func (b *Bridge) handleSubscriptionEvent(params relayPkg.SubscriptionEventParams, relay relayPkg.Relay) ([]byte, error) {
	b.subsLock.RLock()
	subscription, ok := b.subsByCurrentID[subPkg.SubscriptionID(params.Subscription)]
	b.subsLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("subscription not found for current sub ID: %s", params.Subscription)
	}

	// Ensure subscription ID is the original subscription ID
	params.Subscription = string(subscription.OriginalSubID())

	jsonParams, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("error marshalling subscription event params: %w", err)
	}
	relay.Params = json.RawMessage(jsonParams)

	if relay.ID != nil && relay.ID.String() == "" {
		relay.ID = nil
	}

	json, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling subscription event params: %w", err)
	}

	return json, nil
}

func (b *Bridge) handleSubscribeResponse(relay relayPkg.Relay, pendingSub subPkg.PendingSubscribe, tempRelayID string) ([]byte, error) {
	b.log.Info("received eth_subscribe confirmation from gateway")

	if !relay.IsError() {
		if err := b.addSubscription(relay, pendingSub.RequestBody); err != nil {
			return nil, fmt.Errorf("error adding subscription: %w", err)
		}
	}

	// Replace original relay ID before returning response to client
	relay.ID = pendingSub.OriginalRelayID

	msgWithOriginalID, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling subscription confirmation: %w", err)
	}

	// Clear pending sub from map
	b.subsLock.Lock()
	delete(b.pendingSubs, tempRelayID)
	b.subsLock.Unlock()

	return msgWithOriginalID, nil
}

func (b *Bridge) addSubscription(relay relayPkg.Relay, requestBody []byte) error {
	var subID subPkg.SubscriptionID
	if err := json.Unmarshal(relay.Result, &subID); err != nil {
		return fmt.Errorf("error unmarshalling subscription ID: %w", err)
	}

	subscription := subPkg.NewSubscription(subID, requestBody)

	b.subsLock.Lock()
	b.subsByCurrentID[subID] = subscription
	b.subsByOriginalID[subID] = subscription
	b.subsLock.Unlock()

	return nil
}

func (b *Bridge) handleUnsubscribeResponse(relay relayPkg.Relay, pendingUnsub subPkg.PendingUnsubscribe, tempRelayID string) ([]byte, error) {
	b.log.Info("received eth_unsubscribe confirmation from gateway")

	if !relay.IsError() {
		// If the unsubscribe was successful the result field will be 'true'
		var unsubConfirmation bool
		if err := json.Unmarshal(relay.Result, &unsubConfirmation); err != nil {
			return nil, fmt.Errorf("error unmarshalling unsubscribe confirmation: %w", err)
		}
		if unsubConfirmation {
			if err := b.removeSubscription(pendingUnsub.OriginalSubID); err != nil {
				return nil, fmt.Errorf("error removing subscription: %w", err)
			}
		}
	}

	// Replace original relay ID before returning response to client
	relay.ID = pendingUnsub.OriginalRelayID

	msgWithOriginalID, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	// Clear pending unsub from map
	b.subsLock.Lock()
	delete(b.pendingUnsubs, tempRelayID)
	b.subsLock.Unlock()

	return msgWithOriginalID, nil
}

func (b *Bridge) removeSubscription(originalSubID subPkg.SubscriptionID) error {
	b.subsLock.RLock()
	subscription, ok := b.subsByOriginalID[originalSubID]
	b.subsLock.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found for original sub ID: %s", originalSubID)
	}

	b.subsLock.Lock()
	delete(b.subsByOriginalID, originalSubID)
	delete(b.subsByCurrentID, subscription.CurrentSubID())
	b.subsLock.Unlock()

	return nil
}

func (b *Bridge) handleResubscribeResponse(relay relayPkg.Relay, originalSubID subPkg.SubscriptionID, tempRelayID string) error {
	b.log.Info("received eth_subscribe resubscribe confirmation from gateway")

	err := b.updateSubscription(relay, originalSubID)
	if err != nil {
		return fmt.Errorf("error handling resubscribe response: %w", err)
	}

	// Clear pending resub from map
	b.subsLock.Lock()
	delete(b.pendingResubs, tempRelayID)
	b.subsLock.Unlock()

	return nil
}

func (b *Bridge) updateSubscription(relay relayPkg.Relay, originalSubID subPkg.SubscriptionID) error {
	var newSubID subPkg.SubscriptionID
	if err := json.Unmarshal(relay.Result, &newSubID); err != nil {
		return fmt.Errorf("error unmarshalling subscription ID: %w", err)
	}

	b.subsLock.RLock()
	subscription, ok := b.subsByOriginalID[originalSubID]
	b.subsLock.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found for original sub ID: %s", originalSubID)
	}

	// Update current sub ID in current sub IDs map and remove original sub ID from map
	b.subsLock.Lock()
	subscription.SetCurrentSubID(newSubID)
	b.subsByCurrentID[newSubID] = subscription
	delete(b.subsByCurrentID, originalSubID)
	b.subsLock.Unlock()

	return nil
}
