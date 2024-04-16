package bridge

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/utils-go/logger"
	relayPkg "github.com/pokt-foundation/wss-manager/relay"
	subPkg "github.com/pokt-foundation/wss-manager/subscription"
)

const (
	ethSubscribe   = "eth_subscribe"
	ethUnsubscribe = "eth_unsubscribe"
)

type (
	// Bridge routes data between clients and servers
	Bridge struct {
		id string

		clientConn     wsConnection
		gatewayConn    wsConnection
		gatewayURL     string
		gatewayMsgChan chan wsMessage
		doneChan       chan struct{}

		subsByCurrentID  map[subPkg.SubscriptionID]*subPkg.Subscription
		subsByOriginalID map[subPkg.SubscriptionID]*subPkg.Subscription

		// TODO - clear pending subs on interval?
		pendingSubs   map[string]subPkg.PendingSubscribe
		pendingUnsubs map[string]subPkg.PendingUnsubscribe
		pendingResubs map[string]subPkg.SubscriptionID

		mu  sync.RWMutex
		log *logger.Logger
	}

	wsConnection interface {
		ReadMessage() (messageType int, p []byte, err error)
		WriteMessage(messageType int, data []byte) error
		Close() error
	}

	wsMessage struct {
		messageType int
		message     []byte
		err         error
	}
)

func NewBridge(clientConn wsConnection, gatewayURL string, log *logger.Logger) (*Bridge, error) {
	bridge := &Bridge{
		id:               uuid.New().String(),
		clientConn:       clientConn,
		gatewayURL:       gatewayURL,
		gatewayMsgChan:   make(chan wsMessage, 100_000),
		doneChan:         make(chan struct{}),
		subsByCurrentID:  make(map[subPkg.SubscriptionID]*subPkg.Subscription),
		subsByOriginalID: make(map[subPkg.SubscriptionID]*subPkg.Subscription),
		pendingSubs:      make(map[string]subPkg.PendingSubscribe),
		pendingUnsubs:    make(map[string]subPkg.PendingUnsubscribe),
		pendingResubs:    make(map[string]subPkg.SubscriptionID),
		mu:               sync.RWMutex{},
		log:              log,
	}

	gatewayWS, err := bridge.connectGateway()
	if err != nil {
		return nil, fmt.Errorf("error establishing connection to gateway: %s", err.Error())
	}

	bridge.gatewayConn = gatewayWS

	return bridge, nil
}

// Run starts the bridge and establishes a bidirectional communication between the client and server
func (b *Bridge) Run() {
	// Start goroutine to read from client and write to gateway
	go func() {
		for {
			messageType, msg, err := b.clientConn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					b.log.Info("client websocket closed")

					// If client connection is closed, close the gateway connection as well
					close(b.doneChan)
					// close(b.gatewayMsgChan)
					b.gatewayConn.Close()

					return
				}

				b.log.Error("error reading from client websocket:", slog.String("error", err.Error()))
				return
			}

			// Check if the message is a subscribe or unsubscribe request
			processedMsg, err := b.processClientRequest(msg)
			if err != nil {
				b.log.Error("error processing client request:", slog.String("error", err.Error()))
				continue
			}

			err = b.gatewayConn.WriteMessage(messageType, processedMsg)
			if err != nil {
				b.log.Error("error writing to gateway websocket:", slog.String("error", err.Error()))
				return
			}
		}
	}()

	// Start goroutine to read from gateway and send to gatewayMsgChan
	go func() {
		for {
			messageType, message, err := b.gatewayConn.ReadMessage()
			if err != nil {
				// If the gateway connection is closed, attempt to reconnect
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
					b.log.Info("gateway websocket closed unexpectedly, attempting to reconnect")

					if reconnectErr := b.reconnectToGateway(); reconnectErr == nil {
						continue // Resume gateway read loop if reconnection was successful
					}

					b.log.Error("failed to reconnect to gateway, stopping bridge operation")
					return
				}

				// If the error is not a close error it is net error; this handles the case where the
				// gateway connection is closed in response to the client connection being closed
				// TODO - does this handle all possible errors in this case; don't want to be ignoring valid errors if not
				if _, ok := err.(*websocket.CloseError); !ok {
					return
				}
			}

			// Wrap responses in a wsMessage struct and send it to gatewayMsgChan
			b.gatewayMsgChan <- wsMessage{messageType: messageType, message: message, err: err}
		}
	}()

	// Start goroutine to read from gatewayMsgChan and write to client
	go func() {
		for {
			select {
			// If client connection closed, close the gateway connection
			case <-b.doneChan:
				b.log.Info("gateway websocket closed")
				return

			// Otherwise read from gatewayMsgChan and write to client
			case msg := <-b.gatewayMsgChan:
				messageType, message, err := msg.messageType, msg.message, msg.err
				if err != nil {
					b.log.Error("error reading from gateway websocket:", slog.String("error", err.Error()))
					return
				}

				// Check if the message is a response to a pending subscribe or unsubscribe request
				processedMsg, err := b.processGatewayResponse(message)
				if err != nil {
					b.log.Error("error processing gateway request:", slog.String("error", err.Error()))
					continue
				}

				// If the message is a resubscribe confirmation from the gateway do not forward it to the client
				if processedMsg == nil {
					continue
				}

				err = b.clientConn.WriteMessage(messageType, processedMsg)
				if err != nil {
					b.log.Error("error writing to client websocket:", slog.String("error", err.Error()))
					return
				}
			}
		}
	}()
}

/* ---------- Private methods ---------- */

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
	b.log.Info("Received eth_subscribe request from client")

	// Generate a temporary relay ID for the pending subscribe request
	tempRelayID := uuid.New().String()

	// Store the pending subscription with the temporary relay ID
	b.mu.Lock()
	b.pendingSubs[tempRelayID] = subPkg.PendingSubscribe{
		OriginalRelayID: relay.ID,
		RequestBody:     requestBody,
	}
	b.mu.Unlock()

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
	b.log.Info("Received eth_unsubscribe request from client")

	var params []string
	if err := json.Unmarshal(relay.Params, &params); err != nil {
		return nil, fmt.Errorf("error unmarshalling unsubscribe request: %w", err)
	}

	// Get current sub ID from subsByOriginalID map
	b.mu.RLock()
	subscription, ok := b.subsByOriginalID[subPkg.SubscriptionID(params[0])]
	b.mu.RUnlock()
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
	b.mu.Lock()
	b.pendingUnsubs[tempRelayID] = subPkg.PendingUnsubscribe{
		OriginalRelayID: relay.ID,
		OriginalSubID:   subscription.OriginalSubID(),
	}
	b.mu.Unlock()

	// Replace the original relay ID with the temporary one in the message
	relay.ID = relayPkg.IDFromString(tempRelayID)

	// Marshal the modified relay message
	modifiedMessage, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	return modifiedMessage, nil
}

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

	// If a response to a subscribe or unsubscribe request the ID will be a temp UUID
	if tempRelayID := relay.ID.String(); tempRelayID != "" {
		// If response is a subscription confirmation save the subscription to the subscriptions map
		b.mu.RLock()
		if pendingSub, ok := b.pendingSubs[tempRelayID]; ok {
			b.mu.RUnlock()
			return b.handleSubscribeResponse(relay, pendingSub, tempRelayID)
		}

		// If response is a unsubscription confirmation remove the subscription from the subscriptions map
		b.mu.RLock()
		if pendingUnsub, ok := b.pendingUnsubs[tempRelayID]; ok {
			b.mu.RUnlock()
			return b.handleUnsubscribeResponse(relay, pendingUnsub, tempRelayID)
		}

		// If response is a resubscribe confirmation update the current sub ID
		b.mu.RLock()
		if pendingResub, ok := b.pendingResubs[tempRelayID]; ok {
			b.mu.RUnlock()
			err := b.handleResubscribeResponse(relay, pendingResub, tempRelayID)
			if err != nil {
				return nil, fmt.Errorf("error handling resubscribe response: %w", err)
			}
		}
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
	b.mu.RLock()
	subscription, ok := b.subsByCurrentID[subPkg.SubscriptionID(params.Subscription)]
	b.mu.RUnlock()
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

	json, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("error marshalling subscription event params: %w", err)
	}

	return json, nil
}

func (b *Bridge) handleSubscribeResponse(relay relayPkg.Relay, pendingSub subPkg.PendingSubscribe, tempRelayID string) ([]byte, error) {
	b.log.Info("received eth_subscribe confirmation from gateway")

	if err := b.addSubscription(relay, pendingSub.RequestBody); err != nil {
		return nil, fmt.Errorf("error adding subscription: %w", err)
	}

	// Replace original relay ID before returning response to client
	relay.ID = pendingSub.OriginalRelayID

	msgWithOriginalID, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling subscription confirmation: %w", err)
	}

	// Clear pending sub from map
	b.mu.Lock()
	delete(b.pendingSubs, tempRelayID)
	b.mu.Unlock()

	return msgWithOriginalID, nil
}

func (b *Bridge) addSubscription(relay relayPkg.Relay, requestBody []byte) error {
	var subID subPkg.SubscriptionID
	if err := json.Unmarshal(relay.Result, &subID); err != nil {
		return fmt.Errorf("error unmarshalling subscription ID: %w", err)
	}

	subscription := subPkg.NewSubscription(subID, requestBody)

	b.mu.Lock()
	b.subsByCurrentID[subID] = subscription
	b.subsByOriginalID[subID] = subscription
	b.mu.Unlock()

	return nil
}

func (b *Bridge) handleUnsubscribeResponse(relay relayPkg.Relay, pendingUnsub subPkg.PendingUnsubscribe, tempRelayID string) ([]byte, error) {
	b.log.Info("received eth_unsubscribe confirmation from gateway")

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

	// Replace original relay ID before returning response to client
	relay.ID = pendingUnsub.OriginalRelayID

	msgWithOriginalID, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	// Clear pending unsub from map
	b.mu.Lock()
	delete(b.pendingUnsubs, tempRelayID)
	b.mu.Unlock()

	return msgWithOriginalID, nil
}

func (b *Bridge) removeSubscription(originalSubID subPkg.SubscriptionID) error {
	b.mu.RLock()
	subscription, ok := b.subsByOriginalID[originalSubID]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found for original sub ID: %s", originalSubID)
	}

	b.mu.Lock()
	delete(b.subsByOriginalID, originalSubID)
	delete(b.subsByCurrentID, subscription.CurrentSubID())
	b.mu.Unlock()

	return nil
}

func (b *Bridge) handleResubscribeResponse(relay relayPkg.Relay, originalSubID subPkg.SubscriptionID, tempRelayID string) error {
	b.log.Info("received eth_subscribe resubscribe confirmation from gateway")

	err := b.updateSubscription(relay, originalSubID)
	if err != nil {
		return fmt.Errorf("error handling resubscribe response: %w", err)
	}

	// Clear pending resub from map
	b.mu.Lock()
	delete(b.pendingResubs, tempRelayID)
	b.mu.Unlock()

	return nil
}

func (b *Bridge) updateSubscription(relay relayPkg.Relay, originalSubID subPkg.SubscriptionID) error {
	var newSubID subPkg.SubscriptionID
	if err := json.Unmarshal(relay.Result, &newSubID); err != nil {
		return fmt.Errorf("error unmarshalling subscription ID: %w", err)
	}

	b.mu.RLock()
	subscription, ok := b.subsByOriginalID[originalSubID]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("subscription not found for original sub ID: %s", originalSubID)
	}

	// Update current sub ID in current sub IDs map and remove original sub ID from map
	b.mu.Lock()
	subscription.SetCurrentSubID(newSubID)
	b.subsByCurrentID[newSubID] = subscription
	delete(b.subsByCurrentID, originalSubID)
	b.mu.Unlock()

	return nil
}
