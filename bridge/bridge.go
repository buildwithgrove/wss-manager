package bridge

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"
	sub "github.com/pokt-foundation/wss-manager/subscription"
)

const (
	ethSubscribe   = "eth_subscribe"
	ethUnsubscribe = "eth_unsubscribe"
)

type (
	// Bridge routes data between clients and servers
	Bridge struct {
		id    string
		app   types.PortalAppID
		chain types.ChainAlias

		clientConn  wsConnection
		gatewayConn wsConnection

		log *logger.Logger

		subsByCurrentID  map[sub.SubscriptionID]*sub.Subscription
		subsByOriginalID map[sub.SubscriptionID]*sub.Subscription

		// TODO - clear pending subs on interval?
		pendingSubs   map[string]sub.PendingSubscribe
		pendingUnsubs map[string]sub.PendingUnsubscribe
		req           *http.Request

		// TODO - add subscription map for the bridge
	}

	Builder struct {
		log *logger.Logger
	}

	wsConnection interface {
		ReadMessage() (messageType int, p []byte, err error)
		WriteMessage(messageType int, data []byte) error
		Close() error
	}

	// TODO - move to own package
	// TODO - define custom Relay types for subscription bodies, requests and confirmations
	Relay struct {
		// TODO - define ID type to avoid usage of interface{}
		ID      interface{}     `json:"id"`
		JSONRPC string          `json:"jsonrpc"`
		Method  string          `json:"method,omitempty"`
		Result  json.RawMessage `json:"result,omitempty"`
		Params  json.RawMessage `json:"params,omitempty"`
	}
)

func NewBuilder(log *logger.Logger) *Builder {
	return &Builder{
		log: log,
	}
}

func (b *Builder) NewBridge(app types.PortalAppID, chain types.ChainAlias, clientConn, gatewayConn wsConnection, req *http.Request) *Bridge {
	return &Bridge{
		id:               uuid.New().String(),
		app:              app,
		chain:            chain,
		clientConn:       clientConn,
		gatewayConn:      gatewayConn,
		req:              req,
		log:              b.log,
		pendingSubs:      make(map[string]sub.PendingSubscribe),
		pendingUnsubs:    make(map[string]sub.PendingUnsubscribe),
		subsByCurrentID:  make(map[sub.SubscriptionID]*sub.Subscription),
		subsByOriginalID: make(map[sub.SubscriptionID]*sub.Subscription),
	}
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

					// Close the gateway connection when client connection closed
					// TODO - figure out how to gracefully stop the gateway read loop on closing the connection.
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

	// Start goroutine to read from gateway and write to client
	go func() {
		for {
			messageType, message, err := b.gatewayConn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					// TODO - implement Gateway reconnection logic
					b.log.Info("gateway websocket closed")
					return
				}

				b.log.Error("error reading from gateway websocket:", slog.String("error", err.Error()))
				return
			}

			// Check if the message is a response to a pending subscribe or unsubscribe request
			processedMsg, err := b.processGatewayResponse(message)
			if err != nil {
				b.log.Error("error processing gateway request:", slog.String("error", err.Error()))
				continue
			}

			err = b.clientConn.WriteMessage(messageType, processedMsg)
			if err != nil {
				b.log.Error("error writing to client websocket:", slog.String("error", err.Error()))
				return
			}
		}
	}()
}

// Private methods

func (b *Bridge) addSubscription(relay Relay, requestBody []byte) error {
	var subID sub.SubscriptionID
	if err := json.Unmarshal(relay.Result, &subID); err != nil {
		return fmt.Errorf("error unmarshalling subscription ID: %w", err)
	}

	subscription := sub.NewSubscription(subID, requestBody)

	b.subsByCurrentID[subID] = subscription
	b.subsByOriginalID[subID] = subscription

	return nil
}

func (b *Bridge) removeSubscription(originalSubID sub.SubscriptionID) {
	if subscription, ok := b.subsByOriginalID[originalSubID]; ok {
		delete(b.subsByCurrentID, subscription.CurrentSubID())
	}

	delete(b.subsByOriginalID, originalSubID)
}

// processClientRequest checks if a client request is either an 'eth_subscribe' or 'eth_unsubscribe' method.
func (b *Bridge) processClientRequest(message []byte) ([]byte, error) {
	var relay Relay
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

func (b *Bridge) handleSubscribeRequest(relay Relay, requestBody []byte) ([]byte, error) {
	b.log.Info("Received eth_subscribe request from client")

	// Generate a temporary relay ID for the pending subscription
	tempRelayID := uuid.New().String()

	// Store the pending subscription with the temporary relay ID
	b.pendingSubs[tempRelayID] = sub.PendingSubscribe{
		OriginalRelayID: relay.ID,
		RequestBody:     requestBody,
	}

	// Replace the original relay ID with the temporary one in the message
	relay.ID = tempRelayID

	// Marshal the modified relay message
	modifiedMessage, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling subscribe relay: %w", err)
	}

	return modifiedMessage, nil
}

func (b *Bridge) handleUnsubscribeRequest(relay Relay) ([]byte, error) {
	b.log.Info("Received eth_unsubscribe request from client")

	// Generate a temporary relay ID for the pending subscription
	tempRelayID := uuid.New().String()

	var params []string
	if err := json.Unmarshal(relay.Params, &params); err != nil {
		return nil, fmt.Errorf("error unmarshalling unsubscribe request: %w", err)
	}

	// Store the pending subscription with the temporary relay ID
	b.pendingUnsubs[tempRelayID] = sub.PendingUnsubscribe{
		OriginalRelayID: relay.ID,
		OriginalSubID:   sub.SubscriptionID(params[0]),
	}

	// Replace the original relay ID with the temporary one in the message
	relay.ID = tempRelayID

	// Marshal the modified relay message
	modifiedMessage, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	return modifiedMessage, nil
}

// processGatewayResponse checks if a gateway response is either an 'eth_subscribe' or 'eth_unsubscribe' method.
func (b *Bridge) processGatewayResponse(message []byte) ([]byte, error) {
	var relay Relay
	if err := json.Unmarshal(message, &relay); err != nil {
		return nil, fmt.Errorf("error unmarshalling gateway response: %w", err)
	}

	// TODO - check if response is a subscription event. If it is then replace the current
	// subscription ID with the original subscription ID before returning the response to the client

	// If the relay is a response to a subscription or unsubscription its ID is a UUID
	if tempRelayID, ok := relay.ID.(string); ok {

		// If response is a subscription confirmation save the subscription to the subscriptions map
		if pendingSub, ok := b.pendingSubs[tempRelayID]; ok {
			return b.handleSubscribeResponse(relay, pendingSub, tempRelayID)
		}

		// If response is a unsubscription confirmation remove the subscription from the subscriptions map
		if pendingUnsub, ok := b.pendingUnsubs[tempRelayID]; ok {
			return b.handleUnsubscribeResponse(relay, pendingUnsub, tempRelayID)
		}
	}

	// If neither return the unmodified message
	return message, nil
}

func (b *Bridge) handleSubscribeResponse(relay Relay, pendingSub sub.PendingSubscribe, tempRelayID string) ([]byte, error) {
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
	delete(b.pendingSubs, tempRelayID)

	return msgWithOriginalID, nil
}

func (b *Bridge) handleUnsubscribeResponse(relay Relay, pendingUnsub sub.PendingUnsubscribe, tempRelayID string) ([]byte, error) {
	b.log.Info("received eth_unsubscribe confirmation from gateway")

	// Get current sub ID from subsByOriginalID map
	subscription, ok := b.subsByOriginalID[pendingUnsub.OriginalSubID]
	if !ok {
		return nil, fmt.Errorf("subscription not found")
	}

	// If the unsubscribe was successful the result field will be 'true'
	var unsubConfirmation bool
	if err := json.Unmarshal(relay.Result, &unsubConfirmation); err != nil {
		return nil, fmt.Errorf("error unmarshalling unsubscribe confirmation: %w", err)
	}
	if unsubConfirmation {
		b.removeSubscription(pendingUnsub.OriginalSubID)
	}

	// Replace original relay ID before returning response to client
	relay.ID = pendingUnsub.OriginalRelayID

	// Replace params slice with current sub ID
	currentSubID := []sub.SubscriptionID{subscription.CurrentSubID()}
	paramsJSON, err := json.Marshal(currentSubID)
	if err != nil {
		return nil, err
	}
	relay.Params = json.RawMessage(paramsJSON)

	msgWithOriginalID, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	// Clear pending unsub from map
	delete(b.pendingUnsubs, tempRelayID)

	return msgWithOriginalID, nil
}
