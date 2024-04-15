package bridge

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/portal-http-db/v2/types"
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
		id    string
		app   types.PortalAppID
		chain types.ChainAlias

		clientConn  wsConnection
		gatewayConn wsConnection
		gatewayURL  string

		log *logger.Logger

		subsByCurrentID  map[subPkg.SubscriptionID]*subPkg.Subscription
		subsByOriginalID map[subPkg.SubscriptionID]*subPkg.Subscription

		// TODO - clear pending subs on interval?
		pendingSubs   map[string]subPkg.PendingSubscribe
		pendingUnsubs map[string]subPkg.PendingUnsubscribe
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
)

func NewBuilder(log *logger.Logger) *Builder {
	return &Builder{
		log: log,
	}
}

func (bb *Builder) NewBridge(app types.PortalAppID, chain types.ChainAlias, clientConn wsConnection, gatewayURL string, req *http.Request) (*Bridge, error) {
	bridge := &Bridge{
		id:               uuid.New().String(),
		app:              app,
		chain:            chain,
		clientConn:       clientConn,
		gatewayURL:       gatewayURL,
		req:              req,
		log:              bb.log,
		pendingSubs:      make(map[string]subPkg.PendingSubscribe),
		pendingUnsubs:    make(map[string]subPkg.PendingUnsubscribe),
		subsByCurrentID:  make(map[subPkg.SubscriptionID]*subPkg.Subscription),
		subsByOriginalID: make(map[subPkg.SubscriptionID]*subPkg.Subscription),
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
				// , websocket.CloseAbnormalClosure
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {

					// if the gateway connection is closed unexpectedly, attempt to reconnect
					b.log.Info("gateway websocket closed unexpectedly, attempting to reconnect...")
					if reconnectErr := b.reconnectToGateway(); reconnectErr != nil {
						b.log.Error("failed to reconnect to gateway, stopping bridge operation", slog.String("error", reconnectErr.Error()))
						return
					}

					// resume loop if reconnection was successful
					continue
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

func (b *Bridge) addSubscription(relay relayPkg.Relay, requestBody []byte) error {
	var subID subPkg.SubscriptionID
	if err := json.Unmarshal(relay.Result, &subID); err != nil {
		return fmt.Errorf("error unmarshalling subscription ID: %w", err)
	}

	subscription := subPkg.NewSubscription(subID, requestBody)

	b.subsByCurrentID[subID] = subscription
	b.subsByOriginalID[subID] = subscription

	return nil
}

func (b *Bridge) removeSubscription(originalSubID subPkg.SubscriptionID) {
	if subscription, ok := b.subsByOriginalID[originalSubID]; ok {
		delete(b.subsByCurrentID, subscription.CurrentSubID())
	}

	delete(b.subsByOriginalID, originalSubID)
}

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
	b.pendingSubs[tempRelayID] = subPkg.PendingSubscribe{
		OriginalRelayID: relay.ID,
		RequestBody:     requestBody,
	}

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
	subscription, ok := b.subsByOriginalID[subPkg.SubscriptionID(params[0])]
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
	b.pendingUnsubs[tempRelayID] = subPkg.PendingUnsubscribe{
		OriginalRelayID: relay.ID,
		OriginalSubID:   subscription.OriginalSubID(),
	}

	// Replace the original relay ID with the temporary one in the message
	relay.ID = relayPkg.IDFromString(tempRelayID)

	// Marshal the modified relay message
	modifiedMessage, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	return modifiedMessage, nil
}

// processGatewayResponse checks if a gateway response is either an 'eth_subscribe' or 'eth_unsubscribe' method.
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
		if pendingSub, ok := b.pendingSubs[tempRelayID]; ok {
			return b.handleSubscribeResponse(relay, pendingSub, tempRelayID)
		}

		// If response is a unsubscription confirmation remove the subscription from the subscriptions map
		if pendingUnsub, ok := b.pendingUnsubs[tempRelayID]; ok {
			return b.handleUnsubscribeResponse(relay, pendingUnsub, tempRelayID)
		}
	}

	// If none of the above return the unmodified message
	return message, nil
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
	delete(b.pendingSubs, tempRelayID)

	return msgWithOriginalID, nil
}

func (b *Bridge) handleUnsubscribeResponse(relay relayPkg.Relay, pendingUnsub subPkg.PendingUnsubscribe, tempRelayID string) ([]byte, error) {
	b.log.Info("received eth_unsubscribe confirmation from gateway")

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

	msgWithOriginalID, err := json.Marshal(relay)
	if err != nil {
		return nil, fmt.Errorf("error marshalling unsubscribe relay: %w", err)
	}

	// Clear pending unsub from map
	delete(b.pendingUnsubs, tempRelayID)

	return msgWithOriginalID, nil
}

func (b *Bridge) isSubscriptionEvent(relay relayPkg.Relay) (relayPkg.SubscriptionEventParams, bool) {
	var params relayPkg.SubscriptionEventParams
	if err := json.Unmarshal(relay.Params, &params); err == nil {
		return params, true
	}

	return params, false
}

func (b *Bridge) handleSubscriptionEvent(params relayPkg.SubscriptionEventParams, relay relayPkg.Relay) ([]byte, error) {
	subscription, ok := b.subsByCurrentID[subPkg.SubscriptionID(params.Subscription)]
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

/* Reconnection Logic */

func (b *Bridge) connectGateway() (*websocket.Conn, error) {
	u, err := url.Parse(b.gatewayURL)
	if err != nil {
		return nil, err
	}

	var h http.Header
	username := u.User.Username()
	password, _ := u.User.Password()

	if username != "" && password != "" {
		encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		// encoded := username + ":" + password
		h = http.Header{
			"Authorization": []string{"Basic " + encoded},
		}
	}

	// Remove authentication information from URL
	u.User = nil

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), h)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// Reconnects to the gateway in case of connection drop with incremental backoff.
func (b *Bridge) reconnectToGateway() error {
	// TODO - configure in env vars
	maxAttempts := 5
	backoffInterval := 1 * time.Second // Initial backoff interval
	const backoffFactor = 2            // Factor by which the interval increases
	const maxBackoff = 1 * time.Minute // Maximum backoff interval

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		b.log.Info("attempting to reconnect to gateway", slog.Int("attempt", attempt))
		gatewayWS, err := b.connectGateway()
		if err != nil {
			b.log.Error("failed to reconnect to gateway", slog.String("error", err.Error()), slog.Int("attempt", attempt))

			if attempt == maxAttempts {
				b.log.Error("max reconnect attempts reached", slog.Int("maxAttempts", maxAttempts))
				return err
			}

			b.log.Info("retrying to connect after backoff interval")
			time.Sleep(backoffInterval)

			// Increase the backoff interval for the next attempt
			backoffInterval *= backoffFactor
			if backoffInterval > maxBackoff {
				backoffInterval = maxBackoff
			}

			continue
		}

		b.gatewayConn = gatewayWS
		b.log.Info("Successfully reconnected to gateway")
		return nil
	}

	return fmt.Errorf("failed to reconnect to gateway after %d attempts", maxAttempts)
}
