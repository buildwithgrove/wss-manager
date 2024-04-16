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
	relayPkg "github.com/pokt-foundation/wss-manager/relay"
)

// connectGateway connects to the gateway and returns the websocket connection.
// It also handles authentication if the gateway requires it.
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
		// Encoded := username + ":" + password
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

// reconnectToGateway reconnects to the gateway in case of connection drop with incremental backoff.
func (b *Bridge) reconnectToGateway() error {
	// TODO - configure in env vars
	maxAttempts := 10
	backoffInterval := 250 * time.Millisecond // Initial backoff interval
	const backoffFactor = 2                   // Factor by which the interval increases
	const maxBackoff = 10 * time.Second       // Maximum backoff interval

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
			<-time.After(backoffInterval)

			// Increase the backoff interval for the next attempt
			backoffInterval *= backoffFactor
			if backoffInterval > maxBackoff {
				backoffInterval = maxBackoff
			}

			continue
		}

		b.gatewayConn = gatewayWS
		b.log.Info("Successfully reconnected to gateway")

		b.log.Info("resuming subscriptions")
		b.resumeSubscriptions()

		return nil
	}

	return fmt.Errorf("failed to reconnect to gateway after %d attempts", maxAttempts)
}

func (b *Bridge) resumeSubscriptions() {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, sub := range b.subsByCurrentID {
		// Generate a temporary relay ID for the pending resubscribe request
		tempRelayID := uuid.New().String()

		// Store the original subscription ID with the temporary relay ID in the pending resubs map
		b.pendingResubs[tempRelayID] = sub.OriginalSubID()

		var relay relayPkg.Relay
		if err := json.Unmarshal(sub.RequestBody(), &relay); err != nil {
			b.log.Error("error unmarshalling client request:", slog.String("error", err.Error()))
			continue
		}

		relay.ID = relayPkg.IDFromString(tempRelayID)

		// Marshal the modified relay message
		modifiedMessage, err := json.Marshal(relay)
		if err != nil {
			b.log.Error("error marshalling subscribe relay:", slog.String("error", err.Error()))
			continue
		}

		err = b.gatewayConn.WriteMessage(websocket.TextMessage, modifiedMessage)
		if err != nil {
			b.log.Error("failed to resume subscription", slog.String("error", err.Error()))
			continue
		}

		b.log.Info("resumed subscription", slog.String("subscription", string(sub.OriginalSubID())))
	}
}
