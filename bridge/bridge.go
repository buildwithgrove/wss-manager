package bridge

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"
)

type (
	// Bridge routes data between clients and servers
	Bridge struct {
		id    uuid.UUID
		app   types.PortalAppID
		chain types.ChainAlias

		clientConn  wsConnection
		gatewayConn wsConnection

		log *logger.Logger

		req http.Request

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

func (b *Builder) NewBridge(app types.PortalAppID, chain types.ChainAlias, clientConn, gatewayConn wsConnection, req http.Request) *Bridge {
	return &Bridge{
		id:          uuid.New(),
		app:         app,
		chain:       chain,
		clientConn:  clientConn,
		gatewayConn: gatewayConn,
		req:         req,
		log:         b.log,
	}
}

// Run starts the bridge and establishes a bidirectional communication between the client and server
func (b *Bridge) Run() {
	// Start goroutine to read from client and write to gateway
	go func() {
		for {
			messageType, message, err := b.clientConn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					b.log.Info("Client websocket closed")

					// Close the gateway connection when client connection terminated
					// TODO - figure out how to gracefully exit the gateway read loop to avoid error in read loop
					b.gatewayConn.Close()
					return
				}
				b.log.Error("Error reading from client websocket:", err)
				return
			}

			// TODO - intercept `eth_subscription` messages and set the custom ID on them

			err = b.gatewayConn.WriteMessage(messageType, message)
			if err != nil {
				b.log.Error("Error writing to gateway websocket:", err)
				return
			}
		}
	}()

	// Start goroutine to read from gateway and write to client
	go func() {
		for {
			messageType, message, err := b.gatewayConn.ReadMessage()
			if err != nil {
				b.log.Error("Error reading from gateway websocket:", err)
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					// TODO - implement Gateway reconnection logic
					b.log.Info("Gateway websocket closed")
				}
				return
			}

			// TODO - intercept `eth_subscription` message replies, match the ID from the send step and add to the subscriptions map for this bridge

			err = b.clientConn.WriteMessage(messageType, message)
			if err != nil {
				b.log.Error("Error writing to client websocket:", err)
				return
			}
		}
	}()
}

func (b *Bridge) Chain() types.ChainAlias {
	return b.chain
}

func (b *Bridge) App() types.PortalAppID {
	return b.app
}
