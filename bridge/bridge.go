package bridge

import (
	"github.com/google/uuid"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"
)

type (
	// Bridge routes data between clients and servers
	Bridge struct {
		id    uuid.UUID
		app   types.PortalAppID
		chain types.ChainAlias

		clientConn  WSConnection
		gatewayConn WSConnection

		log *logger.Logger

		// TODO - add subscription map for the bridge
	}

	IBridge interface {
		Run()
		Chain() types.ChainAlias
		App() types.PortalAppID
	}

	Builder struct {
		log *logger.Logger
	}

	WSConnection interface {
		ReadMessage() (messageType int, p []byte, err error)
		WriteMessage(messageType int, data []byte) error
	}
)

func NewBuilder(log *logger.Logger) *Builder {
	return &Builder{
		log: log,
	}
}

func (b *Builder) NewBridge(app types.PortalAppID, chain types.ChainAlias, clientConn, gatewayConn WSConnection) IBridge {
	return &Bridge{
		id:          uuid.New(),
		app:         app,
		chain:       chain,
		clientConn:  clientConn,
		gatewayConn: gatewayConn,
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
				b.log.Error("Error reading from client websocket:", err)
			}

			// TODO - intercept `eth_subscription` messages and set the custom ID on them

			err = b.gatewayConn.WriteMessage(messageType, message)
			if err != nil {
				b.log.Error("Error writing to gateway websocket:", err)
			}
		}
	}()

	// Start goroutine to read from gateway and write to client
	go func() {
		for {
			messageType, message, err := b.gatewayConn.ReadMessage()
			if err != nil {
				b.log.Error("Error reading from gateway websocket:", err)
			}

			// TODO - intercept `eth_subscription` message replies, match the ID from the send step and add to the subscriptions map for this bridge

			err = b.clientConn.WriteMessage(messageType, message)
			if err != nil {
				b.log.Error("Error writing to client websocket:", err)
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
