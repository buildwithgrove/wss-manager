package bridge

import (
	"github.com/gorilla/websocket"
	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/logger"
)

type (
	// Bridge routes data between clients and servers
	Bridge struct {
		client *websocket.Conn
		server *websocket.Conn

		app   types.PortalAppID
		chain types.ChainAlias

		log *logger.Logger
	}

	Builder struct {
		log *logger.Logger
	}
)

func NewBuilder(log *logger.Logger) *Builder {
	return &Builder{
		log: log,
	}
}

func (b *Builder) NewBridge(app types.PortalAppID, chain types.ChainAlias, client, server *websocket.Conn) *Bridge {
	bridge := &Bridge{
		app:    app,
		chain:  chain,
		client: client,
		server: server,
		log:    b.log,
	}

	return bridge
}

func (b *Bridge) Run() {
	go func() {
		for {
			messageType, message, err := b.client.ReadMessage()
			if err != nil {
				b.log.Error("Error reading from client websocket:", err)
				return
			}

			err = b.server.WriteMessage(messageType, message)
			if err != nil {
				b.log.Error("Error writing to server websocket:", err)
				return
			}
		}
	}()
}
