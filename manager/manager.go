package manager

// // Manager maintains a pool of websocket connections indexed by a subscription ID.
// type (
// 	Manager struct {
// 		clientConnections map[SubscriptionID]ClientConnection
// 		lock              sync.RWMutex
// 	}

// 	ClientConnection struct {
// 		portalAppID types.PortalAppID
// 		conn        *websocket.Conn
// 	}

// 	SubscriptionID string
// )

// // NewManager creates a new instance of Manager.
// func NewManager() *Manager {
// 	return &Manager{
// 		clientConnections: make(map[SubscriptionID]ClientConnection),
// 	}
// }

// // AddConnection adds a new websocket connection to the pool.
// func (m *Manager) AddConnection(subscriptionID SubscriptionID, conn *websocket.Conn) {
// 	m.lock.Lock()
// 	defer m.lock.Unlock()

// 	m.clientConnections[subscriptionID] = conn
// }

// // RemoveConnection removes a websocket connection from the pool.
// func (m *Manager) RemoveConnection(subscriptionID SubscriptionID) {
// 	m.lock.Lock()
// 	defer m.lock.Unlock()

// 	delete(m.clientConnections, subscriptionID)
// }

// // GetConnection retrieves a websocket connection from the pool by subscription ID.
// func (m *Manager) GetConnection(subscriptionID SubscriptionID) (*websocket.Conn, bool) {
// 	m.lock.RLock()
// 	defer m.lock.RUnlock()

// 	conn, ok := m.clientConnections[subscriptionID]

// 	return conn, ok
// }
