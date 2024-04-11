package manager

// func TestManager_AddConnection(t *testing.T) {
// 	tests := []struct {
// 		name           string
// 		subscriptionID SubscriptionID
// 		conn           *websocket.Conn
// 		wantLen        int
// 	}{
// 		{
// 			name:           "should add a new connection",
// 			subscriptionID: "sub1",
// 			conn:           &websocket.Conn{},
// 			wantLen:        1,
// 		},
// 		{
// 			name:           "should overwrite existing connection",
// 			subscriptionID: "sub1",
// 			conn:           &websocket.Conn{},
// 			wantLen:        1,
// 		},
// 		{
// 			name:           "should add multiple connections",
// 			subscriptionID: "sub2",
// 			conn:           &websocket.Conn{},
// 			wantLen:        2,
// 		},
// 	}

// 	m := NewManager()
// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			m.AddConnection(test.subscriptionID, test.conn)

// 			assert.Equal(t, test.wantLen, len(m.clientConnections))
// 		})
// 	}
// }

// func TestManager_RemoveConnection(t *testing.T) {
// 	tests := []struct {
// 		name           string
// 		subscriptionID SubscriptionID
// 		setup          func(m *Manager)
// 		wantLen        int
// 	}{
// 		{
// 			name:           "should remove an existing connection",
// 			subscriptionID: "sub1",
// 			setup: func(m *Manager) {
// 				m.AddConnection("sub1", &websocket.Conn{})
// 			},
// 			wantLen: 0,
// 		},
// 		{
// 			name:           "should do nothing when removing a non-existing connection",
// 			subscriptionID: "sub2",
// 			setup: func(m *Manager) {
// 				m.AddConnection("sub1", &websocket.Conn{})
// 			},
// 			wantLen: 1,
// 		},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			m := NewManager()
// 			test.setup(m)

// 			m.RemoveConnection(test.subscriptionID)

// 			assert.Equal(t, test.wantLen, len(m.clientConnections))
// 		})
// 	}
// }

// func TestManager_GetConnection(t *testing.T) {
// 	tests := []struct {
// 		name           string
// 		subscriptionID SubscriptionID
// 		setup          func(m *Manager)
// 		wantConn       *websocket.Conn
// 		wantOk         bool
// 	}{
// 		{
// 			name:           "should retrieve an existing connection",
// 			subscriptionID: "sub1",
// 			setup: func(m *Manager) {
// 				m.AddConnection("sub1", &websocket.Conn{})
// 			},
// 			wantConn: &websocket.Conn{},
// 			wantOk:   true,
// 		},
// 		{
// 			name:           "should return false for non-existing connection",
// 			subscriptionID: "sub2",
// 			setup:          func(m *Manager) {},
// 			wantConn:       nil,
// 			wantOk:         false,
// 		},
// 	}

// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			m := NewManager()
// 			test.setup(m)

// 			conn, ok := m.GetConnection(test.subscriptionID)

// 			assert.Equal(t, test.wantOk, ok)
// 			if test.wantOk {
// 				assert.NotNil(t, conn)
// 			} else {
// 				assert.Nil(t, conn)
// 			}
// 		})
// 	}
// }
