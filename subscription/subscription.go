package subscription

import relayPkg "github.com/pokt-foundation/wss-manager/relay"

type (
	Subscription struct {
		originalSubID SubscriptionID
		currentSubID  SubscriptionID
		requestBody   []byte
	}

	PendingSubscribe struct {
		OriginalRelayID *relayPkg.ID
		RequestBody     []byte
	}

	PendingUnsubscribe struct {
		OriginalRelayID *relayPkg.ID
		OriginalSubID   SubscriptionID
	}

	SubscriptionID string
)

func NewSubscription(subID SubscriptionID, requestBody []byte) *Subscription {
	return &Subscription{
		originalSubID: subID,
		currentSubID:  subID,
		requestBody:   requestBody,
	}
}

func (s *Subscription) RequestBody() []byte {
	return s.requestBody
}

func (s *Subscription) OriginalSubID() SubscriptionID {
	return s.originalSubID
}

func (s *Subscription) CurrentSubID() SubscriptionID {
	return s.currentSubID
}

func (s *Subscription) SetCurrentSubID(id SubscriptionID) {
	s.currentSubID = id
}
