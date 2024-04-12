package subscription

type (
	Subscription struct {
		requestBody []byte

		originalSubID SubscriptionID
		currentSubID  SubscriptionID
	}

	PendingSubscribe struct {
		// TODO - define ID type to avoid usage of interface{}
		OriginalRelayID interface{}
		RequestBody     []byte
	}

	PendingUnsubscribe struct {
		// TODO - define ID type to avoid usage of interface{}
		OriginalRelayID interface{}
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
