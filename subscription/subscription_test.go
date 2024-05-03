package subscription

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SubscriptionMethods(t *testing.T) {
	tests := []struct {
		name              string
		subscription      *Subscription
		newCurrentSubID   SubscriptionID
		wantOriginalSubID SubscriptionID
		wantCurrentSubID  SubscriptionID
		wantRequestBody   []byte
	}{
		{
			name:              "should correctly return original subscription ID",
			subscription:      NewSubscription("sub-123", []byte(`{"jsonrpc":"2.0"}`)),
			wantOriginalSubID: "sub-123",
			wantCurrentSubID:  "sub-123",
			wantRequestBody:   []byte(`{"jsonrpc":"2.0"}`),
		},
		{
			name:              "should correctly update and return current subscription ID",
			subscription:      NewSubscription("sub-123", []byte(`{"jsonrpc":"2.0"}`)),
			newCurrentSubID:   "sub-456",
			wantOriginalSubID: "sub-123",
			wantCurrentSubID:  "sub-456",
			wantRequestBody:   []byte(`{"jsonrpc":"2.0"}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.newCurrentSubID != "" {
				test.subscription.SetCurrentSubID(test.newCurrentSubID)
			}

			assert.Equal(t, test.wantOriginalSubID, test.subscription.OriginalSubID())
			assert.Equal(t, test.wantCurrentSubID, test.subscription.CurrentSubID())
			assert.Equal(t, test.wantRequestBody, test.subscription.RequestBody())
		})
	}
}

func Test_Subscription_SetCurrentSubID(t *testing.T) {
	tests := []struct {
		name          string
		initialSubID  SubscriptionID
		newSubID      SubscriptionID
		expectedSubID SubscriptionID
	}{
		{
			name:          "should update the current subscription ID",
			initialSubID:  "initialID",
			newSubID:      "newID",
			expectedSubID: "newID",
		},
		{
			name:          "should update the current subscription ID to an empty string",
			initialSubID:  "initialID",
			newSubID:      "",
			expectedSubID: "",
		},
		{
			name:          "should keep the current subscription ID unchanged when new ID is the same",
			initialSubID:  "sameID",
			newSubID:      "sameID",
			expectedSubID: "sameID",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			subscription := NewSubscription(test.initialSubID, []byte("requestBody"))
			subscription.SetCurrentSubID(test.newSubID)
			assert.Equal(t, test.expectedSubID, subscription.CurrentSubID())
		})
	}
}
