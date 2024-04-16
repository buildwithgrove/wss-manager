package router

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pokt-foundation/utils-go/logger"
	"github.com/stretchr/testify/require"
)

func Test_websocketHandler(t *testing.T) {
	tests := []struct {
		name           string
		app            string
		expectedStatus int
	}{
		// {
		// 	name:           "should return 200 when app is not provided",
		// 	app:            "",
		// 	expectedStatus: http.StatusOK,
		// },
		{
			name:           "should return 200 when app is provided",
			app:            "1a2b3c4d",
			expectedStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := require.New(t)

			config := Config{
				Logger: logger.New(),
			}
			router := newAPIRouter(config)

			// Setup mux & handlers
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/{app}", router.websocketHandler)
			ts := httptest.NewServer(mux)
			defer ts.Close()

			// Create request
			req, err := http.NewRequest("GET", fmt.Sprintf("%s/v1/%s", ts.URL, test.app), nil)
			c.NoError(err)

			// Perform request
			client := &http.Client{}
			resp, err := client.Do(req)
			c.NoError(err)
			defer resp.Body.Close()

			// Test assertions
			c.Equal(test.expectedStatus, resp.StatusCode)
		})
	}
}
