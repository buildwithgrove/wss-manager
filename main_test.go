package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_gatherOptions(t *testing.T) {
	tests := []struct {
		name     string
		envVars  map[string]string
		expected options
	}{
		{
			name: "should gather all options from environment variables",
			envVars: map[string]string{
				gatewayDomain:           "example.com",
				maxReconnectionAttempts: "5",
				port:                    "8080",
				tls:                     "true",
				imageTag:                "v1.0.0",
			},
			expected: options{
				gatewayDomain:           "example.com",
				maxReconnectionAttempts: 5,
				port:                    "8080",
				tls:                     true,
				imageTag:                "v1.0.0",
			},
		},
		{
			name: "should use default values for optional environment variables",
			envVars: map[string]string{
				gatewayDomain: "example.com",
			},
			expected: options{
				gatewayDomain:           "example.com",
				maxReconnectionAttempts: defaultMaxReconnectionAttempts,
				port:                    defaultPort,
				tls:                     defaultTLS,
				imageTag:                defaultImageTag,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range test.envVars {
				os.Setenv(key, value)
			}

			result := gatherOptions()
			assert.Equal(t, test.expected, result)

			for key := range test.envVars {
				os.Unsetenv(key)
			}
		})
	}
}
