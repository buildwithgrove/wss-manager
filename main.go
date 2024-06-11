package main

import (
	"context"
	"fmt"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/router"
)

const (
	// Required env variables
	gatewayDomain = "GATEWAY_DOMAIN"
	wsAuthKey     = "WS_AUTH_KEY"

	// Optional env variables
	maxReconnectionAttempts        = "MAX_RECONNECTION_ATTEMPTS"
	defaultMaxReconnectionAttempts = 100
	useWSS                         = "USE_WSS"
	defaultUseWSS                  = false
	port                           = "PORT"
	defaultPort                    = "8100"
	imageTag                       = "IMAGE_TAG"
	defaultImageTag                = "development"

	// eg. wss://eth-mainnet.rpc.grove.city/v1/1a2b3c4d
	gatewayURLTemplate = "%s://%s.%s/v1/%s"
)

type options struct {
	// Required env variables
	gatewayDomain string
	wsAuthKey     string
	// Optional env variables
	maxReconnectionAttempts int
	useWSS                  bool
	port                    string
	imageTag                string
}

func gatherOptions() options {
	return options{
		// Required env variables
		gatewayDomain: environment.MustGetString(gatewayDomain),
		wsAuthKey:     environment.MustGetString(wsAuthKey),
		// Optional env variables
		maxReconnectionAttempts: int(environment.GetInt64(maxReconnectionAttempts, defaultMaxReconnectionAttempts)),
		useWSS:                  environment.GetBool(useWSS, defaultUseWSS),
		port:                    environment.GetString(port, defaultPort),
		imageTag:                environment.GetString(imageTag, defaultImageTag),
	}
}

func main() {
	options := gatherOptions()

	logger := logger.New()

	gatewayURLFunc := func(chain types.ChainAlias, appID types.PortalAppID) string {
		scheme := "ws"
		if options.useWSS {
			scheme = "wss"
		}
		return fmt.Sprintf(gatewayURLTemplate, scheme, chain, options.gatewayDomain, appID)
	}

	err := router.Start(context.Background(), router.Config{
		GatewayURLFunc:          gatewayURLFunc,
		MaxReconnectionAttempts: options.maxReconnectionAttempts,
		Port:                    options.port,
		WSAuthKey:               options.wsAuthKey,
		ImageTag:                options.imageTag,
		Logger:                  logger,
	})
	if err != nil {
		logger.Error(fmt.Sprintf("create API router failed with error: %s", err.Error()))
		panic(err)
	}
}
