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
	gatewayDomain = "GATEWAY_DOMAIN"
	port          = "PORT"
	imageTag      = "IMAGE_TAG"

	defaultPort     = "8080"
	defaultImageTag = "development"

	// eg. wss://eth-mainnet.rpc.grove.city/v1/1a2b3c4d
	// TODO - update gateway URL to use wss:// ?
	gatewayURLTemplate = "ws://%s.%s/v1/%s"
)

type options struct {
	gatewayDomain string
	port          string
	imageTag      string
}

func gatherOptions() options {
	return options{
		gatewayDomain: environment.MustGetString(gatewayDomain),
		port:          environment.GetString(port, defaultPort),
		imageTag:      environment.GetString(imageTag, defaultImageTag),
	}
}

func main() {
	options := gatherOptions()

	logger := logger.New()

	gatewayURLFunc := func(chain types.ChainAlias, appID types.PortalAppID) string {
		return fmt.Sprintf(gatewayURLTemplate, chain, options.gatewayDomain, appID)
	}

	err := router.Start(context.Background(), router.Config{
		GatewayURLFunc: gatewayURLFunc,
		Port:           options.port,
		ImageTag:       options.imageTag,
		Logger:         logger,
	})
	if err != nil {
		logger.Error(fmt.Sprintf("create API router failed with error: %s", err.Error()))
		panic(err)
	}
}
