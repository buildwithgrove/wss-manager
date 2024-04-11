package main

import (
	"context"
	"fmt"

	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/router"
)

const (
	// TODO - this will change when routing requests to the gateway
	gatewayDomain = "GATEWAY_DOMAIN"
	port          = "PORT"
	imageTag      = "IMAGE_TAG"

	defaultPort     = "8080"
	defaultImageTag = "development"
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

	err := router.Start(context.Background(), router.Config{
		GatewayDomain: options.gatewayDomain,
		Port:          options.port,
		ImageTag:      options.imageTag,
		Logger:        logger,
	})
	if err != nil {
		logger.Error(fmt.Sprintf("create API router failed with error: %s", err.Error()))
		panic(err)
	}
}
