package main

import (
	"context"
	"fmt"

	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/router"
)

const (
	port     = "PORT"
	imageTag = "IMAGE_TAG"

	defaultPort     = "8080"
	defaultImageTag = "development"
)

type options struct {
	port, imageTag string
}

func gatherOptions() options {
	return options{
		port:     environment.GetString(port, defaultPort),
		imageTag: environment.GetString(imageTag, defaultImageTag),
	}
}

func main() {
	options := gatherOptions()

	ctx := context.Background()

	logger := logger.New()

	// Initialize API Router
	err := router.Start(ctx, router.Config{
		Logger:   logger,
		ImageTag: options.imageTag,
	})
	if err != nil {
		logger.Error(fmt.Sprintf("create API router failed with error: %s", err.Error()))
		panic(err)
	}
}
