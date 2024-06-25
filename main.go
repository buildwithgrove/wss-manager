package main

import (
	"context"
	"fmt"

	"github.com/pokt-foundation/portal-http-db/v2/types"
	"github.com/pokt-foundation/utils-go/environment"
	"github.com/pokt-foundation/utils-go/logger"
	"github.com/pokt-foundation/wss-manager/metrics"
	"github.com/pokt-foundation/wss-manager/router"
)

const (
	// Required env variables
	gatewayDomain = "GATEWAY_DOMAIN"

	// Optional env variables
	maxReconnectionAttempts        = "MAX_RECONNECTION_ATTEMPTS"
	defaultMaxReconnectionAttempts = 100
	port                           = "PORT"
	defaultPort                    = "8100"
	tls                            = "TLS"
	defaultTLS                     = true
	imageTag                       = "IMAGE_TAG"
	defaultImageTag                = "development"

	// Metric namespace
	metricNamespace = "wss-manager"
)

type options struct {
	// Required env variables
	gatewayDomain string
	// Optional env variables
	maxReconnectionAttempts int
	port                    string
	tls                     bool
	imageTag                string
}

func gatherOptions() options {
	return options{
		// Required env variables
		gatewayDomain: environment.MustGetString(gatewayDomain),
		// Optional env variables
		maxReconnectionAttempts: int(environment.GetInt64(maxReconnectionAttempts, defaultMaxReconnectionAttempts)),
		port:                    environment.GetString(port, defaultPort),
		tls:                     environment.GetBool(tls, defaultTLS),
		imageTag:                environment.GetString(imageTag, defaultImageTag),
	}
}

func main() {
	options := gatherOptions()

	logger := logger.New()

	defer func() {
		if r := recover(); r != nil {
			logger.Error(fmt.Sprintf("application panicked: %v", r))
		}
	}()

	// Init metric exporter and register all metrics
	metricExporter := metrics.NewMetricExporter(metricNamespace)

	// eg. [https/ws]://eth-mainnet.rpc.grove.city/v1/1a2b3c4d
	gatewayURLFunc := func(scheme string, chain types.ChainAlias, path string) string {
		const gatewayURLTemplate = "%s://%s.%s%s"
		return fmt.Sprintf(gatewayURLTemplate, scheme, chain, options.gatewayDomain, path)
	}

	err := router.Start(context.Background(), router.Config{
		GatewayURLFunc:          gatewayURLFunc,
		MetricExporter:          metricExporter,
		MaxReconnectionAttempts: options.maxReconnectionAttempts,
		Port:                    options.port,
		TLS:                     options.tls,
		ImageTag:                options.imageTag,
		Logger:                  logger,
	})
	if err != nil {
		logger.Error(fmt.Sprintf("create API router failed with error: %s", err.Error()))
		panic(err)
	}
}
