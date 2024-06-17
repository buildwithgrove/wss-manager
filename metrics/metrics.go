package metrics

import "github.com/pokt-foundation/portal-middleware/metrics/exporter"

const (
	LabelAttempt = "attempt"
	LabelSuccess = "success"
	LabelError   = "error"

	// Router metrics
	CategoryRelay = "relay"

	NameHTTPRelay = "http-relay"
	NameWSRelay   = "ws-relay"

	// Bridge metrics
	CategoryBridge = "bridge"

	NameClientRelay  = "client-relay"
	NameGatewayRelay = "gateway-relay"
	NameReconnect    = "reconnect"
	NameSubscribe    = "subscribe"

	LabelErrorWrite   = "write_error"
	LabelErrorRead    = "read_error"
	LabelErrorMarshal = "marshal_error"
	LabelErrorProcess = "process_error"

	LabelSubscriptionAdd    = "subscription_add"
	LabelSubscriptionRemove = "subscription_remove"
)

var (
	// Router Labels
	LabelsRelay = []string{"relay"}

	// Bridge Labels
	LabelsWSRelay       = []string{"ws-relay"}
	LabelsReconnection  = []string{"reconnection"}
	LabelsSubscriptions = []string{"subscription"}
)

// NewMetricExporter registers all metrics to the metrics exporter
func NewMetricExporter(namespace string) exporter.MetricExporter {
	metricsExporter := exporter.NewMetricExporter(namespace)

	// Router metrics
	_ = metricsExporter.NewCounter(CategoryRelay, NameHTTPRelay, LabelsRelay, "HTTP Relays")
	_ = metricsExporter.NewCounter(CategoryRelay, NameWSRelay, LabelsRelay, "WebSocket Relays")

	// Bridge metrics
	_ = metricsExporter.NewCounter(CategoryBridge, NameClientRelay, LabelsWSRelay, "WebSocket Client Relays")
	_ = metricsExporter.NewCounter(CategoryBridge, NameGatewayRelay, LabelsWSRelay, "WebSocket Gateway Relays")
	_ = metricsExporter.NewCounter(CategoryBridge, NameReconnect, LabelsReconnection, "WebSocket Gateway Reconnects")
	_ = metricsExporter.NewGauge(CategoryBridge, NameSubscribe, LabelsSubscriptions, "WebSocket Subscriptions")

	return metricsExporter
}
