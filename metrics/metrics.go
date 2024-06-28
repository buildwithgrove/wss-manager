package metrics

import "github.com/pokt-foundation/portal-middleware/metrics/exporter"

const (
	NamePanic = "panic_recovered"

	LabelAttempt = "attempt"
	LabelSuccess = "success"
	LabelError   = "error"

	// Router metrics
	CategoryRouter = "router"

	NameHTTPRelay = "http_relay"
	NameWSRelay   = "ws_relay"

	// Bridge metrics
	CategoryBridge = "bridge"

	NameClientRelay  = "client_relay"
	NameGatewayRelay = "gateway_relay"
	NameReconnect    = "reconnect"
	NameSubscribe    = "subscribe"
	NameResubscribe  = "resubscribe"

	LabelErrorWrite   = "write_error"
	LabelErrorRead    = "read_error"
	LabelErrorMarshal = "marshal_error"
	LabelErrorProcess = "process_error"

	LabelActiveSubscriptions = "active_subscriptions"
)

var (
	LabelsRecovered = []string{"outcome", "error"}

	// Router Labels
	LabelsRelay = []string{"outcome", "chain_alias", "error"}

	// Bridge Labels
	LabelsWSRelay       = []string{"outcome", "error"}
	LabelsReconnection  = []string{"outcome", "error"}
	LabelsSubscriptions = []string{"outcome", "sub_id", "error"}
)

// NewMetricExporter registers all metrics to the metrics exporter
func NewMetricExporter(namespace string) exporter.MetricExporter {
	metricsExporter := exporter.NewMetricExporter(namespace)

	// Router metrics
	_ = metricsExporter.NewCounter(CategoryRouter, NamePanic, LabelsRecovered, "Panic Recovered")
	_ = metricsExporter.NewCounter(CategoryRouter, NameHTTPRelay, LabelsRelay, "HTTP Relays")
	_ = metricsExporter.NewCounter(CategoryRouter, NameWSRelay, LabelsRelay, "WebSocket Relays")

	// Bridge metrics
	_ = metricsExporter.NewCounter(CategoryBridge, NamePanic, LabelsRecovered, "Panic Recovered")
	_ = metricsExporter.NewCounter(CategoryBridge, NameClientRelay, LabelsWSRelay, "WebSocket Client Relays")
	_ = metricsExporter.NewCounter(CategoryBridge, NameGatewayRelay, LabelsWSRelay, "WebSocket Gateway Relays")
	_ = metricsExporter.NewCounter(CategoryBridge, NameReconnect, LabelsReconnection, "WebSocket Gateway Reconnects")
	_ = metricsExporter.NewGauge(CategoryBridge, NameSubscribe, LabelsSubscriptions, "WebSocket Subscriptions")

	return metricsExporter
}
