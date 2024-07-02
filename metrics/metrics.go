package metrics

import (
	"github.com/pokt-foundation/portal-middleware/metrics/exporter"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	NamePanic = "panic_recovered"

	LabelAttempt = "attempt"
	LabelSuccess = "success"
	LabelError   = "error"

	// Router metrics
	CategoryRouter = "router"

	NameHTTPRelay      = "http_relay"
	NameHTTPRelayError = "http_relay_error"
	NameWSRelay        = "ws_relay"
	NameWSRelayError   = "ws_relay_error"

	// Bridge metrics
	CategoryBridge = "bridge"

	NameClientRelay       = "client_relay"
	NameClientRelayError  = "client_relay_error"
	NameGatewayRelay      = "gateway_relay"
	NameGatewayRelayError = "gateway_relay_error"
	NameReconnect         = "reconnect"
	NameReconnectError    = "reconnect_error"
	NameSubscribe         = "subscribe"
	NameSubscribeError    = "subscribe_error"
	NameResubscribe       = "resubscribe"
	NameResubscribeError  = "resubscribe_error"

	LabelErrorWrite   = "write_error"
	LabelErrorRead    = "read_error"
	LabelErrorMarshal = "marshal_error"
	LabelErrorProcess = "process_error"

	LabelActiveSubscriptions = "active_subscriptions"
)

var (
	LabelsRecovered = []string{"package", "error"}

	// Router Labels
	LabelsRelay      = []string{"outcome", "chain_alias"}
	LabelsRelayError = []string{"error"}

	// Bridge Labels
	LabelsWSRelay           = []string{"outcome"}
	LabelsWSRelayError      = []string{"error"}
	LabelsReconnection      = []string{"outcome"}
	LabelsReconnectionError = []string{"error"}
	LabelsSubscriptions     = []string{"outcome", "sub_id"}
	LabelsSubscriptionError = []string{"error"}
)

type MetricExporter struct {
	exporter.MetricExporter
}

// NewMetricExporter registers all metrics to the metrics exporter
func NewMetricExporter(namespace string) *MetricExporter {
	metricExporter := exporter.NewMetricExporter(namespace)

	// Router metrics
	_ = metricExporter.NewCounter(CategoryRouter, NamePanic, LabelsRecovered, "Panic Recovered")
	_ = metricExporter.NewCounter(CategoryRouter, NameHTTPRelay, LabelsRelay, "HTTP Relays")
	_ = metricExporter.NewCounter(CategoryRouter, NameHTTPRelayError, LabelsRelayError, "HTTP Relay Errors")
	_ = metricExporter.NewCounter(CategoryRouter, NameWSRelay, LabelsRelay, "WebSocket Relays")
	_ = metricExporter.NewCounter(CategoryRouter, NameWSRelayError, LabelsRelayError, "WebSocket Relay Errors")

	// Bridge metrics
	_ = metricExporter.NewCounter(CategoryBridge, NamePanic, LabelsRecovered, "Panic Recovered")
	_ = metricExporter.NewCounter(CategoryBridge, NameClientRelay, LabelsWSRelay, "WebSocket Client Relays")
	_ = metricExporter.NewCounter(CategoryBridge, NameClientRelayError, LabelsWSRelayError, "WebSocket Client Relay Errors")
	_ = metricExporter.NewCounter(CategoryBridge, NameGatewayRelay, LabelsWSRelay, "WebSocket Gateway Relays")
	_ = metricExporter.NewCounter(CategoryBridge, NameGatewayRelayError, LabelsWSRelayError, "WebSocket Gateway Relay Errors")
	_ = metricExporter.NewCounter(CategoryBridge, NameReconnect, LabelsReconnection, "WebSocket Gateway Reconnects")
	_ = metricExporter.NewCounter(CategoryBridge, NameReconnectError, LabelsReconnectionError, "WebSocket Gateway Reconnect Errors")
	_ = metricExporter.NewGauge(CategoryBridge, NameSubscribe, LabelsSubscriptions, "WebSocket Subscriptions")
	_ = metricExporter.NewCounter(CategoryBridge, NameSubscribeError, LabelsSubscriptionError, "WebSocket Subscription Errors")
	_ = metricExporter.NewCounter(CategoryBridge, NameResubscribe, LabelsSubscriptions, "WebSocket Resubscriptions")
	_ = metricExporter.NewCounter(CategoryBridge, NameResubscribeError, LabelsSubscriptionError, "WebSocket Resubscription Errors")

	return &MetricExporter{metricExporter}
}

func (me *MetricExporter) IncPanicRecovered(packageName, err string) {
	me.Counter(CategoryRouter, NamePanic).IncWithLabels(prometheus.Labels{
		"package": packageName,
		"error":   err,
	})
}

// Router methods

func (me *MetricExporter) IncHTTPRelayAttempt(chainAlias string) {
	me.Counter(CategoryRouter, NameHTTPRelay).IncWithLabels(prometheus.Labels{
		"outcome":     LabelAttempt,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncHTTPRelaySuccess(chainAlias string) {
	me.Counter(CategoryRouter, NameHTTPRelay).IncWithLabels(prometheus.Labels{
		"outcome":     LabelSuccess,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncHTTPRelayError(chainAlias, err string) {
	me.Counter(CategoryRouter, NameHTTPRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
	})
}

func (me *MetricExporter) IncWSRelayAttempt(chainAlias string) {
	me.Counter(CategoryRouter, NameWSRelay).IncWithLabels(prometheus.Labels{
		"outcome":     LabelAttempt,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncWSRelaySuccess(chainAlias string) {
	me.Counter(CategoryRouter, NameWSRelay).IncWithLabels(prometheus.Labels{
		"outcome":     LabelSuccess,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncWSRelayError(chainAlias, err string) {
	me.Counter(CategoryRouter, NameWSRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
	})
}

// Bridge methods

func (me *MetricExporter) IncClientRelayAttempt() {
	me.Counter(CategoryBridge, NameClientRelay).IncWithLabels(prometheus.Labels{
		"outcome": LabelAttempt,
	})
}

func (me *MetricExporter) IncClientRelaySuccess() {
	me.Counter(CategoryBridge, NameClientRelay).IncWithLabels(prometheus.Labels{
		"outcome": LabelSuccess,
	})
}

func (me *MetricExporter) IncClientRelayError(err string) {
	me.Counter(CategoryBridge, NameClientRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
	})
}

func (me *MetricExporter) IncGatewayRelayAttempt() {
	me.Counter(CategoryBridge, NameGatewayRelay).IncWithLabels(prometheus.Labels{
		"outcome": LabelAttempt,
	})
}

func (me *MetricExporter) IncGatewayRelaySuccess() {
	me.Counter(CategoryBridge, NameGatewayRelay).IncWithLabels(prometheus.Labels{
		"outcome": LabelSuccess,
	})
}

func (me *MetricExporter) IncGatewayRelayError(err string) {
	me.Counter(CategoryBridge, NameGatewayRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
	})
}

func (me *MetricExporter) IncResubscribeSuccess(subID string) {
	me.Counter(CategoryBridge, NameResubscribe).IncWithLabels(prometheus.Labels{
		"outcome": LabelSuccess,
		"sub_id":  subID,
	})
}

func (me *MetricExporter) IncResubscribeError(err string) {
	me.Counter(CategoryBridge, NameResubscribeError).IncWithLabels(prometheus.Labels{
		"error": err,
	})
}

func (me *MetricExporter) AddSubscribe(val float64) {
	me.Gauge(CategoryBridge, NameSubscribe).AddWithLabels(prometheus.Labels{
		"outcome": LabelActiveSubscriptions,
	}, val)
}
