package metrics

import (
	"github.com/pokt-foundation/portal-middleware/metrics/exporter"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	namePanic = "panic_recovered"

	labelAttempt = "attempt"
	labelSuccess = "success"
	labelError   = "error"

	// Router metrics
	categoryRouter = "router"

	nameHTTPRelay      = "http_relay"
	nameHTTPRelayError = "http_relay_error"
	nameWSRelay        = "ws_relay"
	nameWSRelayError   = "ws_relay_error"

	// Bridge metrics
	categoryBridge = "bridge"

	nameClientRelay       = "client_relay"
	nameClientRelayError  = "client_relay_error"
	nameGatewayRelay      = "gateway_relay"
	nameGatewayRelayError = "gateway_relay_error"
	nameReconnect         = "reconnect"
	nameSubscriptions     = "subscriptions"
	nameSubscribeError    = "subscribe_error"
	nameResubscribe       = "resubscribe"
	nameResubscribeError  = "resubscribe_error"

	LabelErrorWrite   = "write_error"
	LabelErrorRead    = "read_error"
	LabelErrorMarshal = "marshal_error"
	LabelErrorProcess = "process_error"
	LabelErrorParse   = "parse_error"
	LabelErrorProxy   = "proxy_error"
	LabelErrorBridge  = "bridge_error"
	LabelErrorUpgrade = "upgrade_error"
)

var (
	labelsRecovered = []string{"package", "error"}

	// Router labels
	labelsRelay      = []string{"status", "chain_alias"}
	labelsRelayError = []string{"error", "type"}

	// Bridge labels
	labelsWSRelay           = []string{"status"}
	labelsWSRelayError      = []string{"error", "type"}
	labelsReconnection      = []string{"status"}
	labelsSubscribe         = []string{"subscriptions"}
	labelsSubscriptions     = []string{"sub_id"}
	labelsSubscriptionError = []string{"error"}
)

type MetricExporter struct {
	exporter.MetricExporter
}

// NewMetricExporter registers all metrics to the metrics exporter
func NewMetricExporter(namespace string) *MetricExporter {
	metricExporter := exporter.NewMetricExporter(namespace)

	// Router metrics
	_ = metricExporter.NewCounter(categoryRouter, namePanic, labelsRecovered, "Panic Recovered")
	_ = metricExporter.NewCounter(categoryRouter, nameHTTPRelay, labelsRelay, "HTTP Relays")
	_ = metricExporter.NewCounter(categoryRouter, nameHTTPRelayError, labelsRelayError, "HTTP Relay Errors")
	_ = metricExporter.NewCounter(categoryRouter, nameWSRelay, labelsRelay, "WebSocket Relays")
	_ = metricExporter.NewCounter(categoryRouter, nameWSRelayError, labelsRelayError, "WebSocket Relay Errors")

	// Bridge metrics
	_ = metricExporter.NewCounter(categoryBridge, namePanic, labelsRecovered, "Panic Recovered")
	_ = metricExporter.NewCounter(categoryBridge, nameClientRelay, labelsWSRelay, "WebSocket Client Relays")
	_ = metricExporter.NewCounter(categoryBridge, nameClientRelayError, labelsWSRelayError, "WebSocket Client Relay Errors")
	_ = metricExporter.NewCounter(categoryBridge, nameGatewayRelay, labelsWSRelay, "WebSocket Gateway Relays")
	_ = metricExporter.NewCounter(categoryBridge, nameGatewayRelayError, labelsWSRelayError, "WebSocket Gateway Relay Errors")
	_ = metricExporter.NewCounter(categoryBridge, nameReconnect, labelsReconnection, "WebSocket Gateway Reconnects")
	_ = metricExporter.NewGauge(categoryBridge, nameSubscriptions, labelsSubscribe, "WebSocket Subscribe Events")
	_ = metricExporter.NewCounter(categoryBridge, nameSubscribeError, labelsSubscriptionError, "WebSocket Subscribe Errors")
	_ = metricExporter.NewCounter(categoryBridge, nameResubscribe, labelsSubscriptions, "WebSocket Resubscriptions")
	_ = metricExporter.NewCounter(categoryBridge, nameResubscribeError, labelsSubscriptionError, "WebSocket Resubscription Errors")

	return &MetricExporter{metricExporter}
}

func (me *MetricExporter) IncPanicRecovered(packagename, err string) {
	me.Counter(categoryRouter, namePanic).IncWithLabels(prometheus.Labels{
		"package": packagename,
		"error":   err,
	})
}

// Router methods

func (me *MetricExporter) IncHTTPRelayAttempt(chainAlias string) {
	me.Counter(categoryRouter, nameHTTPRelay).IncWithLabels(prometheus.Labels{
		"status":      labelAttempt,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncHTTPRelaySuccess(chainAlias string) {
	me.Counter(categoryRouter, nameHTTPRelay).IncWithLabels(prometheus.Labels{
		"status":      labelSuccess,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncHTTPRelayError(chainAlias, err, errType string) {
	me.Counter(categoryRouter, nameHTTPRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
		"type":  errType,
	})
}

func (me *MetricExporter) IncWSRelayAttempt(chainAlias string) {
	me.Counter(categoryRouter, nameWSRelay).IncWithLabels(prometheus.Labels{
		"status":      labelAttempt,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncWSRelaySuccess(chainAlias string) {
	me.Counter(categoryRouter, nameWSRelay).IncWithLabels(prometheus.Labels{
		"status":      labelSuccess,
		"chain_alias": chainAlias,
	})
}

func (me *MetricExporter) IncWSRelayError(chainAlias, err, errType string) {
	me.Counter(categoryRouter, nameWSRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
		"type":  errType,
	})
}

// Bridge methods

func (me *MetricExporter) IncClientRelayAttempt() {
	me.Counter(categoryBridge, nameClientRelay).IncWithLabels(prometheus.Labels{
		"status": labelAttempt,
	})
}

func (me *MetricExporter) IncClientRelaySuccess() {
	me.Counter(categoryBridge, nameClientRelay).IncWithLabels(prometheus.Labels{
		"status": labelSuccess,
	})
}

func (me *MetricExporter) IncClientRelayError(err string, errType string) {
	me.Counter(categoryBridge, nameClientRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
		"type":  errType,
	})
}

func (me *MetricExporter) IncGatewayRelayAttempt() {
	me.Counter(categoryBridge, nameGatewayRelay).IncWithLabels(prometheus.Labels{
		"status": labelAttempt,
	})
}

func (me *MetricExporter) IncGatewayRelaySuccess() {
	me.Counter(categoryBridge, nameGatewayRelay).IncWithLabels(prometheus.Labels{
		"status": labelSuccess,
	})
}

func (me *MetricExporter) IncGatewayRelayError(err string, errType string) {
	me.Counter(categoryBridge, nameGatewayRelayError).IncWithLabels(prometheus.Labels{
		"error": err,
		"type":  errType,
	})
}

func (me *MetricExporter) IncResubscribeSuccess(subID string) {
	me.Counter(categoryBridge, nameResubscribe).IncWithLabels(prometheus.Labels{
		"sub_id": subID,
	})
}

func (me *MetricExporter) IncResubscribeError(err string, errType string) {
	me.Counter(categoryBridge, nameResubscribeError).IncWithLabels(prometheus.Labels{
		"error": err,
		"type":  errType,
	})
}

func (me *MetricExporter) AddSubscribe(amount float64) {
	me.Gauge(categoryBridge, nameSubscriptions).Add(labelSuccess, amount)
}

func (me *MetricExporter) IncSubscribeError(err string) {
	me.Counter(categoryBridge, nameSubscribeError).IncWithLabels(prometheus.Labels{
		"error": err,
	})
}

func (me *MetricExporter) IncReconnect() {
	me.Counter(categoryBridge, nameReconnect).IncWithLabels(prometheus.Labels{
		"status": labelSuccess,
	})
}
