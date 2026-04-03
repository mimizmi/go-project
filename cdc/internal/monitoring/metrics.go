// Package monitoring 定义 Prometheus 指标并暴露 HTTP /metrics 端点。
package monitoring

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Metrics 汇总所有 Prometheus 指标。
type Metrics struct {
	// 采集端
	CdcEventsTotal  *prometheus.CounterVec
	CdcErrorsTotal  *prometheus.CounterVec
	CdcLagSeconds   *prometheus.GaugeVec

	// Kafka 端
	KafkaTxnTotal    *prometheus.CounterVec
	KafkaSendLatency *prometheus.HistogramVec

	// 消费端
	SinkEventsTotal     *prometheus.CounterVec
	SinkDuplicatesTotal *prometheus.CounterVec
	SinkWriteLatency    *prometheus.HistogramVec

	// 位点
	OffsetPosition *prometheus.GaugeVec

	// 端到端
	E2ELatency *prometheus.HistogramVec

	reg *prometheus.Registry
}

// NewMetrics 创建并注册所有指标。
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &Metrics{reg: reg}

	m.CdcEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_events_total",
		Help: "Total CDC events captured",
	}, []string{"source", "table", "op_type"})

	m.CdcErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cdc_errors_total",
		Help: "CDC capture errors",
	}, []string{"source", "error_type"})

	m.CdcLagSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_lag_seconds",
		Help: "Estimated CDC capture lag in seconds",
	}, []string{"source"})

	m.KafkaTxnTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_txn_total",
		Help: "Kafka transactions (committed/aborted)",
	}, []string{"source", "status"})

	m.KafkaSendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_send_latency_seconds",
		Help:    "Kafka batch send latency",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
	}, []string{"source"})

	m.SinkEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sink_events_total",
		Help: "Events written to sink",
	}, []string{"table", "op_type"})

	m.SinkDuplicatesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "sink_duplicates_total",
		Help: "Duplicate events skipped by idempotency guard",
	}, []string{"table"})

	m.SinkWriteLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "sink_write_latency_seconds",
		Help:    "Sink write latency per event",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0},
	}, []string{"table"})

	m.OffsetPosition = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "offset_position",
		Help: "Current offset position (binlog_pos or LSN numeric)",
	}, []string{"source", "type"})

	m.E2ELatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "e2e_latency_seconds",
		Help:    "End-to-end latency from source write to sink write",
		Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0},
	}, []string{"source", "table"})

	reg.MustRegister(
		m.CdcEventsTotal, m.CdcErrorsTotal, m.CdcLagSeconds,
		m.KafkaTxnTotal, m.KafkaSendLatency,
		m.SinkEventsTotal, m.SinkDuplicatesTotal, m.SinkWriteLatency,
		m.OffsetPosition, m.E2ELatency,
	)
	return m
}

// Serve 在指定端口暴露 /metrics 和 /health 端点。
func (m *Metrics) Serve(port int, logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	})

	addr := fmt.Sprintf(":%d", port)
	logger.Info("metrics server listening", zap.String("addr", addr))
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("metrics server error", zap.Error(err))
	}
}
