// Package metrics 定义全局 Prometheus 指标。
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics 聚合所有 Prometheus 指标。
type Metrics struct {
	// 同步层
	SyncRows prometheus.CounterVec
	SyncLag  prometheus.GaugeVec

	// 缓存层
	CacheHits   prometheus.CounterVec   // label: level (L1/L2)
	CacheMisses prometheus.Counter
	CacheSize   prometheus.GaugeVec     // label: level

	// 查询层
	QueryRequests prometheus.CounterVec  // label: endpoint, status
	QueryDuration prometheus.HistogramVec // label: endpoint
}

// New 注册并返回 Metrics 实例。
func New() *Metrics {
	m := &Metrics{
		SyncRows: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "syncer_rows_total",
			Help: "Number of rows synced from ODS to ClickHouse by table.",
		}, []string{"table"}),

		SyncLag: *prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "syncer_lag_seconds",
			Help: "Time taken for last sync cycle per table.",
		}, []string{"table"}),

		CacheHits: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Number of cache hits by level (L1/L2).",
		}, []string{"level"}),

		CacheMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Number of cache misses (fell through to ClickHouse).",
		}),

		CacheSize: *prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cache_size_items",
			Help: "Current number of items in cache by level.",
		}, []string{"level"}),

		QueryRequests: *prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "query_requests_total",
			Help: "Total query API requests by endpoint and status.",
		}, []string{"endpoint", "status"}),

		QueryDuration: *prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "query_duration_seconds",
			Help:    "Query end-to-end latency histogram.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"endpoint"}),
	}

	prometheus.MustRegister(
		&m.SyncRows, &m.SyncLag,
		&m.CacheHits, m.CacheMisses, &m.CacheSize,
		&m.QueryRequests, &m.QueryDuration,
	)
	return m
}

// Handler 返回 /metrics HTTP handler。
func Handler() http.Handler { return promhttp.Handler() }
