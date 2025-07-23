package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type PrometheusService struct {
	totalSelloutExport    *prometheus.CounterVec
	durationSelloutExport *prometheus.HistogramVec
}

func NewPrometheusService() (*PrometheusService, error) {
	s := PrometheusService{}

	// Init Metrics
	s.totalSelloutExport = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "exporter_sellout_total",
		Help: "Total number of export Sellout.",
	}, []string{"code", "source"})

	s.durationSelloutExport = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "exporter_sellout_duration_seconds",
		Help:    "The latency of export Sellout.",
		Buckets: []float64{10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 600, 900, 1200, 1800},
	}, []string{"code", "source"})

	// Registering
	err := prometheus.Register(s.totalSelloutExport)
	if err != nil && err.Error() != "duplicate metrics collector registration attempted" {
		return nil, err
	}

	err = prometheus.Register(s.durationSelloutExport)
	if err != nil && err.Error() != "duplicate metrics collector registration attempted" {
		return nil, err
	}

	return &s, nil
}

func (srv *PrometheusService) DurationSelloutExport(val float64, code, source string) {
	srv.durationSelloutExport.WithLabelValues(code, source).Observe(val)
}

func (srv *PrometheusService) TotalSelloutExport(code, source string) {
	srv.totalSelloutExport.WithLabelValues(code, source).Inc()
}
