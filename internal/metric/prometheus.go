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
	}, []string{"code"})

	s.durationSelloutExport = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "exporter_sellout_duration_seconds",
		Help:    "The latency of export Sellout.",
		Buckets: []float64{1, 2, 3, 4, 5, 7, 10, 15, 20, 25, 30, 40, 50, 60, 90, 120, 180},
	}, []string{"code"})

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

func (srv *PrometheusService) DurationSelloutExport(code string, val float64) {
	srv.durationSelloutExport.WithLabelValues(code).Observe(val)
}

func (srv *PrometheusService) TotalSelloutExport(code string) {
	srv.totalSelloutExport.WithLabelValues(code).Inc()
}
