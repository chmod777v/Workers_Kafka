package health

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "worker",
			Name:      "request_duration_seconds",
			Help:      "request duration in seconds",
			Buckets:   prometheus.DefBuckets,
		}, []string{},
	)
)

func ObserveRequest(duration time.Duration) {
	requestDuration.WithLabelValues().Observe(duration.Seconds())
}
