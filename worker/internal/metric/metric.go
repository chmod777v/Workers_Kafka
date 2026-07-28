package metric

import (
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func NewRouter() *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.Recoverer) //Для перехвата паник

	router.Handle("/metrics", promhttp.Handler())

	return router
}

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
