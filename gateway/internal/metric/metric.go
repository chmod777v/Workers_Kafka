package metric

import (
	"strconv"
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
	requestTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gateway",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Total HTTP requests",
		},
		[]string{"method", "status"},
	)

	requestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "gateway",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "HTTP request duration in seconds",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "status"},
	)
)

func ObserveRequest(method string, duration time.Duration, status int) {
	requestDuration.WithLabelValues(method, strconv.Itoa(status)).Observe(duration.Seconds())
	requestTotal.WithLabelValues(method, strconv.Itoa(status)).Inc()
}
