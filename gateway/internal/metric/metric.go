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

var requestMetrics = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Namespace:  "test_server",
	Subsystem:  "http",
	Name:       "request",
	Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
}, []string{"method", "status"})

func ObserveRequest(method string, d time.Duration, status int) {
	requestMetrics.WithLabelValues(method, strconv.Itoa(status)).Observe(d.Seconds())
}
