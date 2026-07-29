package metric

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metric struct {
	server *http.Server
}

func StartMetric(addr string, errChan chan error) *Metric {
	router := newRouter()

	serv := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	go func() {
		if err := serv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Metric error", "ERROR", err)
			errChan <- err
		}
	}()

	return &Metric{
		server: serv,
	}
}

func (m *Metric) Close(ctx context.Context) error {
	if err := m.server.Shutdown(ctx); err != nil {
		m.server.Close()
		return err
	}
	return nil
}

func newRouter() *chi.Mux {
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
