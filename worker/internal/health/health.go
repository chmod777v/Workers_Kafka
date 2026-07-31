package health

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Health struct {
	server *http.Server
	ready  atomic.Bool
}

func StartHealth(addr string, errChan chan error) *Health {
	health := &Health{}

	router := chi.NewRouter()
	router.Use(middleware.Recoverer) //Для перехвата паник
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/ready", health.handler)

	serv := &http.Server{
		Addr:    addr,
		Handler: router,
	}
	health.server = serv

	go func() {
		if err := serv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Health error", "ERROR", err)
			errChan <- err
		}
	}()

	return health
}

func (h *Health) Close(ctx context.Context) error {
	if err := h.server.Shutdown(ctx); err != nil {
		h.server.Close()
		return err
	}
	return nil
}

func (h *Health) Ready() {
	h.ready.Store(true)
}

func (h *Health) handler(w http.ResponseWriter, r *http.Request) {
	if !h.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Metrics
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
