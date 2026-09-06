package health

import (
	"context"
	"log/slog"
	"net/http"
	my_kafka "workers_kafka_gateway/internal/kafka"
	my_postgres "workers_kafka_gateway/internal/postgres"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Health struct {
	server    *http.Server
	dbAdapter *my_postgres.DBAdapter
	kafkaAddr string
}

func StartHealth(addr string, errChan chan error, dbAdapter *my_postgres.DBAdapter, kafkaAddr string) *Health {
	health := &Health{
		server:    nil,
		dbAdapter: dbAdapter,
		kafkaAddr: kafkaAddr,
	}

	router := chi.NewRouter()
	router.Use(middleware.Recoverer) //Для перехвата паник

	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/live", health.liveHandler)
	router.HandleFunc("/ready", health.readyHandler)

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

func (h *Health) liveHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (h *Health) readyHandler(w http.ResponseWriter, r *http.Request) {
	//BD
	if err := h.dbAdapter.Ping(); err != nil {
		slog.Error("ReadyHandler, Failed to ping postgreSQL", "ERROR", err.Error())
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	//Kafka
	if err := my_kafka.Ping(h.kafkaAddr); err != nil {
		slog.Error("ReadyHandler, Failed to ping kafka", "ERROR", err.Error())
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}
