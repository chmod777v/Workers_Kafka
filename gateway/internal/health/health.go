package health

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type Health struct {
	server    *http.Server
	dbpool    *pgxpool.Pool
	kafkaAddr string
}

func StartHealth(addr string, errChan chan error, dbpool *pgxpool.Pool, kafkaAddr string) *Health {
	health := &Health{
		server:    nil,
		dbpool:    dbpool,
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
	dbCtx, dbCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dbCancel()

	if err := h.dbpool.Ping(dbCtx); err != nil {
		slog.Error("ReadyHandler, Failed to ping postgreSQL", "ERROR", err.Error())
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	//Kafka
	kafkaCtx, kafkaCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer kafkaCancel()

	conn, err := kafka.DialContext(kafkaCtx, "tcp", h.kafkaAddr)
	if err != nil {
		slog.Error("ReadyHandler, Failed to ping kafka", "ERROR", err.Error())
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer conn.Close()

	w.WriteHeader(http.StatusOK)
}
