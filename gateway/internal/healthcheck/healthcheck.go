package healthcheck

import (
	"context"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
)

type HealthCheck struct {
	ready  atomic.Bool
	server *http.Server
}

func StartHealthCheck(addr string, errChan chan error) *HealthCheck {
	HealthCheck := &HealthCheck{}
	HealthCheck.ready.Store(false)

	router := chi.NewRouter()
	router.Use(middleware.Recoverer)
	router.HandleFunc("/ready", HealthCheck.handler)

	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}
	HealthCheck.server = server

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Helthcheck error", "ERROR", err)
			//Вызов gracefull Shutdown всего приложения для закрытия бд и соединений
		}
	}()

	return HealthCheck
}

func (h *HealthCheck) handler(w http.ResponseWriter, r *http.Request) {
	if !h.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *HealthCheck) Ready() {
	h.ready.Store(true)
}

func (h *HealthCheck) Close(ctx context.Context) error {
	if err := h.server.Shutdown(ctx); err != nil {
		h.server.Close()
		return err
	}
	return nil
}
