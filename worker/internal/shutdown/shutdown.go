package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"workers_kafka_worker/internal/health"
)

func Shutdown(errChan chan error, health *health.Health) {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
		slog.Info("Shutdown")
	case err := <-errChan:
		slog.Error("Service error, initiating shutdown", "ERROR:", err.Error())
	}

label:
	for {
		select {
		case err := <-errChan:
			slog.Error("Additional service error", "error", err)
		default:
			break label
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := health.Close(ctx); err != nil {
		slog.Error("Failed to stop health", "ERROR:", err.Error())
	} else {
		slog.Info("Health stopped successfully")
	}
}
