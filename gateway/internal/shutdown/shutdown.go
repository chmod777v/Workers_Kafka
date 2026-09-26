package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"workers_kafka_gateway/internal/health"
	"workers_kafka_gateway/internal/rest/gateway"
)

func Shutdown(errChan chan error, health *health.Health, gateway *gateway.Gateway) {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
		slog.Info("Shutdown")
	case err := <-errChan:
		slog.Error("Service error, initiating shutdown", "ERROR:", err)
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
	//Gateway
	if err := gateway.Close(ctx); err != nil {
		slog.Error("Failed to stop gateway", "ERROR:", err)
	} else {
		slog.Info("Gateway stopped successfully")
	}

	//Health
	if err := health.Close(ctx); err != nil {
		slog.Error("Failed to stop health", "ERROR:", err)
	} else {
		slog.Info("Health stopped successfully")
	}

}
