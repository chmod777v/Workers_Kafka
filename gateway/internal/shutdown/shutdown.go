package shutdown

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"workers_kafka_gateway/internal/healthcheck"
	"workers_kafka_gateway/internal/metric"
	"workers_kafka_gateway/internal/rest/gateway"
)

func Shutdown(errChan chan error, healthCheck *healthcheck.HealthCheck, metric *metric.Metric, gateway *gateway.Gateway) {
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
	//Gateway
	if err := gateway.Close(ctx); err != nil {
		slog.Error("Failed to stop gateway", "ERROR:", err.Error())
	} else {
		slog.Info("Gateway stopped successfully")
	}

	//Metric
	if err := metric.Close(ctx); err != nil {
		slog.Error("Failed to stop metric", "ERROR:", err.Error())
	} else {
		slog.Info("Metric stopped successfully")
	}

	//HealthCheck
	if err := healthCheck.Close(ctx); err != nil {
		slog.Error("Failed to stop healthcheck", "ERROR:", err.Error())
	} else {
		slog.Info("Healthcheck stopped successfully")
	}
}
