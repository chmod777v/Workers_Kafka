package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"workers_kafka_worker/internal/config"
	my_kafka "workers_kafka_worker/internal/kafka"
	"workers_kafka_worker/internal/logger"
	"workers_kafka_worker/internal/metric"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

	kafkaAddr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
	// Kafka rw
	manager := my_kafka.NewManager(kafkaAddr)
	slog.Info("Kafka writer and reader launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		manager.Close()
	}()

	// Kafka Listen
	listener := my_kafka.NewListener(manager.Reader, manager.Writer)
	listener.Listening()
	slog.Info("Kafka Listen")

	defer func() { //close
		listener.Close()
		slog.Info("Kafka Listener closed successfully")
	}()

	// Metrics/HealthCheck
	metricRouter := metric.NewRouter()

	metricAddr := fmt.Sprintf("%s:%d", cfg.Metric.Host, cfg.Metric.Port)
	metricServ := &http.Server{
		Addr:    metricAddr,
		Handler: metricRouter,
	}

	metricErr := make(chan error, 1)
	go func() {
		defer close(metricErr)
		if err := metricServ.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			metricErr <- err
		}
	}()
	slog.Info("Metric started", "Addr", metricAddr)

	//Shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
		slog.Info("Shutdown")
	case err := <-metricErr:
		slog.Error("Metric error", "ERROR", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := metricServ.Shutdown(ctx); err != nil {
		slog.Error("Failed to stop metric", "ERROR:", err.Error())
		metricServ.Close()
	} else {
		slog.Info("Metric stopped successfully")
	}
}
