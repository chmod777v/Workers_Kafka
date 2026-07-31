package main

import (
	"fmt"
	"log/slog"
	"workers_kafka_worker/internal/config"
	"workers_kafka_worker/internal/health"
	my_kafka "workers_kafka_worker/internal/kafka"
	"workers_kafka_worker/internal/logger"
	"workers_kafka_worker/internal/shutdown"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

	errChan := make(chan error, 10)

	// Health
	healthAddr := fmt.Sprintf("%s:%d", cfg.Health.Host, cfg.Health.Port)
	health := health.StartHealth(healthAddr, errChan)
	slog.Info("Health started", "HealthCheckAddr", healthAddr+"/ready", "MetricsAddr", healthAddr+"/metrics")

	// Kafka rw
	kafkaAddr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
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

	//READY
	health.Ready()
	slog.Info("--READY--")

	shutdown.Shutdown(errChan, health)
}
