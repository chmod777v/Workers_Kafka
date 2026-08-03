package main

import (
	"fmt"
	"log/slog"
	"workers_kafka_worker/internal/config"
	"workers_kafka_worker/internal/health"
	my_kafka_listener "workers_kafka_worker/internal/kafka/listener"
	my_kafka_manager "workers_kafka_worker/internal/kafka/manager"

	"workers_kafka_worker/internal/logger"
	"workers_kafka_worker/internal/shutdown"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

	errChan := make(chan error, 10)

	// Kafka rw
	kafkaAddr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
	manager, err := my_kafka_manager.NewManager(kafkaAddr)
	if err != nil {
		slog.Error("Failed to ping kafka", "ERROR", err.Error())
		return
	}
	slog.Info("Kafka writer and reader launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		manager.Close()
	}()

	// Kafka Listen
	listener := my_kafka_listener.NewListener(manager.Reader, manager.Writer)
	listener.Listening()
	slog.Info("Kafka Listen")

	defer func() { //close
		listener.Close()
		slog.Info("Kafka Listener closed successfully")
	}()

	// Health
	healthAddr := fmt.Sprintf("%s:%d", cfg.Health.Host, cfg.Health.Port)
	health := health.StartHealth(healthAddr, errChan, kafkaAddr)
	slog.Info("Health started", "HealthCheckAddr", healthAddr+"/ready", "MetricsAddr", healthAddr+"/metrics")

	shutdown.Shutdown(errChan, health)
}
