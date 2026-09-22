package main

import (
	"fmt"
	"log/slog"
	"workers_kafka_gateway/internal/config"
	"workers_kafka_gateway/internal/health"
	my_postgres "workers_kafka_gateway/internal/postgres"
	my_redis "workers_kafka_gateway/internal/redis"

	my_kafka "workers_kafka_gateway/internal/kafka"
	"workers_kafka_gateway/internal/logger"
	"workers_kafka_gateway/internal/rest/gateway"

	"workers_kafka_gateway/internal/shutdown"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

	errChan := make(chan error, 10)

	// DB
	dbLink := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.Db.Username, cfg.Db.Password, cfg.Db.Host, cfg.Db.Port, cfg.Db.DbName)

	dbAdapter, err := my_postgres.NewDBAdapter(dbLink)
	if err != nil {
		slog.Error("Failed create dbAdapter", "ERROR", err)
		return
	}
	slog.Info("Database connected successfully")

	defer func() { //close
		dbAdapter.Close()
		slog.Info("Database connection closed successfully")
	}()

	// REDIS
	redisAddr := fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port)
	redisAdapter, err := my_redis.NewAdapter(redisAddr, cfg.Redis.Password)
	if err != nil {
		slog.Error("Failed create redisAdapter", "ERROR", err)
		return
	}
	slog.Info("Redis connected successfully")

	defer func() { //close
		redisAdapter.Close()
		slog.Info("Redis connection closed successfully")
	}()

	// Kafka rw
	kafkaAddr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)

	manager, err := my_kafka.NewManager(kafkaAddr)
	if err != nil {
		slog.Error("Failed to ping kafka", "ERROR", err.Error())
		return
	}
	slog.Info("Kafka writer and reader launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		manager.Close()
	}()

	// Kafka Listen
	listener := my_kafka.NewListener(manager.Reader, dbAdapter)
	listener.Listening()
	slog.Info("Kafka Listen")

	defer func() { //close
		listener.Close()
		slog.Info("Kafka Listener closed successfully")
	}()

	// Gateway
	gatewayAddr := fmt.Sprintf("%s:%d", cfg.Gateway.Host, cfg.Gateway.Port)
	gateway := gateway.StartGateway(gatewayAddr, errChan, manager.Writer, dbAdapter, redisAdapter)
	slog.Info("Gateway started", "Addr", gatewayAddr)

	// Helth
	healthAddr := fmt.Sprintf("%s:%d", cfg.Health.Host, cfg.Health.Port)
	health := health.StartHealth(healthAddr, errChan, dbAdapter, kafkaAddr)
	slog.Info("Health started", "HealthCheckAddr", healthAddr+"/ready"+" & "+healthAddr+"/live", "MetricsAddr", healthAddr+"/metrics")

	shutdown.Shutdown(errChan, health, gateway)
}
