package main

import (
	"context"
	"fmt"
	"log/slog"
	"workers_kafka_gateway/internal/config"
	"workers_kafka_gateway/internal/healthcheck"
	my_kafka "workers_kafka_gateway/internal/kafka"
	"workers_kafka_gateway/internal/logger"
	"workers_kafka_gateway/internal/metric"
	"workers_kafka_gateway/internal/rest/gateway"

	"workers_kafka_gateway/internal/shutdown"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

	errChan := make(chan error, 10)
	//Helthcheck start
	hcAddr := fmt.Sprintf("%s:%d", cfg.HealthCheck.Host, cfg.HealthCheck.Port)
	healthCheck := healthcheck.StartHealthCheck(hcAddr, errChan)
	slog.Info("Healthcheck started", "Addr", hcAddr)

	//DB
	dbLink := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.Db.Username, cfg.Db.Password, cfg.Db.Host, cfg.Db.Port, cfg.Db.DbName)

	dbpool, err := pgxpool.New(context.Background(), dbLink)
	if err != nil {
		slog.Error("Failed to connect to the postgreSQL", "ERROR", err.Error())
		return
	}
	if err := dbpool.Ping(context.Background()); err != nil {
		slog.Error("Failed to ping postgreSQL", "ERROR", err.Error())
		return
	}
	slog.Info("Database connection successfully")

	defer func() { //close
		dbpool.Close()
		dbpool = nil
		slog.Info("Database connection closed successfully")
	}()

	// Kafka rw
	kafkaAddr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)

	manager := my_kafka.NewManager(kafkaAddr)
	slog.Info("Kafka writer and reader launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		manager.Close()
	}()

	// Kafka Listen
	listener := my_kafka.NewListener(manager.Reader, dbpool)
	listener.Listening()
	slog.Info("Kafka Listen")

	defer func() { //close
		listener.Close()
		slog.Info("Kafka Listener closed successfully")
	}()

	// Metrics
	metricAddr := fmt.Sprintf("%s:%d", cfg.Metric.Host, cfg.Metric.Port)
	metric := metric.StartMetric(metricAddr, errChan)
	slog.Info("Metric started", "Addr", metricAddr)

	// Gateway
	gatewayAddr := fmt.Sprintf("%s:%d", cfg.Gateway.Host, cfg.Gateway.Port)
	gateway := gateway.StartGateway(gatewayAddr, errChan, manager.Writer, dbpool)
	slog.Info("Gateway started", "Addr", gatewayAddr)

	//READY
	healthCheck.Ready()
	slog.Info("--READY--")

	shutdown.Shutdown(errChan, healthCheck, metric, gateway)
}
