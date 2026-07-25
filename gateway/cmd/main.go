package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"workers_kafka_gateway/internal/config"
	my_kafka "workers_kafka_gateway/internal/kafka"
	"workers_kafka_gateway/internal/logger"
	"workers_kafka_gateway/internal/metric"
	"workers_kafka_gateway/internal/rest/server"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

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

	// Server
	serverRouter := server.NewRouter(manager.Writer, dbpool)

	serverAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	serverServ := &http.Server{
		Addr:    serverAddr,
		Handler: serverRouter,
	}

	serverErr := make(chan error, 1)
	go func() {
		defer close(serverErr)
		if err := serverServ.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()
	slog.Info("Server started", "Addr", serverAddr)

	server.Shutdown(serverServ, serverErr, metricServ, metricErr)
}
