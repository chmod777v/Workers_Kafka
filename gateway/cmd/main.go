package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
	"workers_kafka_gateway/internal/config"
	my_kafka "workers_kafka_gateway/internal/kafka"
	"workers_kafka_gateway/internal/logger"
	"workers_kafka_gateway/internal/metric"
	"workers_kafka_gateway/internal/rest/server"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

	// Kafka Writer
	kafkaAddr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaAddr},
		Topic:   "tasks",

		RequiredAcks: 1,                      //0: at most once; 1: At least once; -1: At least once, но с дополнительными подтверждениями (по умолчанию), самые надежные гарантии, сообщения подтверждаются всеми репликами
		MaxAttempts:  10,                     //кол-во попыток доставки(по умолчанию 10)
		BatchSize:    1,                      //кол-во сообщений которые накапливает kafka прежде чем отправить (по умолчанию 100)
		WriteTimeout: 3 * time.Second,        //время, которое Writer ждет ответа от Kafka после отправки(по умолчанию 10сек)
		BatchTimeout: 100 * time.Millisecond, //время ожидания накопления батча
		Balancer:     &kafka.RoundRobin{},    //балансировщик(решает, в какую партицию отправить сообщение)

	})
	slog.Info("Kafka writer launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		if err := writer.Close(); err != nil {
			slog.Error("Failed to close kafka writer", "ERROR", err.Error())
		} else {
			slog.Info("Kafka writer closed successfully")
		}
	}()

	// Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Topic:   "result",
		Brokers: []string{kafkaAddr},
		GroupID: "gateway",
	})
	slog.Info("Kafka reader launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		if err := reader.Close(); err != nil {
			slog.Error("Failed to close kafka reader", "ERROR", err.Error())
		} else {
			slog.Info("Kafka reader closed successfully")
		}
	}()

	//DB
	dbLink := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.Db.Username, cfg.Db.Password, cfg.Db.Host, cfg.Db.Port, cfg.Db.DbName)
	dbpool, err := pgxpool.New(context.Background(), dbLink)
	if err != nil {
		slog.Error("Failed to connect to the postgreSQL", "ERROR", err.Error())
		return
	}
	slog.Info("Database connection successfully")

	defer func() { //close
		dbpool.Close()
		dbpool = nil
		slog.Info("Database connection closed successfully")
	}()

	// Kafka Listen
	listener := my_kafka.NewListener(reader, dbpool)
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
	serverRouter := server.NewRouter(writer, dbpool)

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
