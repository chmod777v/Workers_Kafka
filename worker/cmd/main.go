package main

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
	"workers_kafka_worker/internal/config"
	my_kafka "workers_kafka_worker/internal/kafka"
	"workers_kafka_worker/internal/logger"

	"github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.LoadConfig()
	logger.InitLogger(cfg.Env)
	slog.Info("Cfg, Logger launched successfully")

	kafkaAddr := fmt.Sprintf("%s:%d", cfg.Kafka.Host, cfg.Kafka.Port)
	// Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Topic:   "tasks",
		Brokers: []string{kafkaAddr},
		GroupID: "workers",
	})
	slog.Info("Kafka reader launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		if err := reader.Close(); err != nil {
			slog.Error("Failed to close kafka reader", "ERROR", err.Error())
		} else {
			slog.Info("Kafka reader closed successfully")
		}
	}()

	// Kafka Writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaAddr},
		Topic:   "result",

		RequiredAcks: 1,
		MaxAttempts:  10,
		BatchSize:    1,
		WriteTimeout: 3 * time.Second,
		BatchTimeout: 100 * time.Millisecond,
		Balancer:     &kafka.RoundRobin{},
	})
	slog.Info("Kafka writer launched successfully", "KafkaAddr", kafkaAddr)

	defer func() { //close
		if err := writer.Close(); err != nil {
			slog.Error("Failed to close kafka writer", "ERROR", err.Error())
		} else {
			slog.Info("Kafka writer closed successfully")
		}
	}()

	// Kafka Listen
	listener := my_kafka.NewListener(reader, writer)
	listener.Listening()
	slog.Info("Kafka Listen")

	defer func() { //close
		listener.Close()
		slog.Info("Kafka Listener closed successfully")
	}()

	//Shutdown
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	<-done
	slog.Info("Shutdown")
}
