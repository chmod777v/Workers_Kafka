package my_kafka

import (
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Manager struct {
	Writer *kafka.Writer
	Reader *kafka.Reader
}

func NewManager(kafkaAddr string) *Manager {
	// Kafka Writer
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaAddr},
		Topic:   "result",

		RequiredAcks: 1,                      //0: at most once; 1: At least once; -1: At least once, но с дополнительными подтверждениями (по умолчанию), самые надежные гарантии, сообщения подтверждаются всеми репликами
		MaxAttempts:  10,                     //кол-во попыток доставки(по умолчанию 10)
		BatchSize:    1,                      //кол-во сообщений которые накапливает kafka прежде чем отправить (по умолчанию 100)
		WriteTimeout: 3 * time.Second,        //время, которое Writer ждет ответа от Kafka после отправки(по умолчанию 10сек)
		BatchTimeout: 100 * time.Millisecond, //время ожидания накопления батча
		Balancer:     &kafka.RoundRobin{},    //балансировщик(решает, в какую партицию отправить сообщение)
	})

	// Kafka Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Topic:   "tasks",
		Brokers: []string{kafkaAddr},
		GroupID: "workers",
	})

	return &Manager{
		Writer: writer,
		Reader: reader,
	}
}

func (m *Manager) Close() {
	if err := m.Writer.Close(); err != nil {
		slog.Error("Failed to close kafka writer", "ERROR", err.Error())
	} else {
		slog.Info("Kafka writer closed successfully")
	}

	if err := m.Reader.Close(); err != nil {
		slog.Error("Failed to close kafka reader", "ERROR", err.Error())
	} else {
		slog.Info("Kafka reader closed successfully")
	}
}
