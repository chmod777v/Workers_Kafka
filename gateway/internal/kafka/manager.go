package my_kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Manager struct {
	Writer *kafka.Writer
	Reader *kafka.Reader
}

func NewManager(kafkaAddr string) (*Manager, error) {
	//Ping
	kafkaCtx, kafkaCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer kafkaCancel()

	conn, err := kafka.DialContext(kafkaCtx, "tcp", kafkaAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	//Writer
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

	//Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Topic:   "result",
		Brokers: []string{kafkaAddr},
		GroupID: "gateway",
	})

	return &Manager{
		Writer: writer,
		Reader: reader,
	}, nil
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
