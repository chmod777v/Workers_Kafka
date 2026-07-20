package my_kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"
	"workers_kafka_worker/internal/worker"

	"github.com/segmentio/kafka-go"
)

type Data struct {
	Token   string
	Message string
}

type Listener struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	reader *kafka.Reader
	writer *kafka.Writer
}

func NewListener(reader *kafka.Reader, writer *kafka.Writer) Listener {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	return Listener{
		ctx:    ctx,
		cancel: cancel,
		wg:     wg,
		reader: reader,
		writer: writer,
	}
}

func (l *Listener) Listening() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		for {
			if l.ctx.Err() != nil {
				return
			}

			// Kafka read
			msg, err := l.reader.ReadMessage(l.ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				slog.Error("Listening err, Failed read message", "ERROR", err.Error())
				continue
			}
			token := string(msg.Value)
			message := worker.Worker(token)

			// Kafka write
			data := Data{
				Token:   token,
				Message: message,
			}
			value, err := json.Marshal(data)
			if err != nil {
				slog.Error("Listening err, Marshal", "ERROR", err.Error())
				continue
			}

			writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer writeCancel()
			err = l.writer.WriteMessages(writeCtx, kafka.Message{
				Value: value,
			})
			if err != nil {
				slog.Error("Listening err, Kafka", "ERROR", err.Error())
				continue
			}
		}
	}()
}

func (l *Listener) Close() {
	l.cancel()
	l.wg.Wait()
}
