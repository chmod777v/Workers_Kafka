package my_kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	my_postgres "workers_kafka_gateway/internal/postgres"

	"github.com/segmentio/kafka-go"
)

type Data struct {
	Token   string
	Message string
}

type Listener struct {
	ctx       context.Context
	cancel    context.CancelFunc
	wg        *sync.WaitGroup
	reader    *kafka.Reader
	dbAdapter *my_postgres.DBAdapter
}

func NewListener(reader *kafka.Reader, dbAdapter *my_postgres.DBAdapter) Listener {
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	return Listener{
		ctx:       ctx,
		cancel:    cancel,
		wg:        wg,
		reader:    reader,
		dbAdapter: dbAdapter,
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
				slog.Error("Listening err, read message", "ERROR", err.Error())
				continue
			}
			var data Data
			if err := json.Unmarshal(msg.Value, &data); err != nil {
				slog.Error("Listening err, unmarshal message", "ERROR", err.Error())
				continue
			}
			slog.Debug("", "Token", data.Token, "Message", data.Message)

			//DB
			if err := l.dbAdapter.UpdateTask(data.Message, data.Token); err != nil {
				slog.Error("Listening error, BD", "ERROR", err.Error())
			}
		}
	}()
}
func (l *Listener) Close() {
	l.cancel()
	l.wg.Wait()
}
