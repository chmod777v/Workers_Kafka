package worker

import (
	"log/slog"
	"math/rand"
	"time"
	"workers_kafka_worker/internal/metric"
)

var message = []string{"Fuck, you're weak!", "Your mother is a whore!", "Your father is a condom!", "Your grandma is a prostitute!", "Your granny is a hooker!"}

func Worker(id string) string { //Имитация работы
	start := time.Now()
	slog.Debug("", "Message", id, "Status", "Processing")

	time.Sleep(time.Second * time.Duration(rand.Intn(7)+3))
	msg := message[rand.Intn(5)]

	slog.Debug("", "Message", id, "Status", "Finished")
	metric.ObserveRequest(time.Since(start))
	return msg
}
