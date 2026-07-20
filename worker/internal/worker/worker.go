package worker

import (
	"log/slog"
	"math/rand"
	"time"
)

var message = []string{"Fuck, you're weak!", "Your mother is a whore!", "Your father is a condom!", "Your grandma is a prostitute!", "Your granny is a hooker!"}

func Worker(id string) string { //Имитация работы
	slog.Debug("", "Message", id, "Status", "Processing")

	time.Sleep(time.Second * 5)
	msg := message[rand.Intn(5)]

	slog.Debug("", "Message", id, "Status", "Finished")
	return msg
}
