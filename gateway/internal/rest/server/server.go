package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	my_middleware "workers_kafka_gateway/internal/rest/middleware"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

func handlerPost(writer *kafka.Writer, dbpool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Generate id
		bytesToken := make([]byte, 5)
		if _, err := rand.Read(bytesToken); err != nil {
			return
		}
		token := base64.URLEncoding.EncodeToString(bytesToken)[:5]

		// BD
		_, err := dbpool.Exec(context.Background(),
			"INSERT INTO tasks (token, message) VALUES ($1, '')", token)
		if err != nil {
			slog.Error("HandlerPost error, BD", "ERROR", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// Kafka
		err = writer.WriteMessages(context.Background(), kafka.Message{
			Value: []byte(token),
		})
		if err != nil {
			slog.Error("HandlerPost error, Kafka", "ERROR", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(token))
	}
}
func handlerGet(dbpool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Token string }
		if err := render.DecodeJSON(r.Body, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			slog.Error("HandlerGet err, render.DecodeJSON", "ERROR", err.Error())
			return
		}
		// BD
		var message string
		err := dbpool.QueryRow(context.Background(),
			"SELECT message FROM tasks WHERE token=$1", request.Token).Scan(&message)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			slog.Error("HandlerGet error, BD", "ERROR", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if message == "" {
			w.Write([]byte("In progress"))
			return
		}
		w.Write([]byte(message))
	}
}
func NewRouter(writer *kafka.Writer, dbpool *pgxpool.Pool) *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.Recoverer) //Для перехвата паник
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(my_middleware.Logger)

	router.Post("/", handlerPost(writer, dbpool))
	router.Get("/", handlerGet(dbpool))
	return router
}

func Shutdown(serverServ *http.Server, serverErr chan error, metricServ *http.Server, metricErr chan error) {
	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
		slog.Info("Shutdown")
	case err := <-serverErr:
		slog.Error("Server error", "ERROR", err)
	case err := <-metricErr:
		slog.Error("Metric error", "ERROR", err)
	}

	//SHUTDOWN
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := serverServ.Shutdown(ctx); err != nil {
		slog.Error("Failed to stop server", "ERROR:", err.Error())
		serverServ.Close()
	} else {
		slog.Info("Server stopped successfully")
	}

	if err := metricServ.Shutdown(ctx); err != nil {
		slog.Error("Failed to stop metric", "ERROR:", err.Error())
		metricServ.Close()
	} else {
		slog.Info("Metric stopped successfully")
	}
}
