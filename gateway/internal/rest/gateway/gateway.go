package gateway

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"log/slog"
	"net/http"
	my_middleware "workers_kafka_gateway/internal/rest/middleware"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Gateway struct {
	server *http.Server
}

func StartGateway(addr string, errChan chan error, writer *kafka.Writer, dbpool *pgxpool.Pool) *Gateway {
	router := newRouter(writer, dbpool)

	server := &http.Server{
		Addr:    addr,
		Handler: router,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Gateway error", "ERROR", err)
			errChan <- err
		}
	}()

	return &Gateway{
		server: server,
	}
}
func (g *Gateway) Close(ctx context.Context) error {
	if err := g.server.Shutdown(ctx); err != nil {
		g.server.Close()
		return err
	}
	return nil
}

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
func newRouter(writer *kafka.Writer, dbpool *pgxpool.Pool) *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.Recoverer) //Для перехвата паник
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(my_middleware.Logger)

	router.Post("/", handlerPost(writer, dbpool))
	router.Get("/", handlerGet(dbpool))
	return router
}
