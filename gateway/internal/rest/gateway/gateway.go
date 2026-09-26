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
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type Database interface {
	CreateTask(ctx context.Context, token string) error
	GetTask(ctx context.Context, token string) (string, error)
}

type Cache interface {
	AddTask(ctx context.Context, token, message string) error
	GetTask(ctx context.Context, token string) (string, error)
}

type Gateway struct {
	server *http.Server
}

func StartGateway(addr string, errChan chan error, writer *kafka.Writer, db Database, cache Cache) *Gateway {
	router := newRouter(db, cache, writer)

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

func handlerPost(db Database, writer *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Generate id
		bytesToken := make([]byte, 5)
		if _, err := rand.Read(bytesToken); err != nil {
			return
		}
		token := base64.URLEncoding.EncodeToString(bytesToken)[:5]

		// BD
		if err := db.CreateTask(r.Context(), token); err != nil {
			slog.Error("HandlerPost error, BD", "ERROR", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Kafka
		err := writer.WriteMessages(r.Context(), kafka.Message{
			Value: []byte(token),
		})
		if err != nil {
			slog.Error("HandlerPost error, Kafka", "ERROR", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(token))
	}
}
func handlerGet(db Database, cache Cache) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request struct{ Token string }
		if err := render.DecodeJSON(r.Body, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			slog.Error("HandlerGet err, render.DecodeJSON", "ERROR", err)
			return
		}
		//Redis
		message, err := cache.GetTask(r.Context(), request.Token)

		if err == nil {
			if message == "" {
				w.Write([]byte("In progress"))
				return
			}
			w.Write([]byte(message))
			return
		}

		if !errors.Is(err, redis.Nil) {
			slog.Error("HandlerGet error, Cache GetTask", "ERROR", err)
		}

		// BD
		message, err = db.GetTask(r.Context(), request.Token)

		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			slog.Error("HandlerGet error, BD", "ERROR", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if message == "" {
			w.Write([]byte("In progress"))
			return
		}
		w.Write([]byte(message))

		//REDIS
		if err = cache.AddTask(r.Context(), request.Token, message); err != nil {
			slog.Error("HandlerGet error, Cache AddTask", "ERROR", err)
		}

	}
}
func newRouter(db Database, cache Cache, writer *kafka.Writer) *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.Recoverer) //Для перехвата паник
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(my_middleware.Logger)

	router.Post("/", handlerPost(db, writer))
	router.Get("/", handlerGet(db, cache))
	return router
}
