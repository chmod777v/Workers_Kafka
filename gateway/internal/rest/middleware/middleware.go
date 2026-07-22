package my_middleware

import (
	"log/slog"
	"net/http"
	"time"
	"workers_kafka_gateway/internal/metric"

	"github.com/go-chi/chi/middleware"
)

func Logger(h http.Handler) http.Handler {
	fn := func(w http.ResponseWriter, r *http.Request) {

		entry := slog.With(
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("request_id", middleware.GetReqID(r.Context())),
			slog.String("remote_addr", r.RemoteAddr),
			//slog.String("user_agent", r.UserAgent()),
		)
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()

		h.ServeHTTP(ww, r)

		entry.Debug("request",
			slog.Int("status", ww.Status()),
			slog.Int("bytes", ww.BytesWritten()),
			slog.String("duration", time.Since(start).String()),
		)

		metric.ObserveRequest(r.Method, time.Since(start), ww.Status())
	}

	return http.HandlerFunc(fn)
}
