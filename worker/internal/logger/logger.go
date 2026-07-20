package logger

import (
	"log/slog"
	"os"

	"github.com/lmittmann/tint"
)

func InitLogger(env string) {
	var handler slog.Handler
	switch env {
	case "development":
		handler = tint.NewHandler(os.Stdout, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: "15:04:05",
			NoColor:    false,
		})
	case "production":
		handler = tint.NewHandler(os.Stdout, &tint.Options{
			Level:      slog.LevelInfo,
			TimeFormat: "15:04:05",
			NoColor:    false,
		})
	}
	slog.SetDefault(slog.New(handler))
}
