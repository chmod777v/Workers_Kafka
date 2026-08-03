package my_postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(dbLink string) (*pgxpool.Pool, error) {
	ctx, dbCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dbCancel()
	dbpool, err := pgxpool.New(ctx, dbLink)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to the postgreSQL: %s", err.Error())
	}

	if err := Ping(dbpool); err != nil {
		dbpool.Close()
		return nil, err
	}

	return dbpool, nil
}

func Ping(dbpool *pgxpool.Pool) error {
	retries := 3
	for attempt := 1; attempt <= retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := dbpool.Ping(ctx)
		cancel()

		if err == nil {
			return nil
		}
		if attempt == retries {
			return fmt.Errorf("Failed ping postgreSQL: %s", err.Error())
		}
		time.Sleep(3 * time.Second)
	}
	return nil
}
