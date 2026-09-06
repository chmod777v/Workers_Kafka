package my_postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DBAdapter struct {
	pool *pgxpool.Pool
}

func NewDBAdapter(dbLink string) (*DBAdapter, error) {
	ctx, dbCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dbCancel()

	dbpool, err := pgxpool.New(ctx, dbLink)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to the postgreSQL: %s", err.Error())
	}

	adapter := &DBAdapter{pool: dbpool}

	if err := adapter.Ping(); err != nil {
		dbpool.Close()
		return nil, err
	}

	return adapter, nil
}

func (d *DBAdapter) Ping() error {
	retries := 3
	for attempt := 1; attempt <= retries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := d.pool.Ping(ctx)
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

func (d *DBAdapter) Close() {
	if d.pool != nil {
		d.pool.Close()
	}
}

//

func (d *DBAdapter) UpdateTask(message, token string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := d.pool.Exec(ctx,
		"UPDATE tasks SET message=$1 WHERE token = $2", message, token)
	if err != nil {
		return err
	}
	return nil
}

func (d *DBAdapter) CreateTask(ctx context.Context, token string) error {
	_, err := d.pool.Exec(ctx,
		"INSERT INTO tasks (token, message) VALUES ($1, '')", token)
	if err != nil {
		return err
	}
	return nil
}

func (d *DBAdapter) GetTask(ctx context.Context, token string) (string, error) {
	var message string
	err := d.pool.QueryRow(ctx,
		"SELECT message FROM tasks WHERE token=$1", token).Scan(&message)
	if err != nil {
		return "", err
	}
	return message, nil
}
