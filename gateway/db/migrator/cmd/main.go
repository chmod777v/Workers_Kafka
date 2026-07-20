package main

import (
	"flag"
	"fmt"
	"migrator/migrator"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

type DBConfig struct {
	Username string
	Password string
	Host     string
	Port     int
	DbName   string
}

func main() {
	//Db config
	dbConfig := DBConfig{
		Username: "postgres",
		Password: "1234",
		Host:     "localhost",
		Port:     56483,
		DbName:   "Tasks",
	}

	storagePathDefault := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		dbConfig.Username, dbConfig.Password, dbConfig.Host, dbConfig.Port, dbConfig.DbName)

	//Flag
	var mode, migrationsDir, storagePath string
	var value int

	flag.StringVar(&mode, "mode", "", "")
	flag.StringVar(&migrationsDir, "dir", "../migrations", "")
	flag.IntVar(&value, "value", 0, "")
	flag.StringVar(&storagePath, "storage-path", storagePathDefault, "")
	flag.Parse()

	storagePath = storagePath + "?sslmode=disable&x-migrations-table=migrationsTable"

	migrationsDir = "file://" + migrationsDir
	if mode == "" {
		panic("mode is required")
	}

	//Migrator
	migrator := migrator.NewMigrator(migrationsDir, storagePath)
	defer migrator.Close()

	switch mode {
	case "Up":
		if !migrator.Up() {
			return
		}
	case "Down":
		if !migrator.Down() {
			return
		}
	case "Steps":
		if value == 0 {
			panic("value is required")
		}
		if !migrator.Steps(value) {
			return
		}
	case "Force":
		if !migrator.Force() {
			return
		}
	case "Status":
		migrator.Status()
		return
	default:
		panic("There is no such regime")
	}

	fmt.Println("migrations applied")
}
