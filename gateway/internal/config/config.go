package config

import (
	"errors"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env    string   `yaml:"env" env:"ENV"`
	Server Server   `yaml:"server"`
	Kafka  Kafka    `yaml:"kafka"`
	Db     Database `yaml:"database"`
	Metric Metric   `yaml:"metric"`
}

type Server struct {
	Host string `yaml:"host" env:"SERVER_HOST"`
	Port int    `yaml:"port" env:"SERVER_PORT"`
}

type Kafka struct {
	Host string `yaml:"host" env:"KAFKA_HOST"`
	Port int    `yaml:"port" env:"KAFKA_PORT"`
}

type Database struct {
	Username string `yaml:"username" env:"DB_USERNAME"`
	Password string `yaml:"password" env:"DB_PASSWORD"`
	Host     string `yaml:"host" env:"DB_HOST"`
	Port     int    `yaml:"port" env:"DB_PORT"`
	DbName   string `yaml:"db_name" env:"DB_DBNAME"`
}

type Metric struct {
	Host string `yaml:"host" env:"METRIC_HOST"`
	Port int    `yaml:"port" env:"METRIC_PORT"`
}

func LoadConfig() *Config {
	var cfg Config
	if err := cleanenv.ReadConfig("config.yaml", &cfg); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err := cleanenv.ReadEnv(&cfg); err != nil {
				panic("Config is empty & failed to read env:" + err.Error())
			}

		} else {
			panic("Failet to read config" + err.Error())
		}
	}
	return &cfg
}
