package config

import (
	"errors"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env    string `yaml:"env" env:"ENV"`
	Kafka  Kafka  `yaml:"kafka"`
	Metric Metric `yaml:"metric"`
}

type Kafka struct {
	Host string `yaml:"host" env:"KAFKA_HOST"`
	Port int    `yaml:"port" env:"KAFKA_PORT"`
}

type Metric struct {
	Host string `yaml:"host" env:"METRIC_HOST"`
	Port int    `yaml:"port" env:"METRIC_PORT"`
}

func LoadConfig() *Config {
	var cfg Config
	if err := cleanenv.ReadConfig("config-local.yaml", &cfg); err != nil {
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
