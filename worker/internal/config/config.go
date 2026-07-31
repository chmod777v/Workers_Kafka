package config

import (
	"errors"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env    string `yaml:"env" env:"ENV"`
	Kafka  Kafka  `yaml:"kafka"`
	Health Health `yaml:"health"`
}

type Kafka struct {
	Host string `yaml:"host" env:"KAFKA_HOST"`
	Port int    `yaml:"port" env:"KAFKA_PORT"`
}

type Health struct {
	Host string `yaml:"host" env:"HEALTH_HOST"`
	Port int    `yaml:"port" env:"HEALTH_PORT"`
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
