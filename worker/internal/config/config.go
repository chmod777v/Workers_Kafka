package config

import (
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env   string `yaml:"env"`
	Kafka Kafka  `yaml:"kafka"`
}

type Kafka struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func LoadConfig() *Config {
	var path string
	flag.StringVar(&path, "config", "", "path") //"config" - имя флага (--config) "path" - описание для справки
	flag.Parse()
	if path == "" {
		path = os.Getenv("CONFIG_PATH")
	}
	if path == "" {
		panic("CONFIG_PATH is empty")
	}
	if _, err := os.Stat(path); os.IsNotExist(err) { //os.Stat- - Проверка существует ли файл, os.IsNotExist-если нет то
		panic("Config file does not exist: " + path)
	}
	var cfg Config
	if err := cleanenv.ReadConfig(path, &cfg); err != nil {
		panic("Failet to read config" + err.Error())
	}
	return &cfg
}
