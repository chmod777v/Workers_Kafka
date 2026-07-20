package config

import (
	"flag"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env    string   `yaml:"env"`
	Server Server   `yaml:"server"`
	Db     Database `yaml:"database"`
	Kafka  Kafka    `yaml:"kafka"`
}

type Kafka struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type Database struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DbName   string `yaml:"db_name"`
}

type Server struct {
	Port int    `yaml:"port"`
	Host string `yaml:"host"`
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
