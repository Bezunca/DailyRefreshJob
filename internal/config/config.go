package config

import (
	"log"

	"github.com/Bezunca/mongo_connection/config"

	"github.com/fogodev/openvvar"
)

type Databases struct {
	HistoricalPrices string
}

type Config struct {
	Environment string `config:"environment;default=DEV;options=DEV, HOMO, PROD, UNK;description=Host environment (DEV, HOMO, PROD or UNK)."`
	Debug       bool   `config:"debug;default=false"`
	CAFile      string `config:"ca-file;required"`

	MongoDB             config.MongoConfigs
	ApplicationDatabase string `config:"application-database;required"`

	RabbitMQ RabbitMQConfig

	InitialB3Year uint `config:"initial-b3-year;default=2015"`

	CronEnable          bool   `config:"cron-enable;default=true"`
	CronSchedulePattern string `config:"cron-schedule-pattern;default=0 1 * * *"`
}

var globalConfig *Config = nil

func New() *Config {
	if globalConfig == nil {
		globalConfig = new(Config)
		if err := openvvar.Load(globalConfig); err != nil {
			log.Fatalf("An error occurred for bad config reasons: %v", err)
		}
	}

	return globalConfig
}

func Get() *Config {
	if globalConfig == nil {
		panic("Trying to get a nil config, you must use New function to instantiate configs before getting it")
	}
	return globalConfig
}
