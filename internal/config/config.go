package config

import (
	"log"
	"strings"

	"github.com/fogodev/openvvar"
)

type Config struct {
	Environment string `config:"environment;default=DEV;options=DEV, HOMO, PROD, UNK;description=Host environment (DEV, HOMO, PROD or UNK)."`
	Debug       bool   `config:"debug;default=false"`

	MongoHost     string `config:"mongo-host;default=localhost"`
	MongoPort     string `config:"mongo-port;default=27017"`
	MongoDatabase string `config:"mongo-database;default=bezunca"`
	MongoUser     string `config:"mongo-user;default=admin"`
	MongoPassword string `config:"mongo-password;required"`

	QueueHost       string `config:"queue-host;default=localhost"`
	QueuePort       string `config:"queue-port;default=27017"`
	QueueUser       string `config:"queue-user;default=admin"`
	QueuePassword   string `config:"queue-password;required"`
	QueueSelfSigned bool   `config:"queue-self-signed;default=0"`

	InitialB3Year uint `config:"initial-b3-year;default=2015"`
}

func (c *Config) MongoAddress() string {
	return strings.Join([]string{c.MongoHost, c.MongoPort}, ":")
}

func (c *Config) QueueAddress() string {
	return strings.Join([]string{c.QueueHost, c.QueuePort}, ":")
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
