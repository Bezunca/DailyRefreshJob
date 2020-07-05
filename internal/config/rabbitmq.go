package config

import (
	"fmt"
	"time"
)

type RabbitMQConfig struct {
	CEIQueue       string        `config:"cei-queue;required"`
	User           string        `config:"user;required"`
	Password       string        `config:"password;required"`
	Host           string        `config:"host;required"`
	AMQPPort       int           `config:"amqpport;required"`
	VHost          string        `config:"vhost;"`
	ReconnectDelay time.Duration `config:"reconnect-delay;default=2s"`
	RestartDelay   time.Duration `config:"restart-delay;default=1s"`
	ResendDelay    time.Duration `config:"resend-delay;default=1s"`
}

// FormatRabbitMQURL returns a RabbitMQ connection url based on received configs
func (c *RabbitMQConfig) FormatRabbitMQURL() string {
	return fmt.Sprintf("amqps://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.AMQPPort, c.VHost)
}
