package queue

import (
	"crypto/tls"
	"fmt"

	"github.com/Bezunca/DailyRefreshJob/internal/config"
	"github.com/streadway/amqp"
)

func GetConnectionAndChannel() (*amqp.Connection, *amqp.Channel, error) {
	configs := config.Get()

	conn, err := amqp.DialTLS(
		fmt.Sprintf("amqps://%s:%s@%s/", configs.QueueUser, configs.QueuePassword, configs.QueueAddress()),
		&tls.Config{
			InsecureSkipVerify: configs.QueueSelfSigned,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return conn, ch, nil
}
