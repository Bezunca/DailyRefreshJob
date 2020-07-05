package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"

	"github.com/Bezunca/DailyRefreshJob/internal/database"

	"github.com/Bezunca/mongo_connection"

	"github.com/Bezunca/DailyRefreshJob/internal/config"

	"github.com/robfig/cron/v3"

	b3_assets "github.com/Bezunca/DailyRefreshJob/internal/assets/b3"
	"github.com/Bezunca/DailyRefreshJob/internal/rabbitmq"
	"go.mongodb.org/mongo-driver/mongo"
)

func _main() {
	log.Print("STARTING")

	// ADD OTHER HISTORICAL ASSETS FUNCTION HERE
	AssetsToParse := []func(*mongo.Client) error{
		b3_assets.InsertOldPriceHistory,
		b3_assets.InsertRecentPrices,
	}

	// Loading configs
	configs := config.New()

	caChainBytes, err := ioutil.ReadFile(configs.CAFile)
	if err != nil {
		log.Fatal(err)
	}

	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM(caChainBytes)
	if !ok {
		log.Fatal("unable to parse CA Chain file")
	}

	tlsConfig := &tls.Config{
		RootCAs: roots,
	}

	rabbitMQ, err := rabbitmq.New(&configs.RabbitMQ, tlsConfig)
	if err != nil {
		log.Fatal(err)
	}

	mongoClient, err := mongo_connection.New(&configs.MongoDB, tlsConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer mongo_connection.Close()
	defer rabbitMQ.Close()

	for i := 0; i < len(AssetsToParse); i++ {
		err = AssetsToParse[i](mongoClient)
		if err != nil {
			log.Fatal(err)
		}
	}

	users, err := database.GetUsers(mongoClient)
	if err != nil {
		log.Fatal(err)
	}

	err = b3_assets.SendCEIScrapingRequests(rabbitMQ, users)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("DONE")
}

func main() {
	configs := config.New()
	if configs.CronEnable {
		c := cron.New()
		_, err := c.AddFunc(configs.CronSchedulePattern, func() { _main() })
		if err != nil {
			log.Fatal(err)
		}
		c.Start()
		forever := make(chan bool)
		<-forever
	} else {
		_main()
	}
}
