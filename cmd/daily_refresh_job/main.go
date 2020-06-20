package main

import (
	"fmt"
	"log"

	"github.com/Bezunca/DailyRefreshJob/internal/assets"

	"github.com/Bezunca/DailyRefreshJob/internal/queue"

	"github.com/Bezunca/DailyRefreshJob/internal/config"

	"github.com/Bezunca/DailyRefreshJob/internal/database"
	_ "github.com/streadway/amqp"
)

func main() {
	config.New()

	mongoClient, err := database.GetConnection()
	if err != nil {
		log.Fatal(err)
	}

	users, err := database.GetUsers(mongoClient)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(users)

	queueConn, queueCh, err := queue.GetConnectionAndChannel()
	if err != nil {
		log.Fatal(err)
	}
	defer queueConn.Close()
	defer queueCh.Close()

	err = assets.SendCEIScrapingRequests(queueCh, users)
	if err != nil {
		log.Fatal(err)
	}
}
