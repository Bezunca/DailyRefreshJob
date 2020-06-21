package main

import (
	"log"

	b3_assets "github.com/Bezunca/DailyRefreshJob/internal/assets/b3"
	"github.com/Bezunca/DailyRefreshJob/internal/config"
	"github.com/Bezunca/DailyRefreshJob/internal/database"
	"github.com/Bezunca/DailyRefreshJob/internal/queue"
	"go.mongodb.org/mongo-driver/mongo"
)

func main() {
	log.Print("STARTING")
	config.New()

	// ADD OTHER HISTORICAL ASSETS FUNCTION HERE
	AssetsToParse := []func(*mongo.Client) error{
		b3_assets.InsertOldPriceHistory,
		b3_assets.InsertRecentPrices,
	}

	mongoClient, err := database.GetConnection()
	if err != nil {
		log.Fatal(err)
	}

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

	queueConn, queueCh, err := queue.GetConnectionAndChannel()
	if err != nil {
		log.Fatal(err)
	}
	defer queueConn.Close()
	defer queueCh.Close()

	err = b3_assets.SendCEIScrapingRequests(queueCh, users)
	if err != nil {
		log.Fatal(err)
	}

	log.Print("DONE")
}
