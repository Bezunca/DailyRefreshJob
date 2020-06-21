package main

import (
	"log"

	"go.mongodb.org/mongo-driver/mongo"

	assets_b3 "github.com/Bezunca/DailyRefreshJob/internal/assets/b3"

	"github.com/Bezunca/DailyRefreshJob/internal/config"
	"github.com/Bezunca/DailyRefreshJob/internal/database"
	"github.com/Bezunca/DailyRefreshJob/internal/queue"
)

func main() {
	config.New()

	// ADD OTHER HISTORICAL ASSETS FUNCTION HERE
	historicalAssetsToParse := []func(*mongo.Client) error{
		assets_b3.InsertOldPriceHistory,
	}

	mongoClient, err := database.GetConnection()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(historicalAssetsToParse); i++ {
		err = historicalAssetsToParse[i](mongoClient)
		if err != nil {
			log.Fatal(err)
		}
	}

	// TODO: GET CURRENT YEAR PRICES HERE
	// EXAMPLE URL: http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D14062020.ZIP

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

	err = assets_b3.SendCEIScrapingRequests(queueCh, users)
	if err != nil {
		log.Fatal(err)
	}
}
