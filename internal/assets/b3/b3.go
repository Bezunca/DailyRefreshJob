package assets_b3

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Bezunca/B3History/pkg/b3"
	b3Models "github.com/Bezunca/B3History/pkg/models"
	"github.com/Bezunca/DailyRefreshJob/internal/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/streadway/amqp"

	"github.com/Bezunca/DailyRefreshJob/internal/models"
)

func convertPricesToMongoFormat(b3Data []b3Models.AssetInfo) []interface{} {
	mongoB3Data := make([]interface{}, len(b3Data))
	for i, value := range b3Data {
		mongoB3Data[i] = value
	}
	return mongoB3Data
}

func InsertOldPriceHistory(mongoClient *mongo.Client) error {
	log.Print("Populating B3 Historical Prices")

	configs := config.Get()
	pricesCollection := mongoClient.Database(configs.MongoDatabase).Collection("historical_prices")

	currentYear := uint(time.Now().Year())
	for i := configs.InitialB3Year; i <= currentYear; i++ {
		ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
		var result interface{}
		err := pricesCollection.FindOne(ctx, bson.D{{"year", i}}).Decode(&result)
		if err != nil {
			log.Printf("Inserting %d in database", i)
		} else {
			log.Printf("%d year is already in database", i)
			continue
		}

		b3Data, err := b3.GetHistory(i)
		if err != nil {
			return err
		}

		_, err = pricesCollection.InsertMany(context.Background(), convertPricesToMongoFormat(b3Data))
		if err != nil {
			return err
		}
	}

	return nil
}

func SendCEIScrapingRequests(queueCh *amqp.Channel, userScrapingRequests []models.Scraping) error {
	queue, err := queueCh.QueueDeclare("CEI",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for i := 0; i < len(userScrapingRequests); i++ {
		scrappingRequest := userScrapingRequests[i]

		if scrappingRequest.ScrapingCredentials.CEI != nil {
			request, err := json.Marshal(scrappingRequest)
			if err != nil {
				return err
			}

			err = queueCh.Publish(
				"",         // exchange
				queue.Name, // routing key
				true,       // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType: "application/json",
					Body:        request,
				},
			)
			if err != nil {
				log.Printf(
					"WARN: Error when sending message to queue '%s' on user: %s", queue.Name, scrappingRequest.ID,
				)
			}
		} else {
			log.Printf("WARN: No configuration for CEI on user: %s", scrappingRequest.ID)
		}
	}
	return nil
}
