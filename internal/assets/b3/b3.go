package assets_b3

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	b3_http "github.com/Bezunca/B3History/pkg/http"
	b3_models "github.com/Bezunca/B3History/pkg/models"
	b3_parser "github.com/Bezunca/B3History/pkg/parser"
	zipMemory "github.com/Bezunca/ZipInMemory/pkg/zip"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"

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

func InsertRecentPrices(mongoClient *mongo.Client) error {
	log.Print("Populating B3 Recent Prices")
	configs := config.Get()

	pricesCollection := mongoClient.Database(configs.MongoDatabase).Collection("historical_prices")
	var data map[string]interface{}
	err := pricesCollection.FindOne(
		context.TODO(), bson.D{}, options.FindOne().SetSort(map[string]int{"date": -1}),
	).Decode(&data)
	if err != nil {
		return err
	}
	_date, ok := data["date"]
	if !ok {
		return fmt.Errorf("invalid date on dataset")
	}
	pDate, ok := _date.(primitive.DateTime)
	if !ok {
		return fmt.Errorf("invalid date on dataset")
	}
	lastUpdateDate := pDate.Time()

	var dataComplement []b3_models.AssetInfo
	currentDate := time.Now()
	for i := lastUpdateDate.Add(time.Hour * 24); i.Year() <= currentDate.Year() && i.Month() <= currentDate.Month() && i.Day() <= currentDate.Day(); i = i.Add(time.Hour * 24) {
		if i.Weekday() == time.Saturday || i.Weekday() == time.Sunday {
			continue
		}

		day := fmt.Sprintf("%02d", i.Day())
		mounth := fmt.Sprintf("%02d", i.Month())
		year := fmt.Sprintf("%d", i.Year())
		log.Printf("Scraping data %s/%s/%s", day, mounth, year)

		url := fmt.Sprintf("http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D%s%s%s.ZIP",
			day, mounth, year,
		)
		zipBytes, err := b3_http.Download(url) // TODO: Technical debt - move to B3 lib
		if err != nil {
			return err
		}
		zipFile, err := zipMemory.ExtractInMemory(zipBytes)
		if err != nil {
			return err
		}
		dayData, err := b3_parser.ParseHistoricDataFromBytes(zipFile, i.Year())
		if err != nil {
			return err
		}
		dataComplement = append(dataComplement, dayData...)
	}
	if len(dataComplement) > 0 {
		log.Print("Writing recent prices to database")
		_, err = pricesCollection.InsertMany(context.Background(), convertPricesToMongoFormat(dataComplement))
		if err != nil {
			return err
		}
	} else {
		log.Print("No new data to write")
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
