package assets_b3

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/Bezunca/DailyRefreshJob/internal/database"

	"github.com/Bezunca/DailyRefreshJob/internal/rabbitmq"

	"github.com/Bezunca/DailyRefreshJob/internal/config"
	"github.com/Bezunca/b3lib/history"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/Bezunca/DailyRefreshJob/internal/models"
)

func convertPricesToMongoFormat(b3Assets map[string]*history.Asset, b3Prices []history.Price) (map[string]interface{}, []interface{}) {
	mongoB3Prices := make([]interface{}, len(b3Prices))
	for i, value := range b3Prices {
		mongoB3Prices[i] = value
	}
	mongoB3Assets := make(map[string]interface{})
	for key, value := range b3Assets {
		mongoB3Assets[key] = value
	}
	return mongoB3Assets, mongoB3Prices
}

func updateAssets(mongoClient *mongo.Client, mongoB3Assets map[string]interface{}) error {
	log.Print("Updating assets info")
	if len(mongoB3Assets) > 0 {
		configs := config.Get()

		assetModels := make([]mongo.WriteModel, 0, len(mongoB3Assets))
		for key, asset := range mongoB3Assets {
			mongoAsset, err := database.ToDoc(asset)
			if err != nil {
				return err
			}

			assetModels = append(
				assetModels,
				mongo.NewUpdateOneModel().SetFilter(
					bson.D{{Key: "ticker", Value: key}},
				).SetUpdate(mongoAsset).SetUpsert(true),
			)
		}

		assetsCollection := mongoClient.Database(configs.ApplicationDatabase).Collection("b3_assets")
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(len(mongoB3Assets))*10*time.Second)
		defer cancel()
		opts := options.BulkWrite().SetOrdered(false)
		_, err := assetsCollection.BulkWrite(ctx, assetModels, opts)
		if err != nil {
			log.Printf("WARN: Cannot update assets data")
		}
	}

	return nil
}

func InsertOldPriceHistory(mongoClient *mongo.Client) error {
	log.Print("Populating B3 Historical Prices")

	configs := config.Get()
	pricesCollection := mongoClient.Database(configs.ApplicationDatabase).Collection("historical_prices")

	currentYear := uint(time.Now().Year())

	var mongoB3Assets map[string]interface{}
	var mongoB3Prices []interface{}

	for i := configs.InitialB3Year; i <= currentYear; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var result interface{}
		err := pricesCollection.FindOne(ctx, bson.D{{Key: "year", Value: i}}).Decode(&result)
		if err != nil {
			log.Printf("Inserting %d in database", i)
		} else {
			log.Printf("%d year is already in database", i)
			continue
		}

		b3Assets, b3Prices, err := history.GetByYear(i)
		if err != nil {
			return err
		}

		mongoB3Assets, mongoB3Prices = convertPricesToMongoFormat(b3Assets, b3Prices)
		_, err = pricesCollection.InsertMany(context.Background(), mongoB3Prices)
		if err != nil {
			return err
		}
	}

	err := updateAssets(mongoClient, mongoB3Assets)
	if err != nil {
		return err
	}

	return nil
}

func InsertRecentPrices(mongoClient *mongo.Client) error {
	log.Print("Populating B3 Recent Prices")
	configs := config.Get()

	pricesCollection := mongoClient.Database(
		configs.ApplicationDatabase,
	).Collection("historical_prices")
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

	var dataComplement []history.Price
	var b3Assets map[string]*history.Asset

	currentDate := time.Now()
	for i := lastUpdateDate.Add(time.Hour * 24); i.Year() <= currentDate.Year() && i.Month() != currentDate.Month() && i.Day() != currentDate.Day(); i = i.Add(time.Hour * 24) {
		if i.Weekday() == time.Saturday || i.Weekday() == time.Sunday {
			continue
		}

		log.Printf("Updating day %s", i)

		_b3Assets, b3Prices, err := history.GetSpecificDay(uint(i.Day()), uint(i.Month()), uint(i.Year()))
		b3Assets = _b3Assets

		if err != nil {
			return err
		}
		dataComplement = append(dataComplement, b3Prices...)
	}
	if len(dataComplement) > 0 {
		log.Print("Writing recent prices to database")
		mongoB3Assets, mongoB3Prices := convertPricesToMongoFormat(b3Assets, dataComplement)
		_, err = pricesCollection.InsertMany(context.Background(), mongoB3Prices)
		if err != nil {
			return err
		}

		err := updateAssets(mongoClient, mongoB3Assets)
		if err != nil {
			return err
		}
	} else {
		log.Print("No new data to write")
	}

	return nil
}

func SendCEIScrapingRequests(rabbitMQ *rabbitmq.Session, userScrapingRequests []models.Scraping) error {
	for i := 0; i < len(userScrapingRequests); i++ {
		scrappingRequest := userScrapingRequests[i]

		if scrappingRequest.ScrapingCredentials.CEI != nil {
			request, err := json.Marshal(scrappingRequest)
			if err != nil {
				return err
			}

			err = rabbitMQ.Push(request)
			if err != nil {
				log.Printf(
					"WARN: Error when sending message to rabbitmq 'CEI' on user: %s", scrappingRequest.ID,
				)
			}
		} else {
			log.Printf("WARN: No configuration for CEI on user: %s", scrappingRequest.ID)
		}
	}
	return nil
}
