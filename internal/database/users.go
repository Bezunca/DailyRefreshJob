package database

import (
	"context"
	"log"
	"time"

	"github.com/Bezunca/DailyRefreshJob/internal/parsers"

	"github.com/Bezunca/DailyRefreshJob/internal/config"
	"github.com/Bezunca/DailyRefreshJob/internal/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var userDecodeError = func() { log.Println("WARN: Cant decode data for an user") }

func GetUsers(mongoClient *mongo.Client) ([]models.Scraping, error) {
	configs := config.Get()
	usersCollection := mongoClient.Database(configs.MongoDatabase).Collection("users")

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	cur, err := usersCollection.Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{{"name", 0}}))
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var users []models.Scraping

	for cur.Next(ctx) {
		var data map[string]interface{}
		err := cur.Decode(&data)
		if err != nil {
			log.Println("WARN: Cant decode data for an user")
		}

		id, ok := parsers.ParseID(data)
		if !ok {
			userDecodeError()
		}

		users = append(users, models.Scraping{
			ID: id,
			ScrapingCredentials: models.ScrapingCredentials{
				CEI: *parsers.ParseCEI(data),
			},
		})
	}
	return users, nil
}
