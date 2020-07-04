package database

import (
	"context"
	"fmt"
	"time"

	"github.com/Bezunca/DailyRefreshJob/internal/config"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func GetConnection() (*mongo.Client, error) {
	configs := config.Get()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mongoClient, err := mongo.Connect(
		ctx, options.Client().ApplyURI(
			fmt.Sprintf(
				"mongodb://%s:%s@%s/?appname=Daily%%20Refresh%%20Job",
				configs.MongoUser,
				configs.MongoPassword,
				configs.MongoAddress(),
			),
		),
	)

	if err != nil {
		return nil, err
	}

	return mongoClient, nil
}
