package assets

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"

	"github.com/Bezunca/DailyRefreshJob/internal/models"
)

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
