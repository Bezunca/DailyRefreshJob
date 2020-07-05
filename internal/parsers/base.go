package parsers

import (
	"encoding/hex"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

func ParseBaseScraping(data map[string]interface{}) (map[string]interface{}, bool) {
	_scrapingCredentials, ok := data["scraping_credentials"]
	if !ok {
		return nil, false
	}
	scrapingCredentials, ok := _scrapingCredentials.(map[string]interface{})
	if !ok {
		return nil, false
	}

	return scrapingCredentials, true
}

func ParseID(data map[string]interface{}) (string, bool) {
	_rawId, ok := data["_id"]
	if !ok {
		return "", false
	}

	rawId, ok := _rawId.(primitive.ObjectID)
	if !ok {
		return "", false
	}

	id := make([]uint8, len(rawId))
	for i := 0; i < len(id); i++ {
		id[i] = rawId[i]
	}

	hexID := make([]byte, hex.EncodedLen(len(id)))
	hex.Encode(hexID, id)

	return string(hexID), true
}
