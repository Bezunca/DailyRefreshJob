package models

type ScrapingCredentials struct {
	CEI *CEI `json:"cei" bson:"cei"`
}

type Scraping struct {
	ID                  string              `json:"user_id" bson:"user_id"`
	ScrapingCredentials ScrapingCredentials `json:"scraping_credentials" bson:"scraping_credentials"`
}
