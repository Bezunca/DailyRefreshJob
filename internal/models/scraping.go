package models

type ScrapingCredentials struct {
	CEI *CEI `json:"cei"`
}

type Scraping struct {
	ID                  string              `json:"id"`
	ScrapingCredentials ScrapingCredentials `json:"scraping_credentials"`
}
