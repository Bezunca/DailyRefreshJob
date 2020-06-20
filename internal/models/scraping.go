package models

type ScrapingCredentials struct {
	CEI CEI
}

type Scraping struct {
	ID                  []uint8
	ScrapingCredentials ScrapingCredentials
}
