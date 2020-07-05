package models

type WalletsCredentials struct {
	CEI *CEI `json:"cei" bson:"cei"`
}

type Scraping struct {
	ID                 string             `json:"user_id" bson:"user_id"`
	WalletsCredentials WalletsCredentials `json:"wallets_credentials" bson:"wallets_credentials"`
}
