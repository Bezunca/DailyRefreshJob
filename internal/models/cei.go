package models

type CEI struct {
	User     string `json:"user" bson:"user"`
	Password string `json:"password" bson:"password"`
}
