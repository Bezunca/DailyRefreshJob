package parsers

import (
	"github.com/Bezunca/DailyRefreshJob/internal/models"
)

func ParseCEI(data map[string]interface{}) *models.CEI {
	scrapingCredentials, ok := ParseBaseScraping(data)
	if !ok {
		return nil
	}

	_ceiData, ok := scrapingCredentials["cei"]
	if !ok {
		return nil
	}
	ceiData, ok := _ceiData.(map[string]interface{})
	if !ok {
		return nil
	}

	_username, ok := ceiData["user"]
	if !ok {
		return nil
	}
	username, ok := _username.(string)
	if !ok {
		return nil
	}

	_password, ok := ceiData["password"]
	if !ok {
		return nil
	}
	password, ok := _password.(string)
	if !ok {
		return nil
	}

	return &models.CEI{
		User:     username,
		Password: password,
	}
}
