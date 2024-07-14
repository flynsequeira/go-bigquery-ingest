package ingester

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func getUSDRate(symbolID string, date string) (float64, error) {
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd&date=%s", symbolID, date)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("http.NewRequest: %w", err)
	}

	req.Header.Set("accept", "application/json")
	req.Header.Set("x-cg-api-key", "API_KEY	")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("client.Do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status: %s", resp.Status)
	}
	log.Printf("API successfully requested | Symbol-Date: %s - %s", symbolID, date)

	var result map[string]map[string]float64
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, fmt.Errorf("json.NewDecoder: %w", err)
	}

	usdRate, ok := result[symbolID]["usd"]
	if !ok {
		return 0, fmt.Errorf("usd rate not found for symbol: %s", symbolID)
	}

	return usdRate, nil
}
