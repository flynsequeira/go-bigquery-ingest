// Package helloworld provides a set of Cloud Functions samples.
package ingester

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/cloudevents/sdk-go/v2/event"
)

func init() {
	functions.CloudEvent("TransformCSV", TransformCSV)
}

// StorageObjectData contains metadata of the Cloud Storage object.
type StorageObjectData struct {
	Bucket         string    `json:"bucket,omitempty"`
	Name           string    `json:"name,omitempty"`
	Metageneration int64     `json:"metageneration,string,omitempty"`
	TimeCreated    time.Time `json:"timeCreated,omitempty"`
	Updated        time.Time `json:"updated,omitempty"`
}

// Props struct to unmarshal the props field
type Props struct {
	CurrencySymbol string `json:"currencySymbol"`
}

// Nums struct to unmarshal the nums field
type Nums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
}

// TransformedRecord represents the transformed data structure
type TransformedRecord struct {
	Date             string
	ProjectID        string
	NumTransactions  int
	TotalVolumeInUSD float64
}

// TransformCSV consumes a CloudEvent message and processes the CSV file.
func TransformCSV(ctx context.Context, e event.Event) error {
	log.Printf("Event ID: %s", e.ID())
	log.Printf("Event Type: %s", e.Type())

	var data StorageObjectData
	if err := e.DataAs(&data); err != nil {
		return fmt.Errorf("event.DataAs: %v", err)
	}

	log.Printf("Bucket: %s", data.Bucket)
	log.Printf("File: %s", data.Name)

	// GCP Storage setup
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer client.Close()

	inputBucket := client.Bucket(data.Bucket)
	inputObj := inputBucket.Object(data.Name)
	r, err := inputObj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %w", data.Name, err)
	}
	defer r.Close()

	// Load symbol_id_map.json
	symbolMap, err := loadSymbolMap()
	if err != nil {
		return fmt.Errorf("Failed to load symbol map: %w", err)
	}

	// Output setup (change as needed)
	outputBucketName := "blockdata-output" // Or use a dynamic name
	outputFileName := fmt.Sprintf("transformed_%s", data.Name)
	outputBucket := client.Bucket(outputBucketName)
	outputObj := outputBucket.Object(outputFileName)
	w := outputObj.NewWriter(ctx)
	defer w.Close()

	// CSV Processing
	reader := csv.NewReader(r)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("Error parsing data: %w", err)
	}

	writer := csv.NewWriter(w)
	defer writer.Flush()

	header := []string{"key", "date", "project_id", "volume", "currency", "volume_usd"}
	writer.Write(header)

	for _, record := range records[1:] { // Skip the header row
		ts := record[1]
		projectID := record[3]
		propsJSON := record[14]
		numsJSON := record[15]

		var props Props
		var nums Nums

		// Unmarshal JSON fields
		err := json.Unmarshal([]byte(propsJSON), &props)
		if err != nil {
			// return fmt.Errorf("Error parsing currency value: %w", err)
			fmt.Println("error")
			continue
		}

		err = json.Unmarshal([]byte(numsJSON), &nums)
		if err != nil {
			// return fmt.Errorf("Error parsing currency value: %w", err)
			fmt.Println("error")
			continue
		}

		// Parse timestamp and format date
		timestamp, err := time.Parse("2006-01-02 15:04:05.000", ts)
		if err != nil {
			// return fmt.Errorf("Error parsing currency value: %w", err)
			fmt.Println("error")
			continue
		}

		date := timestamp.Format("2006-01-02")

		// Convert currency value to float
		var currencyValueDecimal float64
		_, err = fmt.Sscanf(nums.CurrencyValueDecimal, "%f", &currencyValueDecimal)
		if err != nil {
			fmt.Println("error")
			continue
		}

		// Map CurrencySymbol to symbol_id
		symbolID, ok := symbolMap[props.CurrencySymbol]
		if !ok {
			fmt.Println("error")
			continue
		}

		// Get the conversion rate to USD
		usdValue, err := getUSDValue(symbolID, currencyValueDecimal, date)
		if err != nil {
			fmt.Println("error")
			continue
		}

		// Create a unique key for the map
		key := date + "_" + projectID

		transformedRecord := []string{key, date, projectID, fmt.Sprintf("%.2f", currencyValueDecimal), symbolID, fmt.Sprintf("%.2f", usdValue)}

		// Write the transformed record to the output CSV file
		writer.Write(transformedRecord)
	}
	log.Printf("Transformed CSV: %s/%s", outputBucketName, outputFileName)
	return nil
}

// loadSymbolMap loads the symbol_id map from the JSON file in the same directory
func loadSymbolMap() (map[string]string, error) {
	filePath := "symbol_id_map.json"

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("os.ReadFile: %w", err)
	}

	var symbolMap map[string]string
	if err := json.Unmarshal(data, &symbolMap); err != nil {
		return nil, fmt.Errorf("json.Unmarshal: %w", err)
	}

	return symbolMap, nil
}

// getUSDValue gets the conversion rate to USD from CoinGecko API
func getUSDValue(symbolID string, currencyValueDecimal float64, date string) (float64, error) {
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd&date=%s", symbolID, date)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("http.NewRequest: %w", err)
	}

	req.Header.Set("accept", "application/json")
	req.Header.Set("x-cg-api-key", "CG-MpAym1juMY83MMxEqGK3QBzT")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("client.Do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var result map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("json.NewDecoder: %w", err)
	}

	usdRate, ok := result[symbolID]["usd"]
	if !ok {
		return 0, fmt.Errorf("usd rate not found for symbol: %s", symbolID)
	}

	return usdRate * currencyValueDecimal, nil
}
