// Package helloworld provides a set of Cloud Functions samples.
package ingester

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
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

// TransformedRecord represents the transformed data structure (added struct)
type Transformed struct {
	Key       string  `json:"key"`
	Date      string  `json:"date"`
	ProjectID int     `json:"project_id"`
	Volume    float64 `json:"volume"`
	Currency  string  `json:"currency"`
	VolumeUSD float64 `json:"volume_usd"`
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

	// PUB-SUB
	pubsubClient, err := pubsub.NewClient(ctx, "blockdataproject")
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %w", err)
	}
	defer pubsubClient.Close()
	topic := pubsubClient.Topic("transformed-data")

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
	// log.Printf("GOT BUCKET AND GETTING SYMBOL")
	// Load symbol_id_map.json

	// Load symbol_id_map.json from GCP Storage
	symbolMap, err := loadSymbolMapFromGCS(ctx, client, "blockdata-input", "symbol_id_map.json")
	if err != nil {
		return fmt.Errorf("Failed to load symbol map: %w", err)
	}

	// log.Printf("obtained symbol map")
	// Output setup (change as needed)
	outputBucketName := "blockdata-output" // Or use a dynamic name
	outputFileName := fmt.Sprintf("transformed_data.csv")
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

	log.Printf("Looping through records")
	usdRateCache := make(map[string]float64)

	for i, record := range records[1:] { // Skip the header row
		fmt.Println("i: ", i)
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
			fmt.Errorf("propsJsonParseError: %w", err)
			continue
		}

		err = json.Unmarshal([]byte(numsJSON), &nums)
		if err != nil {
			// return fmt.Errorf("Error parsing currency value: %w", err)
			fmt.Errorf("numsJsonParseError: %w", err)
			continue
		}

		// Parse timestamp and format date
		timestamp, err := time.Parse("2006-01-02 15:04:05.000", ts)
		if err != nil {
			// return fmt.Errorf("Error parsing currency value: %w", err)
			fmt.Errorf("timeParseError: %w", err)
			continue
		}

		date := timestamp.Format("2006-01-02")

		// Convert currency value to float
		var currencyValueDecimal float64
		_, err = fmt.Sscanf(nums.CurrencyValueDecimal, "%f", &currencyValueDecimal)
		if err != nil {
			fmt.Errorf("currencyValueDecimalError: %w", err)
			continue
		}

		// Map CurrencySymbol to symbol_id
		symbolID, ok := symbolMap[strings.ToLower(props.CurrencySymbol)]
		if !ok {
			fmt.Errorf("SymbolId not mapped(%s) | \n Error: %w", strings.ToLower(props.CurrencySymbol), err)
			continue
		}

		// Check cache for USD rate
		cacheKey := symbolID + "_" + date
		usdRate, rateInCache := usdRateCache[cacheKey]
		fmt.Println("i: ", cacheKey)

		if !rateInCache {
			// Get the conversion rate to USD
			// log.Println("API Used")
			usdRate, err = getUSDRate(symbolID, date)
			if err != nil {
				fmt.Errorf("CacheKey(%s) | \n Error: %w", cacheKey, err)
				continue
			}
			// Store rate in cache
			usdRateCache[cacheKey] = usdRate
		}
		// Get the conversion rate to USD
		usdValue := usdRate * currencyValueDecimal

		// Create a unique key for the map
		key := date + "_" + projectID
		projectIDInt, err := strconv.Atoi(projectID)

		transformedRecord := []string{key, date, projectID, fmt.Sprintf("%.2f", currencyValueDecimal), symbolID, fmt.Sprintf("%.2f", usdValue)}

		// Create a TransformedRecord struct
		transformed := Transformed{
			Key:       key,
			Date:      date,
			ProjectID: projectIDInt,
			Volume:    currencyValueDecimal,
			Currency:  symbolID,
			VolumeUSD: usdValue,
		}
		// Create Pub/Sub message data
		jsonData, err := json.Marshal(transformed) // Convert to JSON
		if err != nil {
			return fmt.Errorf("json.Marshal: %w", err)
		}

		// Publish message to Pub/Sub topic
		msg := &pubsub.Message{
			Data: jsonData,
		}
		result, err := topic.Publish(ctx, msg).Get(ctx)
		if err != nil {
			return fmt.Errorf("topic.Publish: %w", err)
		}
		log.Printf("Published message to Pub/Sub with ID: %s", result)

		// BACKUP: write transformed data to CSV file) ...
		writer.Write(transformedRecord)
	}
	// log.Printf("Transformed CSV: %s/%s", outputBucketName, outputFileName)
	// Serialize the map to JSON
	usdRateCacheJSON, err := json.Marshal(usdRateCache)
	if err != nil {
		log.Printf("Error marshalling usdRateCache to JSON: %v", err)
		return err // Or choose to continue without logging the cache
	}

	// Log the cache as a JSON string
	log.Printf("USD rate cache content: %s", usdRateCacheJSON)
	return nil
}

// loadSymbolMapFromGCS loads the symbol_id map from a JSON file in a GCP Storage bucket
func loadSymbolMapFromGCS(ctx context.Context, client *storage.Client, bucketName, fileName string) (map[string]string, error) {
	bucket := client.Bucket(bucketName)
	obj := bucket.Object(fileName)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("Object(%q).NewReader: %w", fileName, err)
	}
	defer r.Close()

	var symbolMap map[string]string
	if err := json.NewDecoder(r).Decode(&symbolMap); err != nil {
		return nil, fmt.Errorf("json.NewDecoder: %w", err)
	}

	return symbolMap, nil
}

// getUSDValue gets the conversion rate to USD from CoinGecko API
func getUSDRate(symbolID string, date string) (float64, error) {
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd&date=%s", symbolID, date)
	// log.Println("url:", url)
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
	} else {
		log.Println("API successfully requested | Symbol-Date: %s - %s", symbolID, date)
	}
	var result map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("json.NewDecoder: %w", err)
	}

	usdRate, ok := result[symbolID]["usd"]
	if !ok {
		return 0, fmt.Errorf("usd rate not found for symbol: %s", symbolID)
	}

	return usdRate, nil
}
