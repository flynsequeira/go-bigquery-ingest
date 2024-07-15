package ingester

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
)

var callCounter = 0   // CALL COUNTER
var refTime time.Time // TIME THE FOR LOOP STARTS

const maxCallsPerMinute = 3
const timeWindowSeconds = 60

var requestTimestamps []time.Time

func processCSV(ctx context.Context, data StorageObjectData) error {
	storageClient, err := getStorageClient(ctx)
	if err != nil {
		return err
	}
	defer storageClient.Close()

	pubsubClient, err := getPubSubClient(ctx, "blockdataproject")
	if err != nil {
		return err
	}
	defer pubsubClient.Close()
	topic := pubsubClient.Topic("transformed-data")

	symbolMap, err := loadSymbolMapFromGCS(ctx, storageClient, "blockdata-input", "symbol_id_map.json")
	if err != nil {
		return fmt.Errorf("Failed to load symbol map: %w", err)
	}

	inputBucket := storageClient.Bucket(data.Bucket)
	inputObj := inputBucket.Object(data.Name)
	r, err := inputObj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("Object(%q).NewReader: %w", data.Name, err)
	}
	defer r.Close()

	// Set up output CSV file
	outputBucketName := "blockdata-output"
	outputFileName := "transformed_data.csv"
	outputBucket := storageClient.Bucket(outputBucketName)
	outputObj := outputBucket.Object(outputFileName)
	w := outputObj.NewWriter(ctx)
	defer w.Close()

	writer := csv.NewWriter(w)
	defer writer.Flush()

	// Write header
	header := []string{"key", "date", "project_id", "volume", "currency", "volume_usd"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("Error writing CSV header: %w", err)
	}

	reader := csv.NewReader(r)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("Error parsing data: %w", err)
	}

	usdRateCache := make(map[string]float64)

	// API LIMIT MANAGER
	refTime = time.Now()

	for i, record := range records[1:] {
		// API LIMIT MANAGER FOR 30 API / MIN LIMIT

		transformed, err := transformRecord(record, symbolMap, &usdRateCache)
		log.Printf("Transforming record %d", i)
		if err != nil {
			log.Printf("Error: %v", err)
			continue
		}

		// Write to CSV
		csvRecord := []string{
			transformed.Key,
			transformed.Date,
			transformed.ProjectID,
			fmt.Sprintf("%.2f", transformed.Volume),
			transformed.Currency,
			fmt.Sprintf("%.2f", transformed.VolumeUSD),
		}
		if err := writer.Write(csvRecord); err != nil {
			log.Printf("Error writing CSV record: %v", err)
		}
		log.Printf("Attempting publish %s | Topic: %s", transformed.Key, topic)
		if err := publishToPubSub(ctx, topic, transformed); err != nil {
			log.Printf("Error publishing to Pub/Sub: %v", err)
		}
	}

	log.Printf("Transformed CSV: %s/%s", outputBucketName, outputFileName)

	// Log the USD rate cache
	usdRateCacheJSON, err := json.Marshal(usdRateCache)
	if err != nil {
		log.Printf("Error marshalling usdRateCache to JSON: %v", err)
	} else {
		log.Printf("USD rate cache content: %s", usdRateCacheJSON)
	}

	return nil
}

func transformRecord(record []string, symbolMap map[string]string, usdRateCache *map[string]float64) (Transformed, error) {
	ts := record[1]
	projectID := record[3]
	propsJSON := record[14]
	numsJSON := record[15]

	var props Props
	var nums Nums

	if err := json.Unmarshal([]byte(propsJSON), &props); err != nil {
		return Transformed{}, fmt.Errorf("propsJsonParseError: %w", err)
	}

	if err := json.Unmarshal([]byte(numsJSON), &nums); err != nil {
		return Transformed{}, fmt.Errorf("numsJsonParseError: %w", err)
	}

	key := props.TransactionHash

	timestamp, err := time.Parse("2006-01-02 15:04:05.000", ts)
	if err != nil {
		return Transformed{}, fmt.Errorf("timeParseError: %w", err)
	}

	date := timestamp.Format("2006-01-02")

	var currencyValueDecimal float64
	_, err = fmt.Sscanf(nums.CurrencyValueDecimal, "%f", &currencyValueDecimal)
	if err != nil {
		return Transformed{}, fmt.Errorf("currencyValueDecimalError: %w", err)
	}

	symbolID, ok := symbolMap[strings.ToLower(props.CurrencySymbol)]
	if !ok {
		return Transformed{}, fmt.Errorf("SymbolId not mapped(%s)", strings.ToLower(props.CurrencySymbol))
	}

	cacheKey := symbolID + "_" + date
	usdRate, rateInCache := (*usdRateCache)[cacheKey]

	if !rateInCache {
		// Rate limiting logic
		now := time.Now()
		for len(requestTimestamps) >= maxCallsPerMinute && now.Sub(requestTimestamps[0]) < timeWindowSeconds*time.Second {
			timeToWait := timeWindowSeconds*time.Second - now.Sub(requestTimestamps[0])
			log.Printf("Waiting for %.2f seconds due to rate limit...", timeToWait.Seconds())
			time.Sleep(timeToWait) // Wait until enough time has passed
			now = time.Now()       // Update the current time
		}

		usdRate, err = getUSDRate(symbolID, date)
		if err != nil {
			return Transformed{}, fmt.Errorf("CacheKey(%s) | Error: %w", cacheKey, err)
		}
		(*usdRateCache)[cacheKey] = usdRate
		callCounter++
		log.Printf("CallCounter = %d", callCounter)

		// Add timestamp to the end of the slice and remove the oldest one if the slice exceeds the limit
		requestTimestamps = append(requestTimestamps, now)
		if len(requestTimestamps) > maxCallsPerMinute {
			requestTimestamps = requestTimestamps[1:] // Remove the oldest timestamp
		}
	}

	usdValue := usdRate * currencyValueDecimal

	if err != nil {
		return Transformed{}, fmt.Errorf("projectIDParseError: %w", err)
	}

	return Transformed{
		Key:       key,
		Date:      date,
		ProjectID: projectID,
		Volume:    currencyValueDecimal,
		Currency:  symbolID,
		VolumeUSD: usdValue,
	}, nil
}
