package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

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

func main() {
	inputFile := "sample_data.csv"
	outputFile := "output.csv"

	// Open the input CSV file
	file, err := os.Open(inputFile)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()

	// Read the input CSV file
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Prepare the output CSV file
	outFile, err := os.Create(outputFile)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// Write the header for the output CSV file
	header := []string{"key", "date", "project_id", "vol_in_usd"}
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
			fmt.Println("Error unmarshaling props JSON:", err, "Record:", props.CurrencySymbol)
			continue
		}
		err = json.Unmarshal([]byte(numsJSON), &nums)
		if err != nil {
			fmt.Println("Error unmarshaling nums JSON:", err, "Record:", nums.CurrencyValueDecimal)
			continue
		}

		// Parse timestamp and format date
		timestamp, err := time.Parse("2006-01-02 15:04:05.000", ts)
		if err != nil {
			fmt.Println("Error parsing timestamp:", err, "Record:", record)
			continue
		}
		date := timestamp.Format("2006-01-02")

		// Convert currency value to float
		var currencyValueDecimal float64
		_, err = fmt.Sscanf(nums.CurrencyValueDecimal, "%f", &currencyValueDecimal)
		if err != nil {
			fmt.Println("Error parsing currency value:", err, "Record:", record)
			continue
		}

		// Create a unique key for the map
		key := date + "_" + projectID

		// For simplicity, let's assume 1 currency unit = 1 USD
		// In a real scenario, you would fetch the conversion rate from an API like CoinGecko
		volInUsd := currencyValueDecimal
		transformedRecord := []string{key, date, projectID, fmt.Sprintf("%.2f", volInUsd)}

		// Write the transformed record to the output CSV file
		writer.Write(transformedRecord)

	}
}
