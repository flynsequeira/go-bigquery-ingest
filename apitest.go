package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
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

// TransformCSV processes the CSV file.
func TransformCSV(inputFileName string, outputFileName string, symbolMapFileName string) error {
	// Open the input CSV file
	inputFile, err := os.Open(inputFileName)
	if err != nil {
		return fmt.Errorf("could not open input file: %v", err)
	}
	defer inputFile.Close()

	// Load the symbol_id_map.json
	symbolMap, err := loadSymbolMapFromFile(symbolMapFileName)
	if err != nil {
		return fmt.Errorf("failed to load symbol map: %v", err)
	}

	// Open the output CSV file
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf("could not create output file: %v", err)
	}
	defer outputFile.Close()

	// CSV Processing
	reader := csv.NewReader(inputFile)
	records, err := reader.ReadAll()
	if err != nil {
		return fmt.Errorf("error parsing data: %v", err)
	}

	writer := csv.NewWriter(outputFile)
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
			fmt.Println("error unmarshelling propsJSON", err)
			continue
		}

		err = json.Unmarshal([]byte(numsJSON), &nums)
		if err != nil {
			fmt.Println("error unmarshelling numsJSON", err)
			continue
		}

		// Parse timestamp and format date
		timestamp, err := time.Parse("2006-01-02 15:04:05.000", ts)
		if err != nil {
			fmt.Println("error parsing timestamp", err)
			continue
		}

		date := timestamp.Format("2006-01-02")

		// Convert currency value to float
		var currencyValueDecimal float64
		_, err = fmt.Sscanf(nums.CurrencyValueDecimal, "%f", &currencyValueDecimal)
		if err != nil {
			fmt.Println("error converting currency value to float", err)
			continue
		}

		// Map CurrencySymbol to symbol_id
		symbolID, ok := symbolMap[strings.ToLower(props.CurrencySymbol)]
		if !ok {
			fmt.Println("symbolID not found in map")
			continue
		}

		// Get the conversion rate to USD
		usdValue, err := getUSDValue(symbolID, currencyValueDecimal, date)
		if err != nil {
			fmt.Println("error getting USD value", err)
			continue
		}

		// Create a unique key for the map
		key := date + "_" + projectID

		transformedRecord := []string{key, date, projectID, fmt.Sprintf("%.2f", currencyValueDecimal), symbolID, fmt.Sprintf("%.2f", usdValue)}

		// Write the transformed record to the output CSV file
		writer.Write(transformedRecord)
	}

	return nil
}

// loadSymbolMapFromFile loads the symbol_id map from a JSON file
func loadSymbolMapFromFile(fileName string) (map[string]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("could not open file: %v", err)
	}
	defer file.Close()

	var symbolMap map[string]string
	if err := json.NewDecoder(file).Decode(&symbolMap); err != nil {
		return nil, fmt.Errorf("json.NewDecoder: %v", err)
	}

	return symbolMap, nil
}

// getUSDValue gets the conversion rate to USD from CoinGecko API
func getUSDValue(symbolID string, currencyValueDecimal float64, date string) (float64, error) {
	url := fmt.Sprintf("https://api.coingecko.com/api/v3/simple/price?ids=%s&vs_currencies=usd&date=%s", symbolID, date)
	log.Println("url:", url)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, fmt.Errorf("http.NewRequest: %v", err)
	}

	req.Header.Set("accept", "application/json")
	req.Header.Set("x-cg-api-key", "CG-MpAym1juMY83MMxEqGK3QBzT")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("client.Do: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var result map[string]map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("json.NewDecoder: %v", err)
	}

	usdRate, ok := result[symbolID]["usd"]
	if !ok {
		return 0, fmt.Errorf("usd rate not found for symbol: %s", symbolID)
	}

	return usdRate * currencyValueDecimal, nil
}

func main() {
	// Replace these with your actual file names
	inputFileName := "input_small_6.csv"
	outputFileName := "output.csv"
	symbolMapFileName := "symbol_id_map.json"

	err := TransformCSV(inputFileName, outputFileName, symbolMapFileName)
	if err != nil {
		log.Fatalf("TransformCSV failed: %v", err)
	}

	log.Println("CSV transformation completed successfully")
}
