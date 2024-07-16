# go-bq-ingester

Efficiently ingest data from CSV files into BigQuery using Go.

## Overview

This repository provides a comprehensive solution for transforming CSV data using Go and loading it into Google BigQuery.

## Key Resources

* **Video Walkthrough:** Get a visual overview of the process with this video tutorial: https://youtu.be/mTveDBGiHTE
* **Detailed Documentation:** Dive into the complete documentation for setup, usage, and customization: https://github.com/flynsequeira/go-bq-ingester/blob/8d3e600f9e31f43c8d532ac954e9ea1e4923613b/DOCUMENTATION.pdf

## Repository Contents

* **code/**: Contains the Go code for data transformation and interaction with Google Cloud Functions. The primary entry point is `function.go`.
* **schema/**: Has the table and materialized view schema
* **api-data/**: Provides JSON mappings for currency symbols to their corresponding IDs.
* **analytics/**: Includes additional analysis performed using Jupyter Notebooks (IPython Notebooks).
* **data/**: Stores various input and output CSV files.

## Next Steps

1. **Demo the Video:** Contains all the pipeline set up and few UI-based metrics available there. 
2. **Explore the Code:** Dive into the `code` and `schema` directory to understand the code implementation details. 


## Overview

This repository provides a complete data pipeline for processing cryptocurrency data:

1. **Extraction and Normalization:** Go scripts extract, flatten, and enrich cryptocurrency data from CSV files.
2. **API Integration:** CoinGecko API is utilized for real-time USD exchange rates.
3. **BigQuery Loading and Aggregation:** Transformed data is loaded into BigQuery, where it's aggregated using Material

## Architecture

![Image of architecture.png](https://raw.githubusercontent.com/flynsequeira/go-bigquery-ingest/main/resources/archi_black.png)


## Detailed Solution

### Part 1: GCP Storage Bucket & Cloud Functions Trigger

*   **Go Cloud Function:** Extracts, transforms, and enriches CSV data. Utilizes a 900-second timeout to handle CoinGecko API rate limiting.
*   **CoinGecko API Interaction:**
    *   A pre-populated `symbol_id_map.json` (generated using PySpark) optimizes API calls.
    *   A hashmap caches USD values to reduce API requests.
    *   A rate limiter ensures compliance with CoinGecko's rate limits.

```go
if callCounter >= 5 {
    time.Sleep(time.Duration(60) * time.Second)
}
```
### Part 2: Pub/Sub Messaging

*   **Main Queue:**  The Staging table subscribes to this queue, where the function publishes transformed data.
*   **Dead Letter Queue (DLQ):**  Handles schema mismatches, bad data, and failed messages.
*   **Handling Data Inconsistency:**  Addresses inconsistencies in volume data for improved analysis.

### Part 3: BigQuery Materialized View

*   **Staging Table:**  Stores transformed data before aggregation.
  
     **Solution 1: BigQuery Staging Table Aggregated to Sink [SELECTED]**
  
         - Extract, clean, and filter data, then drop it into a staging table.
         - Aggregate the staging table in BigQuery into a materialized view.
    
     **~~Solution 2: Aggregate in Go with Memory~~**
    
         - Not viable at the moment as it needs to be scalable.
         - Possible with a smaller dataset, but scalability is assumed to be necessary.

     **Chosen Approach:** Solution 1.

*   **Materialized view:**  Aggregates data by `DATE` and `PROJECT_ID`, calculating `total_transactions` and `total_volume_in_usd` for efficient querying.
*   **Materialized view Cleaned:**  Cleans and filters potential bad data
```sql
    SELECT
    date,
    project_id, 
    COUNT(volume) AS total_transactions,
    SUM(volume_usd) AS total_volume_in_usd 
    FROM `blockdataproject.blockdata.staging`
    GROUP BY date, project_id
```

## COINGECKO FIASCO - API Complications

### Step 1: Creating the Symbol-ID Map

To start, I created a mapping of cryptocurrency symbols to their respective IDs. This allowed for precise data requests later on.

```python
import json

# Load the list of coins from a JSON file
with open('coins_list.json', 'r') as input_file:
    data = json.load(input_file)

# Create a dictionary mapping symbols to their IDs
result = {item['symbol']: item['id'] for item in data}

# Save the symbol-ID map to a JSON file
with open('symbol_id_map.json', 'w') as output_file:
    json.dump(result, output_file, indent=4)
```

This map is crucial for making accurate API requests using the SymbolId.

### Step 2: Making an API Request

Using the mapped symbols, I was able to request specific data from CoinGecko.

```python
curl --request GET \
     --url "https://api.coingecko.com/api/v3/simple/price" \
     --header 'accept: application/json' \
     --header 'x-cg-api-key: API_KEY' \
     --get \
     --data-urlencode "ids=bitcoin" \
     --data-urlencode "vs_currencies=usd" \
     --data-urlencode "date=01-01-2024"
```

### Step 3: Efficient API Calls with a Hashmap

To optimize the process, I utilized a hashmap to store the USD value of a coin for a particular date. This way, the hashmap can be referenced before making an API call, reducing unnecessary requests. The hashmap key is a combination of SymbolId and Date, providing quick access to the average rate of any currency in USD.

### Step 4: Handling API Rate Limits

**Issue:** The API calls were exceeding the allowed limit. Initially, I assumed the limit was 30 calls per minute, but it turned out to be much lower at 5 calls per minute due to some account issue.

**Solution:** Implementing a rate limiter to manage the API calls efficiently.

```sql
if callCounter >= 5 {
    time.Sleep(time.Duration(60) * time.Second)
}
```

By incorporating this rate limiter, I ensured that the API calls stayed within the permitted limit, preventing any overload issues.

## DEAD LETTER QUEUE ANALYSIS

### Potential Points of Failure Requiring DLQ

- Transformation error
- Function timeout
- API request failure
- BigQuery API timeout

### Our Actual Failure Point

The issue occurred when `volume_usd` and `volume` fields had extremely high values that exceeded the `FLOAT`, `NUMERIC`, and even `BIG NUMERIC` data types, causing the schema configuration of the subscriber to the Pub/Sub topic to fail.

Example problematic data:

```python
{"key":"2024-04-02_1609","date":"2024-04-02","project_id":1609,"volume":2565000000000000000,"currency":"matic-network","volume_usd":1355392170000000300}
```

### Analyzing Unusual DLQ Records

Here’s an example of an unusual record that landed in the DLQ:

```python
{"currency":"usdc-rainbow-bridge","date":"2024-04-01","key":"0x420cac3c8566054b8b0623c8556210b720bc1ea1a8bc8feb2ee4e1be949b72ce","project_id":"4974","volume":1110000,"volume_usd":1105140.42}
```

This record didn't have excessively high values, but the `volume` field lacked a decimal. I republished the same record directly from the Pub/Sub topic with decimal points to see if they went through for this project, and they did. So, in conclusion, in Go, JSON, Marshal omits .0 for whole number floats, causing BigQuery schema mismatches. The use of Avro for data serialization could ensure correct float representation and schema compatibility across Pub/Sub and BigQuery.

### Data Inconsistency Problem

Upon analyzing the data, I found that most values range between 1 to 3 digits. Since all data is close to 1 USD, I’m making all the currencies comparable for analytics by ignoring precision.

![Distribution Image](https://raw.githubusercontent.com/flynsequeira/go-bq-ingestor/main/resources/distribution_img.png)

- For "MATIC", where digits range from 18 to 20, corresponding to 1, 2, and 3, I'm dividing the values by \(10^{18}\), resulting in values ranging from 0.xx to xx.xx.
- For "USDC" or "USDC.e", some values in `decimalValue` inconsistently have 7 digits or range from zero to 1 digit. I'm dividing any data with a length of 7 by \(10^{7}\).


### What didn’t work

- I was looking to store the data of aggregate directly using MERGE and UPDATE. This isn’t something directly possible on BigQuery tables.


# Steps to Run

Working demo available here: [Watch Demo](https://youtu.be/mTveDBGiHTE)

## Step-by-Step Instructions

1. **Setup BigQuery Tables:**
    - Go to the BigQuery page.
    - Create new tables using the schemas in the GitHub folder named `schemas`.
    - Ensure you set up tables for staging and DLQ (Dead Letter Queue).

2. **Configure Pub/Sub Topics and Subscribers:**
    - Add two Pub/Sub topics: `transformed_data` and `transformed_dlq`.
    - Create two subscribers to write data to BigQuery tables:
        - **Staging:** Subscriber for the `transformed_data` topic.
        - **DLQ:** Subscriber for the `transformed_dlq` topic.

3. **Upload CSV File:**
    - Add a new CSV file to your Google Cloud Storage's `blockdata-input` bucket.

4. **Run the SQL Queries:**
    - Execute the following SQL queries in BigQuery to verify and analyze the data:
    
    ```sql
    -- View data in the staging table
    SELECT * FROM blockdataproject.blockdata.staging;
    
    -- View data in the DLQ table
    SELECT * FROM blockdataproject.blockdata.dlq;
    
    -- Final Solution: View aggregated transaction data
    SELECT * FROM blockdataproject.blockdata.transaction_aggregate;
    
    -- Final Solution: View cleaned aggregated transaction data
    SELECT * FROM blockdataproject.blockdata.transaction_aggregate_cleaned;
    ```

# References & Helps

1. Pub/Sub to Big Query: https://cloud.google.com/dataflow/docs/tutorials/dataflow-stream-to-bigquery
2. Pub/Sub API : https://pkg.go.dev/cloud.google.com/go/pubsub
3. pub/sub to big query video: https://www.youtube.com/watch?v=jXilpXhUXso&ab_channel=SkillCurb
4. CoinGecko Rate Limit problem - https://support.coingecko.com/hc/en-us/articles/4538771776153-What-is-the-rate-limit-for-CoinGecko-API-public-plan
