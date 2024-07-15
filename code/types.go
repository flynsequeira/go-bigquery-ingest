package ingester

import "time"

type StorageObjectData struct {
	Bucket         string    `json:"bucket,omitempty"`
	Name           string    `json:"name,omitempty"`
	Metageneration int64     `json:"metageneration,string,omitempty"`
	TimeCreated    time.Time `json:"timeCreated,omitempty"`
	Updated        time.Time `json:"updated,omitempty"`
}

type Props struct {
	CurrencySymbol  string `json:"currencySymbol"`
	TransactionHash string `json:"txnHash"`
}

type Nums struct {
	CurrencyValueDecimal string `json:"currencyValueDecimal"`
}

type TransformedRecord struct {
	Date             string
	ProjectID        string
	NumTransactions  int
	TotalVolumeInUSD float64
}

type Transformed struct {
	Key       string  `json:"key"`
	Date      string  `json:"date"`
	ProjectID string  `json:"project_id"`
	Volume    float64 `json:"volume"`
	Currency  string  `json:"currency"`
	VolumeUSD float64 `json:"volume_usd"`
}
