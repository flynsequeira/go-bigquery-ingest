package ingester

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/storage"
)

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

func getStorageClient(ctx context.Context) (*storage.Client, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("storage.NewClient: %w", err)
	}
	return client, nil
}
