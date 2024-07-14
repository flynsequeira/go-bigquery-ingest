package ingester

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
)

func publishToPubSub(ctx context.Context, topic *pubsub.Topic, transformed Transformed) error {
	jsonData, err := json.Marshal(transformed)
	if err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	msg := &pubsub.Message{
		Data: jsonData,
	}
	result, err := topic.Publish(ctx, msg).Get(ctx)
	if err != nil {
		return fmt.Errorf("topic.Publish: %w", err)
	}
	fmt.Printf("Published message to Pub/Sub with ID: %s", result)
	return nil
}

func getPubSubClient(ctx context.Context, projectID string) (*pubsub.Client, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub.NewClient: %w", err)
	}
	return client, nil
}
