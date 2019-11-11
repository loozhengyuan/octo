package gcp

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// PubSubTopic is a topic object in Google Pub Sub
type Topic struct {
	client *pubsub.Client
	ctx    *context.Context
	name   string
}

// Publish is a method to publish messages to a PubSubTopic
func (k *Topic) Publish(message string, attrs map[string]string) (string, error) {
	ctx := context.Background()

	// Get topic object
	t := k.client.Topic(k.name)

	// Publish message
	result := t.Publish(ctx, &pubsub.Message{
		Data:       []byte(message),
		Attributes: attrs,
	})

	// Block until the result is returned and a server-generated
	// ID is returned for the published message.
	id, err := result.Get(ctx)
	if err != nil {
		return "", err
	}
	return id, nil
}
