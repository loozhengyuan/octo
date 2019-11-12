package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
)

// Topic is a topic object in Google Pub Sub
type Topic struct {
	Client *pubsub.Client
	Ctx    *context.Context
	Name   string
}

// NewTopic returns a Topic object type
func NewTopic(ctx *context.Context, project, topic string) (*Topic, error) {

	// Create new client
	client, err := pubsub.NewClient(*ctx, project)
	if err != nil {
		return nil, fmt.Errorf("Failed to create client: %w", err)
	}

	// Create topic
	t := &Topic{
		Client: client,
		Ctx:    ctx,
		Name:   topic,
	}
	return t, nil
}

// Publish is a method to publish messages to a PubSubTopic
func (k *Topic) Publish(message string, attrs map[string]string) (string, error) {
	ctx := context.Background()

	// Get topic object
	t := k.Client.Topic(k.Name)

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
