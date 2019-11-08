package main

import (
	"context"
	"io"
	"os"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

// GoogleCloudProjectID stores the project id of a GCP project
var GoogleCloudProjectID string

// LoggingClient is a logging.Client object
var LoggingClient *logging.Client

// StorageClient is a storage.Client object
var StorageClient *storage.Client

// PubSubClient is a pubsub.Client object
var PubSubClient *pubsub.Client

// InitLoggingClient instantiates the client object for Google Stackdriver Logging
func InitLoggingClient(ctx context.Context) error {
	client, err := logging.NewClient(ctx, GoogleCloudProjectID)
	if err != nil {
		return err
	}
	LoggingClient = client
	return nil
}

// InitStorageClient instantiates the client object for Google Stackdriver Storage
func InitStorageClient(ctx context.Context) error {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	StorageClient = client
	return nil
}

// InitPubSubClient instantiates the client object for Google Stackdriver PubSub
func InitPubSubClient(ctx context.Context) error {
	client, err := pubsub.NewClient(ctx, GoogleCloudProjectID)
	if err != nil {
		return err
	}
	PubSubClient = client
	return nil
}

// StackdriverLog is a log object in Google Stackdriver Logging
type StackdriverLog struct {
	client *logging.Client
	name   string
}

// StorageBucket is a bucket object in Google Cloud Storage
type StorageBucket struct {
	client *storage.Client
	name   string
}

// PubSubTopic is a topic object in Google Pub Sub
type PubSubTopic struct {
	client *pubsub.Client
	name   string
}

// Upload is a method to upload files to a Google Cloud Storage bucket
func (b *StorageBucket) Upload(file, blob string) error {
	ctx := context.Background()

	// Opens file
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// Copy file to bucket
	wc := b.client.Bucket(b.name).Object(blob).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

// Publish is a method to publish messages to a PubSubTopic
func (k *PubSubTopic) Publish(message string, attrs map[string]string) (string, error) {
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
