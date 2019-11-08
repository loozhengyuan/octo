package main

import (
	"context"
	"fmt"
	"log"
	"flag"
	"os"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

// Job variables
var pubSubTopic string
var storageBucket string
var searchPattern string
var blobPrefix string

func uploadExecutor(n int, jobQueue <-chan string, callBack chan<- int) {
	for file := range jobQueue {
		// Create blob format
		blob := blobFormatter(blobPrefix, file)

		// Upload file to Storage
		b := &StorageBucket{
			StorageClient,
			storageBucket,
		}
		log.Printf("Worker #%v: Uploading File %s to %s/%s", n, file, b.name, blob)
		if err := b.Upload(file, blob); err != nil {
			// TODO: Log fatal while allowing other goroutines to gracefully exit
			log.Fatalf("Worker #%v: Error uploading to GCS bucket %s: %v", n, b.name, err)
		}

		// Notify PubSub
		message := fmt.Sprintf("File %s/%s uploaded!", b.name, blob)
		attrs := map[string]string{
			"bucket": b.name,
			"blob":   blob,
		}
		t := &PubSubTopic{
			PubSubClient,
			pubSubTopic,
		}
		log.Printf("Worker #%v: Publishing File %s to Pub/Sub topic: %s", n, file, t.name)
		if _, err := t.Publish(message, attrs); err != nil {
			// TODO: Log fatal while allowing other goroutines to gracefully exit
			log.Fatalf("Worker #%v: Error publishing to Pub/Sub topic %s: %v", n, t.name, err)
		}

		// Delete file before terminating
		if err := os.Remove(file); err != nil {
			// TODO: Log fatal while allowing other goroutines to gracefully exit
			log.Fatalf("Worker #%v: Error deleting file %s: %v", n, file, err)
		}

		callBack <- 1
	}
}

func main() {
	// Parse command flags
	flag.StringVar(&GoogleCloudProjectID, "project", "", "project id of the GCP project")
	flag.StringVar(&pubSubTopic, "topic", "", "pub/sub topic to publish to")
	flag.StringVar(&storageBucket, "bucket", "", "storage bucket to upload to")
	flag.StringVar(&searchPattern, "pattern", "*", "file patterns to search for")
	flag.StringVar(&blobPrefix, "prefix", "", "string to prepend all uploaded blobs")
	flag.Parse()

	// Echo commands
	// TODO: Handle missing parameters
	log.Printf("Setting project-id as: %s", GoogleCloudProjectID)
	log.Printf("Storing all files in bucket: %s", storageBucket)
	log.Printf("Publishing messages to topic: %s", pubSubTopic)

	// Create err variable
	var err error

	// Create ctx variable
	ctx := context.Background()

	// Create LoggingClient first
	// TODO: Instantiate logger
	LoggingClient, err = logging.NewClient(ctx, GoogleCloudProjectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer LoggingClient.Close()

	// Create StorageClient
	StorageClient, err = storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create PubSubClient
	PubSubClient, err = pubsub.NewClient(ctx, GoogleCloudProjectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create jobQueue and callBack channels
	// TODO: Consider if making non-bounded channels is viable
	jobQueue := make(chan string, 100)
	callBack := make(chan int, 100)

	// Create uploadExecutor nodes
	limit := 2
	for w := 0; w < limit; w++ {
		go uploadExecutor(w, jobQueue, callBack)
	}

	// Get list of files
	log.Printf("Searching for files matching pattern: %s", searchPattern)
	files := getFiles(searchPattern)
	log.Printf("Files found: %s", files)

	// Dispatch files to queue
	for _, f := range files {
		log.Printf("Enqueuing File: %s", f)
		jobQueue <- f
	}

	// Await all goroutines to complete
	// Close is not needed because program will automatically
	// terminate once goroutines from all files has finished
	for a := 0; a < len(files); a++ {
		<-callBack
	}
}
