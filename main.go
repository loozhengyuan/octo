package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/spf13/cobra"
)

var (
	// Flag Vars
	projectID     string
	storageBucket string
	pubSubTopic   string
	searchPattern string
	blobPrefix    string
	workerNodes   int

	// Root Command - octo
	rootCmd = &cobra.Command{
		Use: "octo",
		Long: `Fast, performant file uploader for Google Cloud Storage
More information: https://github.com/loozhengyuan/octo`,
	}

	// Sub Command - octo up
	upCmd = &cobra.Command{
		Use:     "up <glob pattern>",
		Short:   "Upload files matching a glob pattern",
		Example: "  octo up '*.gz' -p my-project -b my-bucket -t my-topic",
		Args:    cobra.ExactValidArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			// Set search pattern
			searchPattern = args[0]
			initUpload()
		},
	}
)

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

func initUpload() {
	// Create error variable
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
	// TODO: Add command flag for executor nodes
	log.Printf("Creating %v worker nodes", workerNodes)
	for w := 0; w < workerNodes; w++ {
		go uploadExecutor(w, jobQueue, callBack)
	}

	// Get list of files
	log.Printf("Searching for files matching pattern: %s", searchPattern)
	files := getFiles(searchPattern)
	log.Printf("Files found: %s", files)

	// Dispatch files to queue
	// TODO: Add log message for when no files are found
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

func main() {
	// upCmd Flags
	upCmd.Flags().StringVarP(&GoogleCloudProjectID, "project", "p", "", "name of the Google Cloud project")
	upCmd.Flags().StringVarP(&storageBucket, "bucket", "b", "", "name of the Storage bucket to upload")
	upCmd.Flags().StringVarP(&pubSubTopic, "topic", "t", "", "name of the Pub/Sub topic to publish")
	upCmd.Flags().StringVar(&blobPrefix, "prefix", "", "string prefix to append to the blob")
	upCmd.Flags().IntVar(&workerNodes, "workers", 10, "number of workers nodes to spawn")
	upCmd.MarkFlagRequired("project")
	upCmd.MarkFlagRequired("bucket")
	upCmd.MarkFlagRequired("topic")

	// Execute commands
	rootCmd.AddCommand(upCmd)
	rootCmd.Execute()
}
