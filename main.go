package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/loozhengyuan/octo/api/gcp"
	"github.com/spf13/cobra"
)

var (
	// Create ctx variable
	ctx = context.Background()

	// WaitGroup
	wg sync.WaitGroup

	// Flag Vars
	projectID     string
	storageBucket string
	pubSubTopic   string
	searchPattern string
	blobPrefix    string

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
		Args:    cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

			// Create bucket object
			log.Println("Creating bucket object")
			b, err := gcp.NewBucket(&ctx, projectID, storageBucket)
			if err != nil {
				log.Fatalf("Failed to create new bucket object: %v", err)
			}

			// Create topic object
			log.Println("Creating topic object")
			t, err := gcp.NewTopic(&ctx, projectID, pubSubTopic)
			if err != nil {
				log.Fatalf("Failed to create new topic object: %v", err)
			}

			// Launch goroutine for every file
			for i, path := range args {
				// Increment the WaitGroup counter.
				wg.Add(1)

				// Process path
				log.Printf("Processing: %s", path)
				go func(n int, file string) {

					// Decrement the counter when the goroutine completes.
					defer wg.Done()

					// Create blob format
					blob := blobFormatter(blobPrefix, file)

					// Upload file to Storage
					log.Printf("Worker #%v: Uploading File %s to %s/%s", n, file, b.Name, blob)
					if err := b.Upload(file, blob); err != nil {
						// TODO: Log fatal while allowing other goroutines to gracefully exit
						log.Fatalf("Worker #%v: Error uploading to GCS bucket %s: %v", n, b.Name, err)
					}

					// Notify PubSub
					message := fmt.Sprintf("File %s/%s uploaded!", b.Name, blob)
					attrs := map[string]string{
						"bucket": b.Name,
						"blob":   blob,
					}
					log.Printf("Worker #%v: Publishing File %s to Pub/Sub topic: %s", n, file, t.Name)
					if _, err := t.Publish(message, attrs); err != nil {
						// TODO: Log fatal while allowing other goroutines to gracefully exit
						log.Fatalf("Worker #%v: Error publishing to Pub/Sub topic %s: %v", n, t.Name, err)
					}

					// Delete file before terminating
					if err := os.Remove(file); err != nil {
						// TODO: Log fatal while allowing other goroutines to gracefully exit
						log.Fatalf("Worker #%v: Error deleting file %s: %v", n, file, err)
					}
				}(i, path)
			}
		},
	}

	// Sub Command - octo load
	loadCmd = &cobra.Command{
		Use:   "load gs://<your_uri>",
		Short: "Load files from Storage to BigQuery",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

			// Temp vars
			// TODO: Remove!
			dataset := "Intel"
			table := "test"

			// Create table object
			log.Println("Creating table object")
			t, err := gcp.NewTable(&ctx, projectID, dataset, table)
			if err != nil {
				log.Fatalf("Failed to create new table object: %v", err)
			}

			// Launch goroutine for every file
			for i, uri := range args {
				// Increment the WaitGroup counter.
				wg.Add(1)

				// Process uri
				fmt.Printf("Processing: %s", uri)
				go func(worker int, file string) {
					// Decrement the counter when the goroutine completes.
					defer wg.Done()

					// Load
					log.Printf("Worker #%v: Loading data", worker)
					if err := t.LoadFromGcs(file); err != nil {
						log.Fatalf("Worker #%v: Failed to load data: %v", worker, err)
					}

				}(i, uri)
			}
		},
	}
)

func main() {
	// upCmd Flags
	upCmd.Flags().StringVarP(&projectID, "project", "p", "", "name of the Google Cloud project")
	upCmd.Flags().StringVarP(&storageBucket, "bucket", "b", "", "name of the Storage bucket to upload")
	upCmd.Flags().StringVarP(&pubSubTopic, "topic", "t", "", "name of the Pub/Sub topic to publish")
	upCmd.Flags().StringVar(&blobPrefix, "prefix", "", "string prefix to append to the blob")
	upCmd.MarkFlagRequired("project")
	upCmd.MarkFlagRequired("bucket")
	upCmd.MarkFlagRequired("topic")
	rootCmd.AddCommand(upCmd)

	// loadCmd Flags
	loadCmd.Flags().StringVarP(&projectID, "project", "p", "", "name of the Google Cloud project")
	loadCmd.MarkFlagRequired("project")
	rootCmd.AddCommand(loadCmd)

	// Execute commands
	rootCmd.Execute()

	// Wait for all goroutines to finish executing
	wg.Wait()
}
