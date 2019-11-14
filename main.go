package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/loozhengyuan/octo/api/gcp"
	"github.com/loozhengyuan/octo/utils"
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

				// TODO: Stat file or match glob pattern

				// Process path
				log.Printf("Processing: %s", path)
				go func(n int, file string) {

					// Decrement the counter when the goroutine completes.
					defer wg.Done()

					// Check if extension is meant to be excluded
					log.Printf("Worker #%v: Checking for valid file extension", n)
					if ext := filepath.Ext(file); ext == ".part" {
						log.Printf("Worker #%v: File %s ending with .part is not valid", n, b.Name)
						return
					}

					// Create blob format
					blob := blobFormatter(blobPrefix, file)

					// Upload file to Storage
					log.Printf("Worker #%v: Uploading File %s to %s/%s", n, file, b.Name, blob)
					if err := b.Upload(file, blob); err != nil {
						log.Printf("Worker #%v: Error uploading to GCS bucket %s: %v", n, b.Name, err)
						return
					}

					// Notify PubSub
					message := fmt.Sprintf("File %s/%s uploaded!", b.Name, blob)
					attrs := map[string]string{
						"bucket": b.Name,
						"blob":   blob,
					}
					log.Printf("Worker #%v: Publishing File %s to Pub/Sub topic: %s", n, file, t.Name)
					if _, err := t.Publish(message, attrs); err != nil {
						log.Printf("Worker #%v: Error publishing to Pub/Sub topic %s: %v", n, t.Name, err)
						return
					}

					// Delete file before terminating
					if err := os.Remove(file); err != nil {
						log.Printf("Worker #%v: Error deleting file %s: %v", n, file, err)
						return
					}

					// Log success
					log.Printf("Worker #%v: File %s was successfully uploaded!", n, file)
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
				log.Printf("Processing: %s", uri)
				go func(worker int, file string) {
					// Decrement the counter when the goroutine completes.
					defer wg.Done()

					// Load
					log.Printf("Worker #%v: Loading data", worker)
					if err := t.LoadFromGcs(file); err != nil {
						log.Printf("Worker #%v: Failed to load data: %v", worker, err)
						return
					}

					// Log success
					log.Printf("Worker #%v: Blob %s was successfully uploaded!", worker, file)
				}(i, uri)
			}
		},
	}

	// Sub Command - octo prep
	prepCmd = &cobra.Command{
		Use:   "prep file1 file2",
		Short: "Uncompresses compressed files",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {

			// Launch goroutine for every file
			for i, uri := range args {
				// Increment the WaitGroup counter.
				wg.Add(1)

				// Process uri
				log.Printf("Processing: %s", uri)
				go func(worker int, file string) {
					// Decrement the counter when the goroutine completes.
					defer wg.Done()

					// Handle file
					switch {
					case strings.HasSuffix(file, ".tar.gz"):
						log.Printf("Worker #%v: Exploding file %s", worker, file)

						// Get file handler
						fp, err := os.Open(file)
						if err != nil {
							log.Printf("Worker #%v: Error opening file %s: %v", worker, file, err)
							return
						}
						defer fp.Close()

						// Untar file
						// TODO: Clean up helper function
						// TODO: Standardise log messages (?)
						destinationDir := strings.TrimRight(file, ".tar.gz")
						if err := utils.Untar(fp, destinationDir); err != nil {
							log.Printf("Worker #%v: Error decompressing file %s: %v", worker, file, err)
							return
						}
					default:
						log.Printf("Worker #%v: File %s did not match any cases and will be left unhandled", worker, file)
					}

					// Log success
					log.Printf("Worker #%v: Prep process for File %s has completed", worker, file)
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

	// prepCmd Flags
	rootCmd.AddCommand(prepCmd)

	// Execute commands
	rootCmd.Execute()

	// Wait for all goroutines to finish executing
	wg.Wait()
}
