package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/loozhengyuan/octo/api/gcp"
	"github.com/spf13/cobra"
)

var (
	// Flag Vars
	projectID      string
	storageBucket  string
	pubSubTopic    string
	searchPattern  string
	blobPrefix     string
	workerNodes    int
	autoDecompress bool

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

	// Sub Command - octo load
	loadCmd = &cobra.Command{
		Use:   "load gs://<your_uri>",
		Short: "Load files from Storage to BigQuery",
		Args:  cobra.ExactValidArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			loadFromGcs(args[0])
		},
	}
)

func loadFromGcs(uri string) {
	// Temp vars
	n := 1
	dataset := "Intel"
	table := "test"

	// Create ctx variable
	ctx := context.Background()

	// Create table object
	log.Printf("Worker #%v: Creating table object", n)
	t, err := gcp.NewTable(&ctx, projectID, dataset, table)
	if err != nil {
		log.Fatalf("Worker #%v: Failed to create new table object: %v", n, err)
	}

	// Load
	log.Printf("Worker #%v: Loading data", n)
	if err := t.LoadFromGcs(uri); err != nil {
		log.Fatalf("Worker #%v: Failed to load data: %v", n, err)
	}
}

func uploadFile(n int, file string) error {
	// Create ctx variable
	ctx := context.Background()

	// Decompress gzip file is applicable and desired
	if ext := filepath.Ext(file); ext == ".gz" && autoDecompress == true {
		newFileName := strings.TrimSuffix(file, ext)
		log.Printf("Worker #%v: Uncompressing %s to %s", n, file, newFileName)
		err := UncompressGzipFile(file, newFileName)
		if err != nil {
			log.Fatalf("Worker #%v: Error uncompressing file %s: %v", n, file, err)
		}
		file = newFileName
	}

	// Create blob format
	blob := blobFormatter(blobPrefix, file)

	// Create bucket object
	log.Printf("Worker #%v: Creating bucket object", n)
	b, err := gcp.NewBucket(&ctx, projectID, storageBucket)
	if err != nil {
		log.Fatalf("Worker #%v: Failed to create new bucket object: %v", n, err)
	}

	// Upload file to Storage
	log.Printf("Worker #%v: Uploading File %s to %s/%s", n, file, b.Name, blob)
	if err := b.Upload(file, blob); err != nil {
		// TODO: Log fatal while allowing other goroutines to gracefully exit
		log.Fatalf("Worker #%v: Error uploading to GCS bucket %s: %v", n, b.Name, err)
	}

	// Create topic object
	log.Printf("Worker #%v: Creating topic object", n)
	t, err := gcp.NewTopic(&ctx, projectID, pubSubTopic)
	if err != nil {
		log.Fatalf("Worker #%v: Failed to create new topic object: %v", n, err)
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
	// TODO: Remove both compressed and uncompressed files
	if err := os.Remove(file); err != nil {
		// TODO: Log fatal while allowing other goroutines to gracefully exit
		log.Fatalf("Worker #%v: Error deleting file %s: %v", n, file, err)
	}
	return nil
}

func uploadExecutor(n int, jobQueue <-chan string, callBack chan<- int) {
	for file := range jobQueue {
		// Upload file
		uploadFile(n, file)

		// Await
		callBack <- 1
	}
}

func initUpload() {
	// Create jobQueue and callBack channels
	// TODO: Consider if making non-bounded channels is viable
	jobQueue := make(chan string, 100)
	callBack := make(chan int, 100)

	// Create uploadExecutor nodes
	log.Printf("Creating %v worker nodes", workerNodes)
	for w := 0; w < workerNodes; w++ {
		go uploadExecutor(w, jobQueue, callBack)
	}

	// Get list of files
	log.Printf("Searching for files matching pattern: %s", searchPattern)
	files := getFiles(searchPattern)
	if len(files) < 1 {
		log.Fatalln("No files were found!")
	}
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

func main() {
	// upCmd Flags
	upCmd.Flags().StringVarP(&projectID, "project", "p", "", "name of the Google Cloud project")
	upCmd.Flags().StringVarP(&storageBucket, "bucket", "b", "", "name of the Storage bucket to upload")
	upCmd.Flags().StringVarP(&pubSubTopic, "topic", "t", "", "name of the Pub/Sub topic to publish")
	upCmd.Flags().StringVar(&blobPrefix, "prefix", "", "string prefix to append to the blob")
	upCmd.Flags().IntVar(&workerNodes, "workers", 10, "number of workers nodes to spawn")
	upCmd.Flags().BoolVar(&autoDecompress, "autodecompress", false, "number of workers nodes to spawn")
	upCmd.MarkFlagRequired("project")
	upCmd.MarkFlagRequired("bucket")
	upCmd.MarkFlagRequired("topic")
	rootCmd.AddCommand(upCmd)

	// loadCmd Flags
	loadCmd.Flags().StringVarP(&projectID, "project", "p", "", "name of the Google Cloud project")
	// upCmd.Flags().StringVarP(&storageBucket, "bucket", "b", "", "name of the Storage bucket to upload")
	// upCmd.Flags().StringVarP(&pubSubTopic, "topic", "t", "", "name of the Pub/Sub topic to publish")
	// upCmd.Flags().StringVar(&blobPrefix, "prefix", "", "string prefix to append to the blob")
	// upCmd.Flags().IntVar(&workerNodes, "workers", 10, "number of workers nodes to spawn")
	// upCmd.Flags().BoolVar(&autoDecompress, "autodecompress", false, "number of workers nodes to spawn")
	loadCmd.MarkFlagRequired("project")
	// upCmd.MarkFlagRequired("bucket")
	// upCmd.MarkFlagRequired("topic")
	rootCmd.AddCommand(loadCmd)

	// Execute commands
	rootCmd.Execute()
}
