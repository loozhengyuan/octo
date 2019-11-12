package gcp

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// Table is a Google BigQuery table object
type Table struct {
	Client  *bigquery.Client
	Ctx     *context.Context
	Dataset string
	Table   string
}

// LoadFromGcs is a method to upload files Google Cloud Storage
func (t *Table) LoadFromGcs(uri string) error {

	// Configure source file in GCS
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.AutoDetect = true
	gcsRef.SkipLeadingRows = 1

	// Configure load job
	loader := t.Client.Dataset(t.Dataset).Table(t.Table).LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteTruncate

	// Execute and await job
	job, err := loader.Run(*t.Ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(*t.Ctx)
	if err != nil {
		return err
	}

	// Return error if job completed but with erred
	if status.Err() != nil {
		return fmt.Errorf("job completed with error: %v", status.Err())
	}
	return nil
}
