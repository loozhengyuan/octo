package gcp

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"regexp"

	"cloud.google.com/go/bigquery"
)

// Table is a Google BigQuery table object
type Table struct {
	Client  *bigquery.Client
	Ctx     *context.Context
	Dataset string
	Table   string
}

// NewTable returns a Table object type
func NewTable(ctx *context.Context, project, dataset, table string) (*Table, error) {

	// Create new client
	client, err := bigquery.NewClient(*ctx, project)
	if err != nil {
		return nil, fmt.Errorf("Failed to create client: %w", err)
	}

	// Create table
	t := &Table{
		Client:  client,
		Ctx:     ctx,
		Dataset: dataset,
		Table:   table,
	}
	return t, nil
}

// LoadFromGcs is a method to upload files Google Cloud Storage
func (t *Table) LoadFromGcs(uri string) error {
	// Read schema file
	// TODO: Allow user to pass unique schema filename
	jsonSchema, err := ioutil.ReadFile("schema.json")
	if err != nil {
		return fmt.Errorf("Failed to create schema: %v", err)
	}

	// Convert JSON to bigquery schema object
	schema, err := bigquery.SchemaFromJSON(jsonSchema)
	if err != nil {
		return fmt.Errorf("Failed to parse schema: %v", err)
	}

	// Configure source file in GCS
	gcsRef := bigquery.NewGCSReference(uri)
	gcsRef.SourceFormat = bigquery.CSV
	gcsRef.Schema = schema
	gcsRef.FieldDelimiter = "\t"
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
		// status.Errors preferred over status.Err() so that full
		// array of errors are returned
		return fmt.Errorf("Job completed with error: %v", status.Errors)
	}
	return nil
}

// GetBigQuerySanitisedName returns a BigQuery-compatible
// name for use as a column name.
// Reference: https://cloud.google.com/bigquery/docs/schemas#column_names
func GetBigQuerySanitisedName(src string) (repl string, err error) {

	// Create reserved names map
	reserved := map[string]bool{
		"_TABLE_":     true,
		"_FILE_":      true,
		"_PARTITION_": true,
	}

	// Check for error
	switch {
	case len(src) > 128:
		// Returns error if length exceeds 128 characters
		return "", errors.New("Length of string exceeds 128 characters")
	case reserved[src]:
		// Returns error if string in reserved names
		return "", errors.New("String conflicts with one of BigQuery's reserved names")
	default:
		re := regexp.MustCompile(`\W`)
		repl = re.ReplaceAllString(src, "_")
		// TODO: Check if string does not start with A-Za-z\_
		return repl, nil
	}
}
