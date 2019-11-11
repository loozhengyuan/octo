package gcp

import (
	"context"
	"io"
	"os"

	"cloud.google.com/go/storage"
)

// Bucket is a bucket object in Google Cloud Storage
type Bucket struct {
	client *storage.Client
	ctx    *context.Context
	name   string
}

// Upload is a method to upload files to a Google Cloud Storage bucket
func (b *Bucket) Upload(file, blob string) error {

	// Opens file
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	// Copy file to bucket
	wc := b.client.Bucket(b.name).Object(blob).NewWriter(*b.ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}
