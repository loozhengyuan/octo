package gcp

import (
	"context"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/storage"
)

// Bucket is a bucket object in Google Cloud Storage
type Bucket struct {
	Client *storage.Client
	Ctx    *context.Context
	Name   string
}

// NewBucket returns a Bucket object type
func NewBucket(ctx *context.Context, project, bucket string) (*Bucket, error) {

	// Create new client
	client, err := storage.NewClient(*ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to create client: %w", err)
	}

	// Create Bucket
	b := &Bucket{
		Client: client,
		Ctx:    ctx,
		Name:   bucket,
	}
	return b, nil
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
	wc := b.Client.Bucket(b.Name).Object(blob).NewWriter(*b.Ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}
