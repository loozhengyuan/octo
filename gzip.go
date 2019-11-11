package main

import (
	"compress/gzip"
	"io"
	"os"
)

// UncompressGzipFile is a helper function to uncompress files
func UncompressGzipFile(source, destination string) error {

	// Get file handler
	fi, err := os.Open(source)
	if err != nil {
		return err
	}
	defer fi.Close()

	// Read gzip
	fz, _ := gzip.NewReader(fi)
	if err != nil {
		return err
	}
	defer fz.Close()

	// Create file writer
	fo, _ := os.Create(destination)
	if err != nil {
		return err
	}
	defer fo.Close()

	// Copy file
	if _, err := io.Copy(fo, fz); err != nil {
		return err
	}
	return nil
}
