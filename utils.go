package main

import (
	"compress/gzip"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// Helper method to get files based on pattern
func getFiles(pattern string) []string {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err)
	}
	return matches
}

// Formats prefix and filename into a Storage-compatible string
func blobFormatter(prefix, filename string) string {
	// Extract only the base filename
	base := filepath.Base(filename)
	// Join and clean concatenated filepath
	// Leading slash is also trimmed
	blob := strings.TrimLeft(filepath.Join(prefix, base), "/")
	return blob
}

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
