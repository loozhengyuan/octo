package main

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Helper method to get files based on pattern
func getFiles(patterns []string) ([]string, error) {

	// Loop through every pattern
	matchMap := make(map[string]bool)
	for _, pattern := range patterns {
		// Find files based on pattern
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}

		// Only append valid files
		for _, match := range matches {
			if f, _ := os.Stat(match); !f.IsDir() {
				// TODO: Error handling for os.Stat()
				matchMap[match] = true
			}
		}
	}

	// Returns an array of keys
	files := make([]string, 0, len(matchMap))
	for k := range matchMap {
		files = append(files, k)
	}
	return files, nil
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
