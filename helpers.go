package main

import (
	"log"
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
	blob := strings.TrimLeft(filepath.Join(base, filename), "/")
	return blob
}
