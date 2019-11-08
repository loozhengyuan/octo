package main

import (
	"fmt"
	"log"
	"path/filepath"
)

// Helper method to get files based on pattern
// TODO: Get directory prefix
func getFiles(pattern string) []string {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal(err)
	}
	return matches
}

// Formats prefix and filename into a Storage-compatible string
func blobFormatter(prefix, filename string) (blob string) {
	switch {
	case len(prefix) == 0:
		blob = filename
	case prefix[len(prefix)-1:] == "/":
		blob = fmt.Sprintf("%s%s", prefix, filename)
	default:
		blob = fmt.Sprintf("%s/%s", prefix, filename)
	}
	return blob
}
