package main

import (
	"fmt"
	"log"
	"path/filepath"
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
// TODO: Consider case when prefix with leading slash
func blobFormatter(prefix, filename string) (blob string) {
	// Extract only the base filename
	base := filepath.Base(filename)

	// Formulate filename
	switch {
	case len(prefix) == 0:
		blob = base
	case prefix[len(prefix)-1:] == "/":
		blob = fmt.Sprintf("%s%s", prefix, base)
	default:
		blob = fmt.Sprintf("%s/%s", prefix, base)
	}
	return blob
}
