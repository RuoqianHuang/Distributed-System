package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Extract Fields Operator (Transform)
// Extracts first N fields from a comma-separated line
// Usage: extract_fields_op <N>
// Protocol:
//   Input:  key: <key>\nvalue: <value>\n
//   Output: forward\nkey: <key>\nvalue: <first_N_fields>\n
// Notes:
//   - Fields are separated by commas
//   - If line has fewer than N fields, output all available fields
//   - Preserves original key

func extractFirstNFields(line string, n int) string {
	// Split by comma
	fields := strings.Split(line, ",")
	
	// Take first N fields (or all if fewer than N)
	if n > len(fields) {
		n = len(fields)
	}
	
	// Join first N fields with commas
	result := strings.Join(fields[:n], ",")
	
	return result
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <N>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Extracts first N fields from comma-separated line\n")
		os.Exit(1)
	}
	
	n, err := strconv.Atoi(os.Args[1])
	if err != nil || n < 1 {
		fmt.Fprintf(os.Stderr, "Error: N must be a positive integer\n")
		os.Exit(1)
	}
	
	scanner := bufio.NewScanner(os.Stdin)
	
	for scanner.Scan() {
		// Read key line: "key: <key>"
		keyLine := scanner.Text()
		if !strings.HasPrefix(keyLine, "key: ") {
			fmt.Fprintf(os.Stderr, "Error: expected 'key: ' prefix, got: %s\n", keyLine)
			continue
		}
		key := strings.TrimPrefix(keyLine, "key: ")
		
		// Read value line: "value: <value>"
		if !scanner.Scan() {
			fmt.Fprintf(os.Stderr, "Error: expected 'value: ' line after key\n")
			break
		}
		valueLine := scanner.Text()
		if !strings.HasPrefix(valueLine, "value: ") {
			fmt.Fprintf(os.Stderr, "Error: expected 'value: ' prefix, got: %s\n", valueLine)
			continue
		}
		line := strings.TrimPrefix(valueLine, "value: ")
		
		// Extract first N fields
		extractedFields := extractFirstNFields(line, n)
		
		// Output with original key and extracted fields
		fmt.Println("forward")
		fmt.Printf("key: %s\n", key)
		fmt.Printf("value: %s\n", extractedFields)
	}
	
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

