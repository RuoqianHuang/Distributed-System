package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Grep Operator (Filter)
// Filters tuples where value contains the specified pattern
// Usage: grep_op <pattern>
// Protocol:
//   Input:  key: <key>\nvalue: <value>\n
//   Output: filter (if no match) OR forward\nkey: <key>\nvalue: <value>\n (if match)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <pattern>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Filters tuples where value contains the pattern\n")
		os.Exit(1)
	}
	
	pattern := os.Args[1]
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
		value := strings.TrimPrefix(valueLine, "value: ")
		
		// Filter: output "filter" if no match, "forward" with key/value if match
		if strings.Contains(value, pattern) {
			fmt.Println("forward")
			fmt.Printf("key: %s\n", key)
			fmt.Printf("value: %s\n", value)
		} else {
			fmt.Println("filter")
		}
	}
	
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

