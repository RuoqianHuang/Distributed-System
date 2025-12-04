package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Replace Operator (Transform)
// Replaces old pattern with new pattern in tuple values
// Usage: replace_op <old_pattern> <new_pattern>
// Protocol:
//   Input:  key: <key>\nvalue: <value>\n
//   Output: forward\nkey: <key>\nvalue: <modified_value>\n

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <old_pattern> <new_pattern>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Replaces old_pattern with new_pattern in tuple values\n")
		os.Exit(1)
	}
	
	oldPattern := os.Args[1]
	newPattern := os.Args[2]
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
		
		// Transform: replace pattern in value
		modifiedValue := strings.ReplaceAll(value, oldPattern, newPattern)
		
		// Output modified tuple
		fmt.Println("forward")
		fmt.Printf("key: %s\n", key)
		fmt.Printf("value: %s\n", modifiedValue)
	}
	
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

