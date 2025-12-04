package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Count By Column Operator (Aggregate)
// Groups by Nth column and counts lines in each group
// Usage: count_by_column_op <N>
// Protocol:
//   Input:  key: <key>\nvalue: <value>\n
//   Output: forward\nkey: <column_value>\nvalue: <count>\n
// Stateful: Maintains counts across invocations
// Notes:
//   - Fields are separated by commas
//   - Missing data (empty field) is treated as empty string (separate key)
//   - Single space ' ' is treated as separate key
//   - Column index is 1-based (first column is 1)

type CountState struct {
	counts map[string]int
	mu     sync.Mutex
}

func NewCountState() *CountState {
	return &CountState{
		counts: make(map[string]int),
	}
}

func (cs *CountState) Increment(key string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.counts[key]++
}

func (cs *CountState) GetCount(key string) int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.counts[key]
}

func extractNthColumn(line string, n int) string {
	// Split by comma
	fields := strings.Split(line, ",")
	
	// Convert to 0-based index
	index := n - 1
	
	// Check if column exists
	if index < 0 || index >= len(fields) {
		// Missing data - return empty string
		return ""
	}
	
	// Get the column value (don't trim - preserve spaces)
	columnValue := fields[index]
	
	// Return as-is (empty string, single space ' ', or actual value)
	return columnValue
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <N>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Groups by Nth column (1-based) and counts lines in each group\n")
		os.Exit(1)
	}
	
	n, err := strconv.Atoi(os.Args[1])
	if err != nil || n < 1 {
		fmt.Fprintf(os.Stderr, "Error: N must be a positive integer\n")
		os.Exit(1)
	}
	
	state := NewCountState()
	scanner := bufio.NewScanner(os.Stdin)
	
	for scanner.Scan() {
		// Read key line: "key: <key>"
		keyLine := scanner.Text()
		if !strings.HasPrefix(keyLine, "key: ") {
			fmt.Fprintf(os.Stderr, "Error: expected 'key: ' prefix, got: %s\n", keyLine)
			continue
		}
		
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
		
		// Extract Nth column from the line
		columnValue := extractNthColumn(line, n)
		
		// Increment count for this column value
		state.Increment(columnValue)
		
		// Output current count for this column value
		count := state.GetCount(columnValue)
		fmt.Println("forward")
		fmt.Printf("key: %s\n", columnValue)
		fmt.Printf("value: %d\n", count)
	}
	
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

