package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
)

// Count Operator (Aggregate)
// Counts occurrences of each key
// Usage: count_op
// Protocol:
//   Input:  key: <key>\nvalue: <value>\n
//   Output: forward\nkey: <key>\nvalue: <count>\n
// Stateful: Maintains counts across invocations

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

func main() {
	state := NewCountState()
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
		// Note: We ignore the input value for count operator, we only care about the key
		
		// Increment count for this key
		state.Increment(key)
		
		// Output current count for this key
		count := state.GetCount(key)
		fmt.Println("forward")
		fmt.Printf("key: %s\n", key)
		fmt.Printf("value: %d\n", count)
	}
	
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
		os.Exit(1)
	}
}

