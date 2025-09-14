package main

import (
	"fmt"
	"os"
	"cs425/mp1/internal/utils"
	"cs425/mp1/internal/caller"
)


func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <grep-args>")
		fmt.Println("Example: go run client.go -e 'error'")
		fmt.Println("Example: go run client.go -i 'warning'")
		os.Exit(1)
	}

	// create query - server will determine filename from hostname
	query := utils.Query{
		Args: os.Args[1:], // All args are grep options
	}

	results, errs := caller.ClientCall(query)
	for _, err := range errs {
		fmt.Printf("%s\n", err.Error())
	}

	// output results with line counts
	totalMatches := 0
	for i, buf := range results {
		if len(buf) > 0 {
			fmt.Printf("=== %s (%d matches) ===\n", caller.HOSTS[i], len(buf))
			for _, line := range buf {
				fmt.Printf("%s\n", line)
			}
			totalMatches += len(buf)
		} else {
			fmt.Printf("=== %s (0 matches) ===\n", caller.HOSTS[i])
		}
	}

	// Print summary
	fmt.Printf("\n=== SUMMARY ===\n")
	fmt.Printf("Total matches across all machines: %d\n", totalMatches)
}
