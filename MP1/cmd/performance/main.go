package main

import (
	"fmt"
	"time"
	"math"
	"cs425/mp1/internal/caller"
	"cs425/mp1/internal/utils"
)

type PerformanceResult struct {
	PatternType string
	Pattern     string
	Trials      []time.Duration
	Average     time.Duration
	StdDev      time.Duration
}

// Query only 4 machines (first 4 VMs) for performance testing
func queryFourMachines(query utils.Query) ([][]string, error) {
	// Call all machines but only use results from first 4
	allResults, errs := caller.ClientCall(query)
	for _, err := range errs {
		return nil, err
	}
	
	// Return only the first 4 results (vm1.log to vm4.log)
	return allResults[:4], nil
}

func main() {
	fmt.Println("==========================================")
	fmt.Println("Distributed Log Querier Performance Tests")
	fmt.Println("==========================================")
	fmt.Println("Using 4 VMs (vm1.log to vm4.log, 60MB each)")
	fmt.Println("Running 5 trials per pattern type")
	fmt.Println("")

	// Define test patterns based on actual vm1.log content
	testPatterns := []struct {
		name    string
		pattern string
		isRegex bool
	}{
		{"Frequent", "DELETE", false}, 
		{"Infrequent", "59.83.180.72", false}, 
		{"Regex", "404 [0-9]+", true},
	}

	var results []PerformanceResult

	// Run performance tests
	for _, test := range testPatterns {
		fmt.Printf("Testing %s pattern: %s\n", test.name, test.pattern)
		result := runPerformanceTest(test.name, test.pattern, test.isRegex)
		results = append(results, result)
		fmt.Println("")
	}

	// Print summary table
	printSummaryTable(results)
	
	// Print detailed results
	printDetailedResults(results)
}

func runPerformanceTest(testName, pattern string, isRegex bool) PerformanceResult {
	fmt.Printf("Running 5 trials...\n")
	
	// Prepare query
	var query utils.Query
	query.Filename = "vm*.log"
	if isRegex {
		query = utils.Query{
			Args: []string{"-E", pattern},
		}
	} else {
		query = utils.Query{
			Args: []string{pattern},
		}
	}
	
	// Run 5 trials
	var trials []time.Duration
	for i := 0; i < 5; i++ {
		fmt.Printf("  Trial %d/5... ", i+1)
		
		start := time.Now()
		results, err := queryFourMachines(query)
		latency := time.Since(start)
		
		if err != nil {
			fmt.Printf("FAILED: %v\n", err)
			continue
		}
		
		// Count total matches
		totalMatches := 0
		for _, result := range results {
			totalMatches += len(result)
		}
		
		trials = append(trials, latency)
		fmt.Printf("SUCCESS - %v (%d matches)\n", latency, totalMatches)
		
		// Small delay between trials
		time.Sleep(100 * time.Millisecond)
	}
	
	if len(trials) == 0 {
		fmt.Printf("All trials failed for pattern: %s\n", pattern)
		return PerformanceResult{
			PatternType: testName,
			Pattern:     pattern,
			Trials:      []time.Duration{},
		}
	}
	
	// Calculate statistics
	avg := calculateAverage(trials)
	stdDev := calculateStandardDeviation(trials, avg)
	
	return PerformanceResult{
		PatternType: testName,
		Pattern:     pattern,
		Trials:      trials,
		Average:     avg,
		StdDev:      stdDev,
	}
}

func calculateAverage(trials []time.Duration) time.Duration {
	var total time.Duration
	for _, trial := range trials {
		total += trial
	}
	return total / time.Duration(len(trials))
}

func calculateStandardDeviation(trials []time.Duration, avg time.Duration) time.Duration {
	var sumSquaredDiffs float64
	for _, trial := range trials {
		diff := float64(trial.Nanoseconds() - avg.Nanoseconds())
		sumSquaredDiffs += diff * diff
	}
	variance := sumSquaredDiffs / float64(len(trials))
	stdDev := time.Duration(int64(math.Sqrt(variance)))
	return stdDev
}

func printSummaryTable(results []PerformanceResult) {
	fmt.Println("==========================================")
	fmt.Println("PERFORMANCE SUMMARY")
	fmt.Println("==========================================")
	fmt.Printf("%-12s | %-15s | %-15s | %-15s\n", "Pattern Type", "Avg Latency (ms)", "Std Dev (ms)", "Success Rate")
	fmt.Println("-------------|-----------------|-----------------|-----------------")
	
	for _, result := range results {
		avgMs := float64(result.Average.Nanoseconds()) / 1e6
		stdDevMs := float64(result.StdDev.Nanoseconds()) / 1e6
		successRate := float64(len(result.Trials)) / 5.0 * 100
		
		fmt.Printf("%-12s | %-15.1f | %-15.1f | %-15.1f%%\n", 
			result.PatternType, avgMs, stdDevMs, successRate)
	}
	fmt.Println("")
}

func printDetailedResults(results []PerformanceResult) {
	fmt.Println("==========================================")
	fmt.Println("DETAILED RESULTS")
	fmt.Println("==========================================")
	
	for _, result := range results {
		fmt.Printf("\n%s Pattern (%s):\n", result.PatternType, result.Pattern)
		fmt.Printf("  Successful Trials: %d/5\n", len(result.Trials))
		
		if len(result.Trials) > 0 {
			fmt.Printf("  Average Latency: %v (%.1f ms)\n", result.Average, float64(result.Average.Nanoseconds())/1e6)
			fmt.Printf("  Standard Deviation: %v (%.1f ms)\n", result.StdDev, float64(result.StdDev.Nanoseconds())/1e6)
			
			fmt.Printf("  Individual Trials: ")
			for i, trial := range result.Trials {
				if i > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%.1fms", float64(trial.Nanoseconds())/1e6)
			}
			fmt.Println()
		} else {
			fmt.Printf("  All trials failed\n")
		}
	}
}
