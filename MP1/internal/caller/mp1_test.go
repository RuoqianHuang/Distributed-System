package caller

import (
	"os"
	"fmt"
	"sync"
	"time"
	"testing"
	"path/filepath"
	"cs425/mp1/internal/caller"
	"cs425/mp1/internal/utils"
)

func TestComprehensiveLogQuerier(t *testing.T) {
	t.Logf("Preparing comprehensive log files with known and random lines")
	tempDir, err := os.MkdirTemp("", "MP1-test-*")
	if err != nil {
		t.Fatalf("Fail to create temp directory for log files: %s\n", err.Error())
	}

	// generate comprehensive test files with known patterns
	lines := 100
	length := 50
	for i, hostname := range caller.HOSTS {
		machineNumber := utils.GetMachineNumber(hostname)
		filename := fmt.Sprintf("machine.%s.log", machineNumber)
		desPath := filepath.Join(tempDir, filename)
		err := utils.GenerateComprehensiveLogFile(lines, length, desPath, i+1)
		if err != nil {
			t.Fatalf("%s\n", err.Error())
		}
		t.Logf("Log file %s with %d lines generated\n", desPath, lines)
	}
	
	// send generated files to servers
	waitGroup := new(sync.WaitGroup)
	chanError := make(chan error, len(caller.HOSTS))
	for _, hostname := range caller.HOSTS {
		machineNumber := utils.GetMachineNumber(hostname)
		filename := fmt.Sprintf("machine.%s.log", machineNumber)
		
		desPath := fmt.Sprintf("/cs425/mp1/%s", filename)
		srcPath := filepath.Join(tempDir, filename)
		t.Logf("Sending '%s' to %s...\n", srcPath, hostname)

		waitGroup.Add(1)
		go utils.SendFile(hostname, desPath, srcPath, waitGroup, chanError)
	}
	waitGroup.Wait()

	close(chanError)
	
	for err := range chanError {
		if err != nil {
			t.Fatalf("%s\n", err.Error())
		}
	}
	t.Logf("All files are sent.")

	// Test multiple query patterns with different frequencies
	testCases := []struct {
		name        string
		pattern     string
		description string
	}{
		{"RarePattern", "RARE_PATTERN_XYZ", "Should find 0-1 matches per file"},
		{"FrequentPattern", "ERROR", "Should find many matches (appears in every file)"},
		{"SomewhatFrequent", "WARN", "Should find moderate matches (appears in some files)"},
		{"SingleMachine", "MACHINE_01_ONLY", "Should only appear in machine 01"},
		{"SomeMachines", "SHARED_PATTERN", "Should appear in machines 01-05"},
		{"AllMachines", "COMMON_LOG", "Should appear in all machines"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing pattern: %s (%s)", tc.pattern, tc.description)
			
			query := utils.Query{
				Args: []string{tc.pattern},
			}

			// run client grep to test 
			start_time := time.Now()
			results_remote, errs := caller.ClientCall(query)	
			for _, err := range errs {
				if err != nil {
					t.Fatalf("Remote grep failed: %s", err.Error())
				}
			}
			t.Logf("Remote grep took %v", time.Since(start_time))

			// run local grep to verify results
			results_local := make([][]string, len(caller.HOSTS))
			for i, hostname := range caller.HOSTS {
				machineNumber := utils.GetMachineNumber(hostname)
				filename := fmt.Sprintf("machine.%s.log", machineNumber)
				srcPath := filepath.Join(tempDir, filename)

				err := utils.GrepFile(srcPath, &results_local[i], query)
				if err != nil {
					t.Fatalf("Fail to grep local file %s. Error: %s", srcPath, err.Error())
				}
				
				// verify result
				if len(results_remote[i]) != len(results_local[i]) {
					t.Errorf("Results on machine %s did not match. Remote: %d, Local: %d", 
						hostname, len(results_remote[i]), len(results_local[i]))
				}
				
				// Log match counts for analysis
				t.Logf("Machine %s: %d matches", hostname, len(results_remote[i]))
			}

			// Verify expected behavior based on pattern type
			totalMatches := 0
			for _, result := range results_remote {
				totalMatches += len(result)
			}
			t.Logf("Total matches across all machines: %d", totalMatches)
		})
	}

	// clean up
	os.RemoveAll(tempDir)
}