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

func TestRandomString(t *testing.T) {
	t.Logf("Preparing log files with random lines")
	tempDir, err := os.MkdirTemp("", "MP1-test-random-lines-*")
	if err != nil {
		t.Fatalf("Fail to create temp directory for log files: %s\n", err.Error())
	}

	// generate random files
	lines := 10000
	length := 250
	for _, hostname := range caller.HOSTS {
		machineNumber := utils.GetMachineNumber(hostname)
		filename := fmt.Sprintf("machine.%s.log", machineNumber)
		desPath := filepath.Join(tempDir, filename)
		err := utils.GenerateRandomdLogFile(lines, length, desPath)
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
		
		desPath := fmt.Sprintf("/cs425/%s", filename)
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

	// query pattern
	query := utils.Query{
		Args: []string{"abc"},
	}

	// run client grep to test 
	start_time := time.Now()
	results_remote, err := caller.ClientCall(query)	
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Logf("Remote grep took %v\n", time.Since(start_time))

	// run local grep to verify results
	results_local  := make([][]string, len(caller.HOSTS))
	for i, hostname := range caller.HOSTS {
		machineNumber := utils.GetMachineNumber(hostname)
		filename := fmt.Sprintf("machine.%s.log", machineNumber)
		srcPath := filepath.Join(tempDir, filename)

		err := utils.GrepFile(srcPath, &results_local[i], query)
		if(err != nil) {
			t.Fatalf("Fail to grep local file %s. Error: %s\n", srcPath, err.Error())
		}
		// verify result
		if len(results_remote[i]) != len(results_local[i]) {
			t.Fatalf("Results on machine %s did not match. Expecting %d lines, but found %d lines\n", hostname, len(results_local[i]), len(results_remote[i]))
		}
	}

	// clean up
	os.RemoveAll(tempDir)
}