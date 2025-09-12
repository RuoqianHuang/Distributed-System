package caller

import (
	"os"
	"fmt"
	"sync"
	"testing"
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
	lines := 100000
	length := 250
	for _, hostname := range caller.HOSTS {
		machineNumber := utils.GetMachineNumber(hostname)
		filename := fmt.Sprintf("%s/machine.%s.log", tempDir, machineNumber)
		utils.GenerateRandomdLogFile(lines, length, filename)
	}
	return 
	// send generated files to servers
	waitGroup := new(sync.WaitGroup)
	for _, hostname := range caller.HOSTS {
		machineNumber := utils.GetMachineNumber(hostname)
		filename := fmt.Sprintf("%s/machine.%s.log", tempDir, machineNumber)
		desPath := fmt.Sprintf("/cs425/%s", filename)
		srcPath := fmt.Sprintf("%s/%s", tempDir, filename)

		waitGroup.Add(1)
		go utils.SendFile(hostname, desPath, srcPath, waitGroup)
	}
	waitGroup.Wait()

	t.Logf("All files are sent.")


	// clean up
	os.RemoveAll(tempDir)
}