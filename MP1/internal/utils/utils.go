package utils

import (
	"os"
	"fmt"
	"sync"
	"time"
	"bytes"
	"bufio"
	"errors"
	"strings"
	"os/exec"
	"math/rand"
)

// Query type for client to send and server to receive
type Query struct {
	Args []string
}

// Character for random string generation
var LETTERS = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	rng := rand.NewSource(time.Now().UnixNano())
	arr := make([]rune, n)
	for i := range arr {
		arr[i] = LETTERS[rng.Int63() % int64(len(LETTERS))]
	}
	return string(arr)
}

func GetMachineNumber(hostname string) string {
	// Extract machine number from hostname
	// Format: fa25-cs425-b6XX.cs.illinois.edu where XX is 01-10
	// Example: fa25-cs425-b605.cs.illinois.edu -> "05"

	// Look for pattern "b6XX" in the hostname
	// Split by "." first to remove domain, then look for b6XX pattern
	domainParts := strings.Split(hostname, ".")
	if len(domainParts) > 0 {
		hostPart := domainParts[0] // Get "fa25-cs425-b6XX" part

		// Look for "b6" followed by digits
		b6Index := strings.Index(hostPart, "b6")
		if b6Index != -1 && b6Index+2 < len(hostPart) {
			// Extract the two digits after "b6"
			numberPart := hostPart[b6Index+2:]
			if len(numberPart) >= 2 {
				// Take first two characters after "b6"
				number := numberPart[:2]
				// Validate it's a valid number (01-10)
				if number >= "01" && number <= "10" {
					return number
				}
			}
		}
	}

	return "01" // Default fallback
}

func GenerateRandomdLogFile(lines int, length int, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}

	defer file.Close()

	writer := bufio.NewWriter(file)
	
	for i := range lines {
		_, err := writer.WriteString(RandStringRunes(length) + "\n")
		if err != nil {
			return errors.New(fmt.Sprintf("Fail to write file to %s at line %d. Error: %s\n", filename, i + 1, err.Error()))
		}
	}
	writer.Flush()
	return nil
}

func SendFile(hostname string, desPath string, srcPath string, waitGroup *sync.WaitGroup, chanError chan<- error) {
	defer waitGroup.Done()

	remotePath := fmt.Sprintf("%s:%s", hostname, desPath)
	cmd := exec.Command("scp", srcPath, remotePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		chanError <- errors.New(fmt.Sprintf("Fail to send file to %s. Error: %s. Output: %s\n", hostname, err.Error(), output))
	} else {
		chanError <- nil
	}
}

func GrepFile(filename string, result *[]string, query Query) error {
	file, err := os.Open(filename)
	if err != nil {
		return errors.New(fmt.Sprintf("Fail to open file %s. Error: %s\n", filename, err.Error()))
	}
	defer file.Close()

	// create grep command to run and pipe the file to it
	Args := []string{"--color=always"}
	Args = append(Args, query.Args...)
	exe := exec.Command("grep", Args...)
	exe.Stdin = file

	// run the grep command and receive result with buffer
	var stdout_buf, stderr_buf bytes.Buffer
	exe.Stdout = &stdout_buf
	exe.Stderr = &stderr_buf
	err = exe.Run()

	// Check if grep found no matches (exit code 1) vs actual error
	if err != nil {
		// grep returns exit code 1 when no matches found (not an error)
		if exitError, ok := err.(*exec.ExitError); ok && exitError.ExitCode() == 1 {
			// No matches found - this is normal, not an error
			*result = []string{}
			return nil
		}
		// Actual error occurred
		return errors.New(fmt.Sprintf("grep Command failed: %v: %s\n", err, stderr_buf.String()))
	}

	// process output and return
	output := strings.TrimSpace(stdout_buf.String())
	if len(output) == 0 {
		*result = []string{}
		return nil
	}

	lines := strings.Split(output, "\n")
	// Add filename prefix to each line (like standard grep)
	for i, line := range lines {
		lines[i] = fmt.Sprintf("%s:%s", filename, line)
	}

	*result = lines
	return nil
}

	
	

	

	