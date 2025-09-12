package utils

import (
	"os"
	"fmt"
	"log"
	"sync"
	"time"
	"bufio"
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

func GenerateRandomdLogFile(lines int, length int, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create file %s: %s\n", filename, err.Error())
		return
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for i := range lines {
		_, err := writer.WriteString(RandStringRunes(length))
		if err != nil {
			log.Printf("Failed to write line %d to file %s: %s\n", i, file, err.Error())
			return
		}
	}
	writer.Flush()
}


func SendFile(hostname string, desPath string, srcPath string, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	remotePath := fmt.Sprintf("%s:%s", hostname, desPath)
	log.Printf("Sending '%s' to %s...\n", srcPath, hostname)
	cmd := exec.Command("scp", srcPath, remotePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Failed to send to %s. Error: %s, Output: %s\n", hostname, err.Error(), string(output))
		return
	}

	log.Printf("File %s sent to %s\n", srcPath, hostname)
}

