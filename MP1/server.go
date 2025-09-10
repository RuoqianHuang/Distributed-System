package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

type Query struct {
	Filename string
	Args     []string
}

const (
	serverPort int32 = 9487
)

func getHostName() string {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	return name
}

func getMachineNumber(hostname string) string {
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

type Grep struct{}

func (g *Grep) Grep(query Query, result *[]string) error {
	// Get hostname and extract machine number
	hostname := getHostName()
	machineNumber := getMachineNumber(hostname)
	filename := fmt.Sprintf("machine.%s.log", machineNumber)

	log.Printf("Received query with args %v, using filename %s (hostname: %s)\n", query.Args, filename, hostname)

	// read file content
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Fail to open file %s: %s", filename, err.Error())
		return err
	}
	defer file.Close()

	// create grep command to run and pipe the file to it
	Args := []string{"--color=always"}
	Args = append(Args, query.Args...)
	exe := exec.Command("grep", Args...)
	exe.Stdin = file

	// run the grep command and receive result with buffer
	log.Printf("Run command %s", exe.String())
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
		log.Printf("Command failed: %v: %s", err, stderr_buf.String())
		return err
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

func main() {
	// handle syscall SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("Exiting...\n")
		os.Exit(0)
	}()

	// setup logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// get host name
	hostname := getHostName()

	// register service
	grep := new(Grep)
	rpc.Register(grep)

	// starting server
	log.Printf("Starting server on node %s:%d...\n", hostname, serverPort)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		log.Fatal("Error creating listener: ", err.Error())
	}
	rpc.Accept(listener)
}
