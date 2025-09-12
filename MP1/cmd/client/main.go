package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Query struct {
	Args []string
}

var hosts = []string{
	"fa25-cs425-b601.cs.illinois.edu",
	"fa25-cs425-b602.cs.illinois.edu",
	"fa25-cs425-b603.cs.illinois.edu",
	"fa25-cs425-b604.cs.illinois.edu",
	"fa25-cs425-b605.cs.illinois.edu",
	"fa25-cs425-b606.cs.illinois.edu",
	"fa25-cs425-b607.cs.illinois.edu",
	"fa25-cs425-b608.cs.illinois.edu",
	"fa25-cs425-b609.cs.illinois.edu",
	"fa25-cs425-b610.cs.illinois.edu",
}

const (
	serverPort        = 9487
	connectionTimeout = 2 * time.Second
	callTimeout       = 5 * time.Second
)

func asyncCallWithTimeout(
	hostname string,
	waitGroup *sync.WaitGroup,
	query Query,
	result *[]string,
) error {
	waitGroup.Add(1)
	go func() {
		// use wait group to wait all async calls
		defer waitGroup.Done()

		// create timeout dial
		conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, serverPort), connectionTimeout)
		if err != nil {
			fmt.Printf("Fail to dial server %s: %s\n", hostname, err.Error())
			return // exit goroutine when connection fails
		}
		defer conn.Close()

		client := rpc.NewClient(conn)
		callChan := make(chan error, 1)

		go func() {
			// make blocking call on remote function
			callChan <- client.Call("Grep.Grep", query, result)
		}()

		select {
		case err := <-callChan:
			if err != nil {
				fmt.Printf("RPC call to server %s failed: %s\n", hostname, err.Error())
			}
		case <-time.After(callTimeout):
			fmt.Printf("RPC call to server %s timed out\n", hostname)
		}
	}()
	return nil
}

func main() {
	fmt.Println(len(os.Args), os.Args)
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <grep-args>")
		fmt.Println("Example: go run client.go -e 'error'")
		fmt.Println("Example: go run client.go -i 'warning'")
		os.Exit(1)
	}

	// create query - server will determine filename from hostname
	query := Query{
		Args: os.Args[1:], // All args are grep options
	}

	waitGroup := new(sync.WaitGroup)
	results := make([][]string, len(hosts))
	for i, hostname := range hosts {
		// Server will determine filename from its own hostname
		asyncCallWithTimeout(hostname, waitGroup, query, &results[i])
	}
	waitGroup.Wait() // wait every call to complete

	// output results with line counts
	totalMatches := 0
	for i, buf := range results {
		if len(buf) > 0 {
			fmt.Printf("=== %s (%d matches) ===\n", hosts[i], len(buf))
			for _, line := range buf {
				fmt.Printf("%s\n", line)
			}
			totalMatches += len(buf)
		} else {
			fmt.Printf("=== %s (0 matches) ===\n", hosts[i])
		}
	}

	// Print summary
	fmt.Printf("\n=== SUMMARY ===\n")
	fmt.Printf("Total matches across all machines: %d\n", totalMatches)
}
