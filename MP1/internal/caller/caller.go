package caller

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
	"errors"
	"cs425/mp1/internal/utils"
)

// server hostnames
var HOSTS = []string{
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
	SERVER_PORT        = 9487
	CONNECTION_TIMEOUT =  2 * time.Second
	CALL_TIMEOUT       = 10 * time.Second
)

func asyncCallWithTimeout(
	hostname string,
	waitGroup *sync.WaitGroup,
	query utils.Query,
	result *[]string,
	chanError chan<- error) {
	
	// use wait group to wait all async calls
	defer waitGroup.Done()

	// create timeout dial
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, SERVER_PORT), CONNECTION_TIMEOUT)
	if err != nil {
		chanError <- errors.New(fmt.Sprintf("Fail to dial server %s: %s", hostname, err.Error()))
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
				chanError <- errors.New(fmt.Sprintf("RPC call to server %s failed: %s", hostname, err.Error()))				
			}
		case <-time.After(CALL_TIMEOUT):
			chanError <- errors.New(fmt.Sprintf("RPC call to server %s timed out", hostname))
	}
}

func ClientCall(query utils.Query) ([][]string, []error) {

	waitGroup := new(sync.WaitGroup)
	results := make([][]string, len(HOSTS))
	chanError := make(chan error, len(HOSTS))

	for i, hostname := range HOSTS {
		// Server will determine filename from its own hostname
		waitGroup.Add(1)
		go asyncCallWithTimeout(hostname, waitGroup, query, &results[i], chanError)
	}
	waitGroup.Wait() // wait every call to complete

	close(chanError)

	errs := make([]error, 0)
	for err := range chanError {
		if err != nil {
			errs = append(errs, err)
		}
	}

	return results, errs
}
