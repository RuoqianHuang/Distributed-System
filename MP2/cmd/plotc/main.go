package main

import (
	"cs425/mp2/internal/member"
	"cs425/mp2/internal/utils"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

// arguments for cli tool
type Args struct {
	Command string
	Rate    float64
}

func CallWithTimeout(
	hostname string,
	port int,
	args Args,
	result *string) {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		// log.Printf("Failed to dial server %s:%d: %s\n", hostname, port, err.Error())
		return
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call("Server.CLI", args, result)
	}()
	select {
	case err := <-callChan:
		if err != nil {
			// log.Printf("RPC call to server %s:%d failed: %s\n", hostname, port, err.Error())
		}
	case <-time.After(CALL_TIMEOUT):
		// log.Printf("RPC call to server %s:%d timed out\n", hostname, port)
	}
}

func getTableWithTimeout(
	hostname string,
	port int,
	args Args,
	result *map[uint64]member.Info) {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		// log.Printf("Failed to dial server %s:%d: %s\n", hostname, port, err.Error())
		return
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call("Server.MemberTable", args, result)
	}()
	select {
	case err := <-callChan:
		if err != nil {
			// log.Printf("RPC call to server %s:%d failed: %s\n", hostname, port, err.Error())
		}
	case <-time.After(CALL_TIMEOUT):
		// log.Printf("RPC call to server %s:%d timed out\n", hostname, port)
	}

}

func test(numOfFailure int, mode Args) {
	// restart server
	for _, hostname := range utils.HOSTS[1:] {
		result := new(string)
		args := Args{
			Command: "stop",
		}
		CallWithTimeout(hostname, 12345, args, result)
		log.Printf("restart server, Hostname: %s\n", hostname)
		startTime := time.Now() // wait 3s
		for {
			if time.Since(startTime) > 3*time.Second {
				break
			}
		}
	}
	
	startTime := time.Now() // wait 10s for every thing to become stable
	for {
		if time.Since(startTime) > 10 * time.Second {
			break
		}
	}

	for i := 0; i < numOfFailure; i++ {
		// let machine utils.HOST[9 - i] leave the group
		args := Args{
			Command: "leave",
		}
		result := new(string)
		CallWithTimeout(utils.HOSTS[9 - i], 12345, args, result)
		log.Printf("Let %s leave the group: %s", utils.HOSTS[9 - i], *result)
	}
	
	ticker := time.NewTicker(time.Second / 10)
	log.Printf("Start experimenting with %d simultaneous failures\n", numOfFailure)
	// collect result for 10 s
	startTime = time.Now()

	var failure_map map[uint64]member.Info = make(map[uint64]member.Info)
	for range ticker.C {

		// set mode
		for _, hostname := range utils.HOSTS {
			result := new(string)
			CallWithTimeout(hostname, 12345, mode, result)
		}

		if time.Since(startTime) > time.Second * 10 {
			break
		}

		for _, hostname := range utils.HOSTS {
			result := new(map[uint64]member.Info)
			args := Args{}
			getTableWithTimeout(hostname, 12345, args, result)
			for id, info := range *result {
				// log.Printf("failed %v\n", info)
				_, ok := failure_map[id]
				if info.State == member.Failed && !ok {
					log.Printf("Node %s:%d failed after %v", info.Hostname, info.Port, time.Since(startTime))
					failure_map[id] = info
				}
			}
		}
		
	}
	log.Printf("Total failure count: %d\n", len(failure_map))
}

func main() {

	if len(os.Args) < 3 {
		log.Printf("Usage: ./bin/plota {gossip,ping} {suspect,nosuspect}")
	}
	protocol := os.Args[1]
	sus := os.Args[2]

	mode := Args{
		Command: fmt.Sprintf("switch(%s,%s)", protocol, sus),
	}

	for _, rate := range []float64{0.0, 0.1, 0.2, 0.3, 0.4, 0.5} {
		test(rate, mode)
	}

}
