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

func test(rate float64, mode Args) {

	log.Printf("Wait 10s for every thing to become stable.")
	startTime := time.Now() // wait 10s for every thing to become stable
	for {
		if time.Since(startTime) > 10*time.Second {
			break
		}
	}

	ticker := time.NewTicker(time.Second / 10)
	log.Printf("Start experimenting with drop rate %f\n", rate)
	// start testing for 30 s
	startTime = time.Now()

	var failure_map map[uint64]member.Info = make(map[uint64]member.Info)
	for range ticker.C {

		// set drop rate and mode
		for _, hostname := range utils.HOSTS {
			result := new(string)
			args := Args{
				Command: "set_drop_rate",
				Rate:    rate,
			}
			CallWithTimeout(hostname, 12345, args, result)
			CallWithTimeout(hostname, 12345, mode, result)
			// log.Printf("Set mode, Hostname: %s: %s\n", hostname, *result)
		}

		currentTime := time.Now()
		if currentTime.Sub(startTime) > time.Minute/2 {
			break
		}
		for _, hostname := range utils.HOSTS {
			result := new(map[uint64]member.Info)
			args := Args{}
			getTableWithTimeout(hostname, 12345, args, result)
			for id, info := range *result {
				// log.Printf("failed %v\n", info)
				if info.State == member.Failed {
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

	for _, rate := range []float64{0.2, 0.3, 0.4, 0.5, 0.6, 0.7} {
		test(rate, mode)
	}

}
