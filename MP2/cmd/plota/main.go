package main

import (
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
	result *string) error {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		return fmt.Errorf("failed to dial server %s:%d: %s", hostname, port, err.Error())
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
			return fmt.Errorf("rpc call to server %s:%d failed: %s", hostname, port, err.Error())
		}
	case <-time.After(CALL_TIMEOUT):
		return fmt.Errorf("rpc call to server %s:%d timed out", hostname, port)
	}
	return nil
}

func getFlowWithTimeout(
	hostname string,
	port int,
	args Args,
	result *float64) error {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		return fmt.Errorf("failed to dial server %s:%d: %s", hostname, port, err.Error())
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call("Server.GetTotalFlow", args, result)
	}()
	select {
	case err := <-callChan:
		if err != nil {
			return fmt.Errorf("rpc call to server %s:%d failed: %s", hostname, port, err.Error())
		}
	case <-time.After(CALL_TIMEOUT):
		return fmt.Errorf("rpc call to server %s:%d timed out", hostname, port)
	}
	return nil
}

func test(n int, mode Args) {
	// restart all nodes
	for i, hostname := range utils.HOSTS {
		result := new(string)
		args := Args{
			Command: "stop",
		}
		CallWithTimeout(hostname, 12345, args, result)
		// log.Printf("Restart %s\n", hostname)

		if i == 0 {
			currentTime := time.Now()
			// wait 3s only for the first one
			for {
				if time.Since(currentTime) > 3*time.Second {
					break
				}
			}
		}
	}

	// set mode and drop rate to 0
	for _, hostname := range utils.HOSTS {
		result := new(string)
		args := Args{
			Command: "set_drop_rate",
			Rate:    0.0,
		}
		CallWithTimeout(hostname, 12345, args, result)
		CallWithTimeout(hostname, 12345, mode, result)
	}

	log.Printf("Wait 10s for every thing to become stable.")
	startTime := time.Now() // wait 10s for every thing to become stable
	for {
		if time.Since(startTime) > 10 * time.Second {
			break
		}
	}

	offHosts := utils.HOSTS[n:]
	curHosts := utils.HOSTS[:n]

	// set mode and drop rate to 0
	for _, hostname := range curHosts {
		result := new(string)
		args := Args{
			Command: "set_drop_rate",
			Rate:    0.0,
		}
		CallWithTimeout(hostname, 12345, args, result)
		CallWithTimeout(hostname, 12345, mode, result)
	}
	// let off nodes leave and halt
	for _, hostname := range offHosts {
		result := new(string)
		args := Args{
			Command: "leave",
		}
		CallWithTimeout(hostname, 12345, args, result)
		log.Printf("Let %s leave\n", hostname)
	}

	ticker := time.NewTicker(time.Second / 10)
	log.Printf("Start experimenting with %d machines for 10s\n", n)
	// start testing for 10 s
	startTime = time.Now()

	var total_flow float64 = 0.0
	var sample_cnt int = 0
	for range ticker.C {
		// set mode
		for _, hostname := range curHosts {
			result := new(string)
			CallWithTimeout(hostname, 12345, mode, result)
		}

		currentTime := time.Now()
		if currentTime.Sub(startTime) > time.Second*10 {
			break
		}
		for _, hostname := range curHosts {
			result := new(float64)
			args := Args{}
			getFlowWithTimeout(hostname, 12345, args, result)
			total_flow += *result
			sample_cnt += 1
		}

	}
	log.Printf("Average flow in 10s: %f\n", total_flow/float64(sample_cnt))
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

	for _, n := range []int{2, 4, 6, 8, 10} {
		test(n, mode)
	}

}
