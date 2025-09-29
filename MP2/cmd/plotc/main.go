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

func getTableWithTimeout(
	hostname string,
	port int,
	args Args,
	result *map[uint64]member.Info) error {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		return fmt.Errorf("failed to dial server %s:%d: %s", hostname, port, err.Error())
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
			return fmt.Errorf("rpc call to server %s:%d failed: %s", hostname, port, err.Error())
		}
	case <-time.After(CALL_TIMEOUT):
		return fmt.Errorf("rpc call to server %s:%d timed out", hostname, port)
	}
	return nil
}

func getIdWithTimeout(
	hostname string,
	port int,
	args Args,
	result *uint64) error {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		return fmt.Errorf("failed to dial server %s:%d: %s", hostname, port, err.Error())
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call("Server.GetId", args, result)
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

func test(numOfFailure int, mode Args) {
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
		if time.Since(startTime) > 10*time.Second {
			break
		}
	}

	var detectTime map[uint64]time.Time = make(map[uint64]time.Time)
	offHosts := utils.HOSTS[10-numOfFailure:]
	// stop these nodes and detect
	for _, hostname := range offHosts {
		currentTime := time.Now()
		Id := new(uint64)
		err := getIdWithTimeout(hostname, 12345, Args{}, Id)
		if err != nil {
			log.Fatalf("fail to get Id from %s: %s\n", hostname, err.Error())
		} else {
			detectTime[*Id] = currentTime
		}
		result := new(string)
		args := Args{
			Command: "stop",
		}
		CallWithTimeout(hostname, 12345, args, result)
		log.Printf("stop %s\n", hostname)
	}

	ticker := time.NewTicker(time.Second / 10)
	log.Printf("Start experimenting with %d simultaneous failures\n", numOfFailure)
	// collect result for 3 s
	startTime = time.Now()

	var firstDetectMap map[uint64]time.Time = make(map[uint64]time.Time)
	var finalDetectMap map[uint64]time.Time = make(map[uint64]time.Time)
	var detectCount map[uint64]map[string]int = make(map[uint64]map[string]int)
	for range ticker.C {

		// set mode
		for _, hostname := range utils.HOSTS {
			result := new(string)
			CallWithTimeout(hostname, 12345, mode, result)
		}
		if time.Since(startTime) > time.Second*30 {
			break
		}

		for _, hostname := range utils.HOSTS {
			result := new(map[uint64]member.Info)
			getTableWithTimeout(hostname, 12345, Args{}, result)
			for id, info := range *result {
				if info.State != member.Failed {
					continue
				}
				_, ok := detectTime[id]
				if !ok {
					continue
				}
				// detect for first time
				_, detected := firstDetectMap[id]
				if !detected {
					firstDetectMap[id] = time.Now()
					// log.Printf("Node %s:%d failed after %v, from %s", info.Hostname, info.Port, time.Since(startTime), hostname)
				}

				// count detect times
				_, ok = detectCount[id]
				if ok {
					detectCount[id][hostname] = 1
				} else {
					detectCount[id] = make(map[string]int)
					detectCount[id][hostname] = 1
				}
				
			}
		}
		for id := range detectTime {
			mp, ok := detectCount[id]
			if ok && len(mp) == 10 {
				_, exist := finalDetectMap[id]
				if !exist {
					// log.Printf("Final detect time for ID %d is %v", id, time.Since(timestamp))
					finalDetectMap[id] = time.Now()
				}
			}
		}
		if len(finalDetectMap) == len(detectTime) {
			log.Printf("All %d failures are detected by all nodes\n", len(detectTime))
			break
		}
	}
	var firstDT time.Duration = 0 * time.Second
	var finalDT time.Duration = 0 * time.Second
	for id, dTime := range detectTime {
		fTime, ok := firstDetectMap[id]
		if ok && firstDT < fTime.Sub(dTime) {
			firstDT = fTime.Sub(dTime)
		}
	}

	for id, dTime := range detectTime {
		fTime, ok := finalDetectMap[id]
		if ok && finalDT < fTime.Sub(dTime) {
			finalDT = fTime.Sub(dTime)
		}
	}

	log.Printf("First Detection Time: %v, Final detection Time: %v\n", firstDT, finalDT)
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

	for _, n := range []int{1, 2, 3, 4, 5} {
		test(n, mode)
	}

}
