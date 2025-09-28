package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

// arguments for cli tool
type Args struct {
	Command string
}

func CallWithTimeout(
	hostname string,
	port int,
	query string,
	result *string) {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		log.Printf("Failed to dial server %s:%d: %s\n", hostname, port, err.Error())
		return
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		args := Args{
			Command: query,
		}
		callChan <- client.Call("Server.CLI", args, result)
	}()
	select {
	case err := <-callChan:
		if err != nil {
			log.Printf("RPC call to server %s:%d failed: %s\n", hostname, port, err.Error())
		}
	case <-time.After(CALL_TIMEOUT):
		log.Printf("RPC call to server %s:%d timed out\n", hostname, port)
	}
}

func main() {
	if len(os.Args) < 3 {
		log.Println("Usage: ./client <Query> <Hostname> -p <Port>")
		log.Println("Example: ./client list_mem fa25-cs425-b601.cs.illinois.edu")
		log.Println("Available commands: list_mem, list_self, join, leave, display_suspects, display_protocol, switch(protocol, suspicion)")
	}

	port := 12345
	hostname := "fa25-cs425-b601.cs.illinois.edu"
	query := "status"

	var otherArgs []string
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-p":
			if (i + 1) >= len(os.Args) {
				log.Fatal("Error: -p port requires a port number.")
			}
			num, err := strconv.Atoi(os.Args[i+1])
			port = num
			if err != nil {
				log.Fatal("Invalid port number!")
			}
			i++
		default:
			otherArgs = append(otherArgs, os.Args[i])
		}
	}

	if len(otherArgs) < 2 {
		log.Fatal("Please specify query and hostname")
	}

	if otherArgs[0] == "set_drop_rate" && len(otherArgs) >= 3 {
		query = otherArgs[0] + " " + otherArgs[1]  // "set_drop_rate 0.5"
		hostname = otherArgs[2]  // "localhost"
	} else {
		query = otherArgs[0]
		hostname = otherArgs[1]
	}

	result := new(string)
	CallWithTimeout(hostname, port, query, result)

	log.Printf("\n%s", *result)

}
