package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

type Args struct {
	Command    string
	Filename   string
	FileSource string
	VMAddress  string
}

func CallWithTimeout(
	hostname string,
	port int,
	args Args,
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
		callChan <- client.Call("Server.CLI", args, result)
	}()
	err = <-callChan
	if err != nil {
		log.Printf("RPC call to server %s:%d failed: %s\n", hostname, port, err.Error())
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Println("Usage: ./client <Query> -p <Port>")
		log.Println("Example: ./client member files")
		log.Println("Available commands: member, status, files, create, append, get")
	}
	port := 8788
	hostname := "localhost"

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

	if len(otherArgs) < 1 {
		log.Fatal("Please specify query")
	}
	args := Args{
		Command: "status",
	}
	args.Command = otherArgs[0]

	if args.Command == "create" || args.Command == "get" || args.Command == "append" {
		if len(otherArgs) < 3 {
			log.Fatal("Pleas specify file name and file source to create file")
		}
		args.Filename = otherArgs[1]
		FileSource, err := filepath.Abs(otherArgs[2])
		if err != nil {
			log.Fatalf("Can't resolve file path: %s: %s", otherArgs[2], err.Error())
		}
		args.FileSource = FileSource
	} 
	if args.Command == "ls" {
		if len(otherArgs) < 2 {
			log.Fatal("Please specify file name for ls command")
		}
		args.Filename = otherArgs[1]
	}
	if args.Command == "getfromreplica" {
		if len(otherArgs) < 4 {
			log.Fatal("Usage: getfromreplica VMaddress HyDFSfilename localfilename")
		}
		args.VMAddress = otherArgs[1]
		args.Filename = otherArgs[2]
		FileSource, err := filepath.Abs(otherArgs[3])
		if err != nil {
			log.Fatalf("Can't resolve file path: %s: %s", otherArgs[3], err.Error())
		}
		args.FileSource = FileSource
	}

	result := new(string)
	CallWithTimeout(hostname, port, args, result)

	log.Printf("\n%s", *result)

}
