package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"cs425/mp1/internal/utils"
	"cs425/mp1/internal/caller"
)


func getHostName() string {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	return name
}


type Grep struct{}

func (g *Grep) Grep(query utils.Query, result *[]string) error {
	// Get hostname and extract machine number
	hostname := getHostName()
	machineNumber := utils.GetMachineNumber(hostname)
	
	// Determine log file path based on environment variable
	var filename string
	if os.Getenv("TEST_MODE") == "1" {
		// Test mode: use machine.XX.log
		filename = fmt.Sprintf("machine.%s.log", machineNumber)
	} else {
		// Normal mode: use vmX.log in current directory
		// Convert "01" -> 1, "02" -> 2, etc.
		if i, err := strconv.Atoi(machineNumber); err == nil {
			filename = fmt.Sprintf("vm%d.log", i)
		} else {
			filename = "vm1.log" // fallback
		}
	}

	log.Printf("Received query with args %v, using filename %s (hostname: %s)\n", query.Args, filename, hostname)

	err := utils.GrepFile(filename, result, query)
	if err != nil {
		log.Printf("%s\n", err.Error());
		return err
	}
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
	log.Printf("Starting server on node %s:%d...\n", hostname, caller.SERVER_PORT)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", caller.SERVER_PORT))
	if err != nil {
		log.Fatal("Error creating listener: ", err.Error())
	}
	rpc.Accept(listener)
}
