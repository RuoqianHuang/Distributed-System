package main

import (
	"os"
	"fmt"
	"log"
	"net"
	"errors"
	"net/rpc"
	"syscall"
	"os/signal"
	"path/filepath"
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

	log.Printf("Received query with args %v, using filename %s (hostname: %s)\n", query.Args, query.Filename, hostname)
	
	// Grep all files that match the regular expression
	matchingFiles, err := filepath.Glob(query.Filename)
	if err != nil {
		return errors.New(fmt.Sprintf("Invalid filename pattern: %s", err.Error()))
	}
	log.Printf("Found %d matching files\n", len(matchingFiles))

	// No matching files
	if len(matchingFiles) == 0 {
		*result = []string{};
		return nil
	}

	lines := []string{}
	for _, filename := range matchingFiles {
		log.Printf("Grep file %s\n", filename)
		partialResult := []string{}
		err := utils.GrepFile(filename, query.Args, &partialResult)
		if err != nil {
			log.Printf("%s\n", err.Error())
		} else {
			lines = append(lines, partialResult...)
		}
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
	log.Printf("Starting server on node %s:%d...\n", hostname, caller.SERVER_PORT)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", caller.SERVER_PORT))
	if err != nil {
		log.Fatal("Error creating listener: ", err.Error())
	}
	rpc.Accept(listener)
}
