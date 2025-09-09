package main

import (
	"fmt"
	"bytes"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
)

type Query struct {
	Filename string
	Args     []string
}

const (
	serverPort int32 = 9487
)

func getHostName() string {
	name, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	return name
}

type Grep struct{}

func (g *Grep) Grep(query Query, result *[]string) error {
	log.Printf("Received query for %s with args %v\n", query.Filename, query.Args)

	// read file content
	file, err := os.Open(query.Filename)
	if err != nil {
		log.Printf("Fail to open file %s: %s", query.Filename, err.Error())
		return err
	}
	defer file.Close()

	// create grep command to run and pipe the file to it
	Args := []string{"--color=always"}
	Args = append(Args, query.Args...)
	exe := exec.Command("grep", Args...)
	exe.Stdin = file

	// run the grep command and receive result with buffer
	log.Printf("Run command %s", exe.String())
	var stdout_buf, stderr_buf bytes.Buffer
	exe.Stdout = &stdout_buf
	exe.Stderr = &stderr_buf
	err = exe.Run()

	if err != nil {
		log.Printf("Command failed: %v: %s", err, stderr_buf.String())
		return err
	}

	// process output and return
	output := strings.TrimSpace(stdout_buf.String())
	if len(output) == 0 {
		*result = []string{}
		return nil
	}

	*result = strings.Split(output, "\n")
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
	log.Printf("Starting server on node %s:%d...\n", hostname, serverPort)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		log.Fatal("Error creating listener: ", err.Error())
	}
	rpc.Accept(listener)
}