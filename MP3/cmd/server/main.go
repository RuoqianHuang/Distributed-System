package main

import (
	"cs425/mp3/internal/detector"
	"cs425/mp3/internal/flow"
	"cs425/mp3/internal/member"
	"cs425/mp3/internal/utils"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

const RPC_PORT = 12345

type Server struct {
	failureDetector   *detector.FD
	InFlow            *flow.FlowCounter
	OutFlow           *flow.FlowCounter
}



type Args struct {
	Command string
}

func (s *Server) CLI(args Args, reply *string) error {
	switch args.Command {
	case "member":
		infoMap := s.failureDetector.Membership.GetInfoMap()
		*reply = "\n" + member.CreateTable(infoMap)
	case "status":
		infoMap := s.failureDetector.Membership.GetInfoMap()
		*reply = fmt.Sprintf("ID: %d, Hostname: %s, Port: %d, Input: %f bytes/s, Output: %f bytes/s\n", 
			s.failureDetector.Info.Id, s.failureDetector.Info.Hostname, s.failureDetector.Info.Port, s.InFlow.Get(), s.OutFlow.Get()) + member.CreateTable(infoMap)
	default:
		*reply = "Unknown command."
	}

	return nil
}

func (s *Server) Start() {
	// register rpc
	rpc.Register(s)
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", RPC_PORT))
	if err != nil {
		log.Fatalf("[Main] TCP Listen failed: %s", err.Error())
	}
	defer tcpListener.Close()
	// start tcp listener
	go func() {
		log.Printf("[Main] TCP RPC service lsitening on port %d", RPC_PORT)
		rpc.Accept(tcpListener)
	}()
	waitGroup := new(sync.WaitGroup)
	
	// Start failure detector
	waitGroup.Add(1)
	go func() { 
		defer waitGroup.Done()
		s.failureDetector.Start()
	}()
	// Start distributed file system

	waitGroup.Wait()
}

func main() {
	// handle syscall SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("[Main] SIGTERM, Exiting...")
		os.Exit(0)
	}()

	// some default parameters
	serverPort := utils.DEFAULT_PORT
	Tround := time.Second / 10
	Tsuspect := Tround * 10
	Tfail := Tround * 10
	Tcleanup := Tround * 100 // don't cleanup too fast, I need this to record FP rate and other stuff

	// parse command line arguments
	if len(os.Args) >= 2 {
		for i := 1; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "-p":
				if i+1 >= len(os.Args) {
					log.Printf("[Main] Error: -p flag requires a port number argument.")
					os.Exit(1)
				}
				_, err := fmt.Sscanf(os.Args[i+1], "%d", &serverPort)
				if err != nil {
					log.Printf("[Main] Error: invalid port number %s", os.Args[i+1])
					os.Exit(1)
				}
				i++
			default:
				log.Printf("[Main] Warning: unknown argument %s", os.Args[i])
			}
		}
	}

	// setup logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// setup my info and membership list
	hostname, err := utils.GetHostName()
	if err != nil {
		log.Fatalf("[Main] Failed to get hostname: %s", err.Error())
	}

	myInfo := member.Info{
		Hostname:  hostname,
		Port:      serverPort,
		Version:   time.Now(),
		Timestamp: time.Now(),
		Counter:   0,
		State:     member.Alive,
	}
	myId := member.HashInfo(myInfo)
	log.Printf("[Main] Starting, ID: %d, Hostname: %s, Port: %d", myId, myInfo.Hostname, myInfo.Port)

	// create membership object
	membership := member.Membership{
		InfoMap: map[uint64]member.Info{
			myId: myInfo,
		},
		Members: []uint64{myId},
		SortedMembers: []uint64{myId},
	}

	// create network flow counter
	inFlow := flow.FlowCounter{}
	outFlow := flow.FlowCounter{}

	// create failure detector
	failureDetector := detector.FD{
		Tround:     Tround,
		Tsuspect:   Tsuspect,
		Tfail:      Tfail,
		Tcleanup:   Tcleanup,
		Id:         myId,
		Info:       myInfo,
		Membership: &membership,
		InFlow:     &inFlow,
		OutFlow:    &outFlow,
	}

	// Create Server
	server := Server{
		failureDetector: &failureDetector,
		InFlow: &inFlow,
		OutFlow: &outFlow,
	}

	// start
	server.Start()
}
