package main

import (
	"cs425/mp3/internal/detector"
	"cs425/mp3/internal/distributed"
	"cs425/mp3/internal/files"
	"cs425/mp3/internal/flow"
	"cs425/mp3/internal/member"
	"cs425/mp3/internal/queue"
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

type Server struct {
	failureDetector *detector.FD
	distributed     *distributed.DistributedFiles
	fileManager     *files.FileManager
	InFlow          *flow.FlowCounter
	OutFlow         *flow.FlowCounter
}

type Args struct {
	Command    string
	Filename   string
	FileSource string
	VMAddress  string
}

func (s *Server) CLI(args Args, reply *string) error {
	switch args.Command {
	case "ls":
		*reply = s.distributed.ListReplicas(args.Filename, 2)
	case "member":
		infoMap := s.failureDetector.Membership.GetInfoMap()
		table, _ := member.CreateTable(infoMap)
		*reply = "\n" + table
	case "status":
		infoMap := s.failureDetector.Membership.GetInfoMap()
		table, _ := member.CreateTable(infoMap)
		*reply = fmt.Sprintf("ID: %d, Hostname: %s, Port: %d, Input: %f bytes/s, Output: %f bytes/s\n",
			s.failureDetector.Info.Id, s.failureDetector.Info.Hostname, s.failureDetector.Info.Port, s.InFlow.Get(), s.OutFlow.Get()) + table
	case "files":
		fileMap := s.distributed.CollectMeta()
		table, _ := files.CreateTable(fileMap)
		*reply = fmt.Sprintf("ID: %d, Hostname: %s, Port: %d, Input: %f bytes/s, Output: %f bytes/s\n",
			s.failureDetector.Info.Id, s.failureDetector.Info.Hostname, s.failureDetector.Info.Port, s.InFlow.Get(), s.OutFlow.Get()) + table
	case "create":
		err := s.distributed.Create(args.Filename, args.FileSource, 2)
		if err != nil {
			*reply = fmt.Sprintf("Failed to create file %s: %s", args.Filename, err.Error())
		} else {
			*reply = "File created successfully!"
		}
	case "get":
		err := s.distributed.Get(args.Filename, args.FileSource, 2)
		if err != nil {
			*reply = fmt.Sprintf("Failed to download %s: %s", args.Filename, err.Error()) 
		} else {
			*reply = fmt.Sprintf("File download to %s successfully!", args.FileSource)
		}
	case "append":
		err := s.distributed.Append(args.Filename, args.FileSource, 2)
		if err != nil {
			*reply = fmt.Sprintf("Failed to appedn %s: %s", args.Filename, err.Error())
		} else {
			*reply = fmt.Sprintf("%s append successfully!", args.Filename)
		}
	case "getfromreplica":
		*reply = "Not implemented, please refer to the interactive client"
	case "flow":
		flow := new(float64)
		s.GetFlow(0, flow)
		*reply = fmt.Sprintf("Current flow: %f bytes/s", *flow)
	default:
		*reply = "Unknown command."
	}
	return nil
}

func (s *Server) Files(_ int, reply *map[uint64]files.Meta) error {
	*reply = s.distributed.CollectMeta()
	return nil
}

func (s *Server) Member(_ int, reply *map[uint64]member.Info) error {
	*reply = s.distributed.Membership.GetInfoMap()
	return nil
}

func (s *Server) GetReplicas(Id uint64, reply *[]member.Info) error {
	replicas, err := s.distributed.Membership.GetReplicas(Id, s.distributed.NumOfReplicas)
	if err != nil {
		return err
	}
	*reply = replicas
	return nil
}

func (s *Server) Leave(_ int, reply *string) error {
	s.failureDetector.StopAndLeave()
	s.distributed.Stop()
	*reply = fmt.Sprintf("%s:%d leave and stop", s.distributed.Hostname, s.distributed.Port)
	return nil
}

func (s *Server) GetFlow(_ int, reply *float64) error {
	*reply = s.distributed.Flow.Get()
	return nil
}

func (s *Server) registerRPC() {
	s.fileManager.RegisterRPC()
	s.distributed.RegisterRPC()
	rpc.Register(s)
}

func (s *Server) Start(rpcPort int) {
	// Register rpc
	s.registerRPC()
	// Create TCP listener
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", rpcPort))
	if err != nil {
		log.Fatalf("[Main] TCP Listen failed: %s", err.Error())
	}
	defer tcpListener.Close()
	// start tcp listener
	go func() {
		log.Printf("[Main] TCP RPC service lsitening on port %d", rpcPort)
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
	go func() {
		defer waitGroup.Done()
		s.distributed.Start()
	}()

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
	udpPort := utils.DEFAULT_PORT
	Tround := time.Second / 10
	Tsuspect := Tround * 20
	Tfail := Tround * 20
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
				_, err := fmt.Sscanf(os.Args[i+1], "%d", &udpPort)
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
	// use udp port + 1 as rpc port
	rpcPort := udpPort + 1

	// setup logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// setup my info and membership list
	hostname, err := utils.GetHostName()
	if err != nil {
		log.Fatalf("[Main] Failed to get hostname: %s", err.Error())
	}

	myInfo := member.Info{
		Hostname:  hostname,
		Port:      udpPort,
		Version:   time.Now(),
		Timestamp: time.Now(),
		Counter:   0,
		State:     member.Alive,
	}
	myId := member.HashInfo(myInfo)
	myInfo.Id = myId // Set the Id field to match the computed hash
	log.Printf("[Main] Starting, ID: %d, Hostname: %s, Port: %d", myId, myInfo.Hostname, myInfo.Port)

	// create membership object
	membership := member.Membership{
		InfoMap: map[uint64]member.Info{
			myId: myInfo,
		},
		Members:       []uint64{myId},
		SortedMembers: []uint64{myId},
	}

	// create network flow counter
	// TODO: merge flow counters
	inFlow := flow.NewFlowCounter()
	outFlow := flow.NewFlowCounter()
	FlowDF := flow.NewFlowCounter()

	// create failure detector
	failureDetector := detector.FD{
		Tround:     Tround,
		Tsuspect:   Tsuspect,
		Tfail:      Tfail,
		Tcleanup:   Tcleanup,
		Id:         myId,
		Info:       myInfo,
		Membership: &membership,
		InFlow:     inFlow,
		OutFlow:    outFlow,
	}

	// create local file manager
	fileManager := files.NewFileManager()

	// create distributed file system
	distributed := distributed.DistributedFiles{
		Tround:               time.Second * 5, // run Merge every 5s
		Hostname:             hostname,
		Port:                 myInfo.Port,
		FileManager:          fileManager,
		Membership:           &membership,
		NumOfReplicas:        3,
		NumOfMetaWorkers:     1,
		NumOfBlockWorkers:    1,
		NumOfBufferedWorkers: 1,
		NumOfTries:           3,
		MetaJobMap:          make(map[uint64]bool),
		MetaJobs:             queue.NewQueue(),
		BlockJobMap:          make(map[uint64]bool),
		BlockJobs:            queue.NewQueue(),
		BufferedBlocks:       queue.NewQueue(),
		BufferedBlockMap:     make(map[uint64]distributed.BufferedBlock),
		LockedMeta:           make(map[uint64]files.Meta),
		Flow:                 FlowDF,
	}

	// Create Server
	server := Server{
		distributed:     &distributed,
		fileManager:     fileManager,
		failureDetector: &failureDetector,
		InFlow:     inFlow,
		OutFlow:    outFlow,
	}

	// start
	server.Start(rpcPort)
}
