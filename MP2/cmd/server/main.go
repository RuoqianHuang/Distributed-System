package main

import (
	"os"
	"fmt"
	"log"
	"net"
	"time"
	"net/rpc"
	"syscall"
	"os/signal"
	"cs425/mp2/internal/swim"
	"cs425/mp2/internal/utils"
	"cs425/mp2/internal/gossip"
	"cs425/mp2/internal/member"
)

type ServerState int

const (
	Gossip ServerState = iota
	Swim
	Failed
)

type Server struct{
	Tround time.Duration       // Duration of a round
	Tsuspect time.Duration     // suspect time for gossip
	Tfail time.Duration        // fail time for gossip
	TpingFail time.Duration    // direct ping fail time for swim
	TpingReqFail time.Duration // indirect ping fail time for swim
	Tcleanup time.Duration     // time to cleanup failed member's information
	Id int64                   // node's ID
	K int                      // k for swim
	Info member.Info           // node's own information
	gossip *gossip.Gossip      // gossip instance
	swim   *swim.Swim          // swim instance
	State ServerState          // server's state
}

// arguments for cli tool
type Args struct {

}

func (s* Server) CLI(args Args, reply *string) error {
	// for CLI tool
	log.Printf("recevied cli %v", args)
	*reply = "OK"
	return nil
}

func (s *Server) StartUDPListener() {
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", s.Info.Port))
	if err != nil {
		log.Fatalf("UDP ResolveAddr failed: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("UDP listen failed: %s", err.Error())
	}
	defer conn.Close()
	log.Printf("UDP service listening on port %s", s.Info.Port)

	data := make([]byte, 4096)
	for {
		_, _, err := conn.ReadFromUDP(data)
		if err != nil {
			log.Printf("UDP Read error: %s", err.Error())
			continue
		}
		message, err := utils.Deserialize(data)
		if err != nil {
			log.Printf("Failed to deserialize message: %s", err.Error())
		}else {	
			if message.Type == utils.Gossip {
				s.gossip.HandleIncomingMessage(message)
			}else if message.Type == utils.Ping || message.Type == utils.Pong || message.Type == utils.PingReq {
				s.swim.HandleIncomingMessage(message, s.Info)
			}else {
				// TODO: switch between gossip and swim
			}
		}
	}
}

func (s *Server) StartFailureDetector() {
	// create ticker 
	ticker := time.NewTicker(s.Tround)
	defer ticker.Stop()
	log.Printf("Failure dector stared with a period of %s", s.Tround.String())

	// The main event loop
	for {
		select {
		case <- ticker.C:
			if s.State == Swim {
				s.swim.SwimStep(s.Id, s.Info, s.K, s.TpingFail, s.TpingReqFail, s.Tcleanup)
			}else if s.State == Gossip {
				s.gossip.GossipStep(s.Id, s.Tfail, s.Tsuspect, s.Tcleanup)
			}
		// TODO: change period
		}
	}
}


func main() {
	// handle syscall SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("Exiting...\n")
		os.Exit(0)
	}()

	// some default parameters
	cliPort := 12345
	serverPort := 8787
	Tround       := time.Second / 10
	Tsuspect     := Tround * 5
	Tfail        := Tround * 5
	Tcleanup     := Tround * 5
	TpingFail    := Tround * 5
	TpingReqFail := Tround * 5

	// parse command line arguments
	if len(os.Args) >= 2 {
		for i := 1; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "-p":
				if i + 1 >= len(os.Args) {
					fmt.Println("Error: -p flag requires a port number argument.")
					os.Exit(1)
				}
				_, err := fmt.Sscanf(os.Args[i + 1], "%d", &serverPort)
				if err != nil {
					fmt.Printf("Error: invalid port number %s\n", os.Args[i + 1])
					os.Exit(1)
				}
				i++
			default:
				fmt.Printf("Warning: unknown argument %s\n", os.Args[i])
			}
		}
	}

	// setup logger
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// setup my info and membership list
	hostname, err := utils.GetHostName()
	if err != nil {
		log.Fatalf("Failed to get hostname: %s", err.Error())
	}
	myInfo := member.Info{
		Hostname:  hostname,
		Port:      serverPort,
		Version: time.Now(),
		Timestamp: time.Now(),
		Counter:   0,
		State:     member.Alive,
	}
	myId := member.HashInfo(myInfo)
	
	log.Printf("My ID: %d, Hostname: %s, Port: %d\n", myId, myInfo.Hostname, myInfo.Port)
	membership := member.Membership{
		InfoMap:   	map[int64]member.Info{
			myId: myInfo,
		},
		Members: []int64{myId},
	}

	// create gossip instance
	myGossip := gossip.Gossip{
		Membership: &membership,
	}
	// create swim instance
	mySwim := swim.Swim{
		Membership: &membership,
	}
	// create server instance
	myServer := Server{
		Tround: Tround,
		Tsuspect: Tsuspect,
		Tfail: Tfail,
		TpingFail: TpingFail,
		TpingReqFail: TpingReqFail,
		Tcleanup: Tcleanup,
		Info: myInfo,
		gossip: &myGossip,
		swim: &mySwim,
	}

	// register rpc server for CLI function
	rpc.Register(&myServer)
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cliPort))
	if err != nil {
		log.Fatalf("TCP Listen failed: %s", err.Error())
	}
	defer tcpListener.Close()
	rpc.Accept(tcpListener)
	log.Printf("TCP RPC service listening on port %s", cliPort)

	// start UDP listener
	go myServer.StartUDPListener()
	// start failure detector
	go myServer.StartFailureDetector()
}