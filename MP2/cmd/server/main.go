package main

import (
	"os"
	"fmt"
	"log"
	"net"
	"time"
	"sync"
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
	Init
)

const ProbeTimeout time.Duration = 10 * time.Second

type Server struct{
	Tround time.Duration           // Duration of a round
	Tsuspect time.Duration         // suspect time for gossip
	Tfail time.Duration            // fail time for gossip
	TpingFail time.Duration        // direct ping fail time for swim
	TpingReqFail time.Duration     // indirect ping fail time for swim
	Tcleanup time.Duration         // time to cleanup failed member's information
	Id int64                       // node's ID
	K int                          // k for swim
	Info member.Info               // node's own information
	membership *member.Membership  // member ship information
	gossip *gossip.Gossip          // gossip instance
	swim   *swim.Swim              // swim instance
	state ServerState              // server's state
	lock  sync.RWMutex             // mutex for server's property
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

func (s *Server) joinGroup() {
	s.lock.Lock()
	defer s.lock.Unlock()

	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", s.Info.Port))
	if err != nil {
		log.Fatalf("UDP ResolveAddr failed: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("UDP listen failed: %s", err.Error())
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(ProbeTimeout))

	// Send message to 10 other introducer
	message := utils.Message{
		Type: utils.Probe,
		SenderInfo: s.Info,
		InfoMap: s.membership.GetInfoMap(),
	}
	for _, hostname := range utils.HOSTS {
		err := utils.SendMessage(message, hostname, utils.DEFAULT_PORT)
		if err != nil {
			log.Printf("Failed to send probe message to %s:%d: %s", hostname, utils.DEFAULT_PORT, err.Error())
		}
	}
	// wait for probe ack
	buffer := make([]byte, 4096)
	for {
		_, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Failed to get probe ack, starting the group alone with gossip mode")
			s.state = Gossip
			break
		}
		message, err := utils.Deserialize(buffer)
		if err != nil {
			log.Printf("Failed to deserialize message: %s", err.Error())
			continue
		}
		if message.Type == utils.ProbeAckGossip{
			s.state = Gossip
			s.membership.Merge(message.InfoMap, time.Now())
			break
		}else if message.Type == utils.ProbeAckSwim {
			s.state = Swim
			s.membership.Merge(message.InfoMap, time.Now())
			break
		}
	}
}

func (s *Server) startUDPListenerLoop(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", s.Info.Port))
	if err != nil {
		log.Fatalf("UDP ResolveAddr failed: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("UDP listen failed: %s", err.Error())
	}
	defer conn.Close()
	log.Printf("UDP service listening on port %d", s.Info.Port)

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
			}else if message.Type == utils.Probe{
				// someone want to join the group, send some information back
				log.Printf("Receive probe message: %v", message)
				s.membership.Merge(message.InfoMap, time.Now()) // merge new member
				messageType := utils.ProbeAckGossip
				s.lock.RLock()
				if s.state == Swim {
					messageType = utils.ProbeAckSwim
				}
				s.lock.RUnlock()
				ackMessage := utils.Message{
					Type: messageType,
					SenderInfo: s.Info,
					TargetInfo: message.SenderInfo,
					InfoMap: s.membership.GetInfoMap(),
				}
				err := utils.SendMessage(ackMessage, message.SenderInfo.Hostname, message.SenderInfo.Port)
				if err != nil {
					log.Printf("Failed to send probe ack message to %s: %s", message.SenderInfo.String(), err.Error())
				}else {
					log.Printf("Welcome, send probe ack message to %s:%d: %v", message.SenderInfo.Hostname, message.SenderInfo.Port, ackMessage)
				}
			}else if message.Type == utils.UseSwim || message.Type == utils.UseGossip {
				// TODO: switch between gossip and swim
			}else { 
				// ignore probe ack, since the node should already join the group
			}
		}
	}
}

func (s *Server) startFailureDetectorLoop(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	// create ticker 
	ticker := time.NewTicker(s.Tround)
	defer ticker.Stop()
	log.Printf("Failure detector stared with a period of %s", s.Tround.String())

	// The main event loop
	for {
		select {
		case <- ticker.C:
			if s.state == Swim {
				s.swim.SwimStep(s.Id, s.Info, s.K, s.TpingFail, s.TpingReqFail, s.Tcleanup)
			}else if s.state == Gossip {
				s.gossip.GossipStep(s.Id, s.Tfail, s.Tsuspect, s.Tcleanup)
			}
		// TODO: change period
		}
	}
}

func (s *Server) Work() {
	// join group
	s.joinGroup()
	waitGroup := new(sync.WaitGroup)
	// start UDP listener
	waitGroup.Add(1)
	go s.startUDPListenerLoop(waitGroup)
	// start failure detector
	waitGroup.Add(1)
	go s.startFailureDetectorLoop(waitGroup)
	waitGroup.Wait()
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
	serverPort := utils.DEFAULT_PORT
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
	myGossip := gossip.NewGossip(&membership)
	// create swim instance
	mySwim := swim.NewSwim(&membership)
	// create server instance
	myServer := Server{
		Tround: Tround,
		Tsuspect: Tsuspect,
		Tfail: Tfail,
		TpingFail: TpingFail,
		TpingReqFail: TpingReqFail,
		Tcleanup: Tcleanup,
		Id: myId,
		K: 3,  // ping req to 3 other member
		Info: myInfo,
		membership: &membership,
		gossip: myGossip,
		swim: mySwim,
		state: Init,
	}

	// register rpc server for CLI function
	rpc.Register(&myServer)
	tcpListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cliPort))
	if err != nil {
		log.Fatalf("TCP Listen failed: %s", err.Error())
	}
	defer tcpListener.Close()
	go func() {
		log.Printf("TCP RPC service listening on port %d", cliPort)
		rpc.Accept(tcpListener)
	}()
	
	myServer.Work()
}