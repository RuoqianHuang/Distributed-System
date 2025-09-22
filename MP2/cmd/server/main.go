package main

import (
	"cs425/mp2/internal/gossip"
	"cs425/mp2/internal/member"
	"cs425/mp2/internal/swim"
	"cs425/mp2/internal/utils"
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

type ServerState int

const (
	Gossip ServerState = iota
	Swim
	Failed
	Init
)

const ProbeTimeout time.Duration = 10 * time.Second

type Server struct {
	Tround       time.Duration      // Duration of a round
	Tsuspect     time.Duration      // suspect time for gossip
	Tfail        time.Duration      // fail time for gossip
	TpingFail    time.Duration      // direct ping fail time for swim
	TpingReqFail time.Duration      // indirect ping fail time for swim
	Tcleanup     time.Duration      // time to cleanup failed member's information
	Id           int64              // node's ID
	K            int                // k for swim
	Info         member.Info        // node's own information
	membership   *member.Membership // member ship information
	gossip       *gossip.Gossip     // gossip instance
	swim         *swim.Swim         // swim instance
	state        ServerState        // server's state
	lock         sync.RWMutex       // mutex for server's property
}

// arguments for cli tool
type Args struct {
	Command string
}

func (s *Server) CLI(args Args, reply *string) error {
	// for CLI tool
	log.Printf("received cli command: %s", args.Command)

	switch args.Command {
	case "switch_gossip":
		s.switchToGossip()
		*reply = "Switched to Gossip protocol"
	case "switch_swim":
		s.switchToSwim()
		*reply = "Switched to SWIM protocol"
	case "status":
		s.lock.RLock()
		state := s.state
		s.lock.RUnlock()
		*reply = fmt.Sprintf("Current protocol: %s", s.getStateString(state))
	case "membership":
		*reply = s.membership.String()
	default:
		*reply = "Unknown command. Available commands: switch_gossip, switch_swim, status, membership"
	}
	return nil
}

func (s *Server) getStateString(state ServerState) string {
	switch state {
	case Gossip:
		return "Gossip"
	case Swim:
		return "SWIM"
	case Failed:
		return "Failed"
	case Init:
		return "Init"
	default:
		return "Unknown"
	}
}

func (s *Server) switchToGossip() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.state == Gossip {
		log.Printf("Already using Gossip protocol")
		return
	}

	log.Printf("Switching to Gossip protocol")
	s.state = Gossip

	// Send switch message to all members
	s.sendSwitchMessage(utils.UseGossip, "")
}

func (s *Server) switchToSwim() {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.state == Swim {
		log.Printf("Already using SWIM protocol")
		return
	}

	log.Printf("Switching to SWIM protocol")
	s.state = Swim

	// Send switch message to all members
	s.sendSwitchMessage(utils.UseSwim, "")
}

func (s *Server) sendSwitchMessage(messageType utils.MessageType, excludeHostname string) {
	// Get all members and send switch message to each
	infoMap := s.membership.GetInfoMap()
	for _, info := range infoMap {
		// Skip excluded hostname (for forwarding)
		if excludeHostname != "" && info.Hostname == excludeHostname {
			continue
		}
		if info.State == member.Alive {
			switchMessage := utils.Message{
				Type:       messageType,
				SenderInfo: s.Info,
				TargetInfo: info,
				InfoMap:    infoMap,
			}
			err := utils.SendMessage(switchMessage, info.Hostname, info.Port)
			if err != nil {
				log.Printf("Failed to send switch message to %s: %s", info.String(), err.Error())
			} else {
				log.Printf("Sent switch message to %s", info.String())
			}
		}
	}
}

func (s *Server) handleSwitchMessage(message utils.Message) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// Merge membership information first
	s.membership.Merge(message.InfoMap, time.Now())

	// Check if we need to switch protocols
	var newState ServerState
	if message.Type == utils.UseGossip {
		newState = Gossip
	} else if message.Type == utils.UseSwim {
		newState = Swim
	} else {
		log.Printf("Unknown switch message type: %v", message.Type)
		return
	}

	// Only switch if we're not already in the target state
	if s.state != newState {
		log.Printf("Received switch message to %s from %s", s.getStateString(newState), message.SenderInfo.String())
		s.state = newState

		// Forward the switch message to other members (gossip-style propagation)
		s.sendSwitchMessage(message.Type, message.SenderInfo.Hostname)
	} else {
		log.Printf("Already using %s protocol, ignoring switch message", s.getStateString(newState))
	}
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
		Type:       utils.Probe,
		SenderInfo: s.Info,
		InfoMap:    s.membership.GetInfoMap(),
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
		if message.Type == utils.ProbeAckGossip {
			s.state = Gossip
			s.membership.Merge(message.InfoMap, time.Now())
			break
		} else if message.Type == utils.ProbeAckSwim {
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
		} else {
			if message.Type == utils.Gossip {
				s.gossip.HandleIncomingMessage(message)
			} else if message.Type == utils.Ping || message.Type == utils.Pong || message.Type == utils.PingReq {
				s.swim.HandleIncomingMessage(message, s.Info)
			} else if message.Type == utils.Probe {
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
					Type:       messageType,
					SenderInfo: s.Info,
					TargetInfo: message.SenderInfo,
					InfoMap:    s.membership.GetInfoMap(),
				}
				err := utils.SendMessage(ackMessage, message.SenderInfo.Hostname, message.SenderInfo.Port)
				if err != nil {
					log.Printf("Failed to send probe ack message to %s: %s", message.SenderInfo.String(), err.Error())
				} else {
					log.Printf("Welcome, send probe ack message to %s:%d: %v", message.SenderInfo.Hostname, message.SenderInfo.Port, ackMessage)
				}
			} else if message.Type == utils.UseSwim || message.Type == utils.UseGossip {
				// Handle algorithm switch messages
				s.handleSwitchMessage(message)
			} else {
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
		case <-ticker.C:
			if s.state == Swim {
				s.swim.SwimStep(s.Id, s.Info, s.K, s.TpingFail, s.TpingReqFail, s.Tcleanup)
			} else if s.state == Gossip {
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
	Tround := time.Second / 10
	Tsuspect := Tround * 5
	Tfail := Tround * 5
	Tcleanup := Tround * 5
	TpingFail := Tround * 5
	TpingReqFail := Tround * 5

	// parse command line arguments
	if len(os.Args) >= 2 {
		for i := 1; i < len(os.Args); i++ {
			switch os.Args[i] {
			case "-p":
				if i+1 >= len(os.Args) {
					fmt.Println("Error: -p flag requires a port number argument.")
					os.Exit(1)
				}
				_, err := fmt.Sscanf(os.Args[i+1], "%d", &serverPort)
				if err != nil {
					fmt.Printf("Error: invalid port number %s\n", os.Args[i+1])
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
		Version:   time.Now(),
		Timestamp: time.Now(),
		Counter:   0,
		State:     member.Alive,
	}
	myId := member.HashInfo(myInfo)

	log.Printf("My ID: %d, Hostname: %s, Port: %d\n", myId, myInfo.Hostname, myInfo.Port)
	membership := member.Membership{
		InfoMap: map[int64]member.Info{
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
		Tround:       Tround,
		Tsuspect:     Tsuspect,
		Tfail:        Tfail,
		TpingFail:    TpingFail,
		TpingReqFail: TpingReqFail,
		Tcleanup:     Tcleanup,
		Id:           myId,
		K:            3, // ping req to 3 other member
		Info:         myInfo,
		membership:   &membership,
		gossip:       myGossip,
		swim:         mySwim,
		state:        Init,
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
