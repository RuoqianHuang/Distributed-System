package main

import (
	"cs425/mp2/internal/flow"
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
	"strings"
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

const ProbeTimeout time.Duration = 5 * time.Second

type Server struct {
	Tround       time.Duration      // Duration of a round
	Tsuspect     time.Duration      // suspect time for gossip
	Tfail        time.Duration      // fail time for gossip
	TpingFail    time.Duration      // direct ping fail time for swim
	TpingReqFail time.Duration      // indirect ping fail time for swim
	Tcleanup     time.Duration      // time to cleanup failed member's information
	Id           uint64             // node's ID
	K            int                // k for swim
	Info         member.Info        // node's own information
	membership   *member.Membership // member ship information
	gossip       *gossip.Gossip     // gossip instance
	swim         *swim.Swim         // swim instance
	state        ServerState        // server's state
	suspicionMode bool              // whether suspicion mechanism is enabled
	lock         sync.RWMutex       // mutex for server's property
	InFlow       flow.FlowCounter   // input network counter
	OutFlow      flow.FlowCounter   // output network counter
}

// arguments for cli tool
type Args struct {
	Command string
}

func (s *Server) CLI(args Args, reply *string) error {
	// for CLI tool
	// log.Printf("received cli command: %s", args.Command)

	switch args.Command {
	case "gossip":
		s.switchToGossip()
		*reply = "Switched to Gossip protocol"
	case "swim":
		s.switchToSwim()
		*reply = "Switched to SWIM protocol"
	case "status":
		s.lock.RLock()
		state := s.state
		s.lock.RUnlock()
		*reply = fmt.Sprintf("Current protocol: %s, Input: %f bytes/s, Output %f bytes/s", s.getStateString(state), s.InFlow.Get(), s.OutFlow.Get())
		*reply = *reply + "\n" + s.membership.Table()
	case "list_mem":
		*reply = s.membership.Table()
	case "list_self":
		*reply = fmt.Sprintf("Self ID: %d, Hostname: %s, Port: %d", s.Id, s.Info.Hostname, s.Info.Port)
	case "join":
		// Join is implicit on startup, but we can provide status
		*reply = "Already joined the group. Use 'list_mem' to see membership."
	case "leave":
		s.leaveGroup()
		*reply = "Left the group voluntarily"
	case "display_suspects":
		*reply = s.getSuspectedNodes()
	case "display_protocol":
		s.lock.RLock()
		state := s.state
		suspicionMode := s.suspicionMode
		s.lock.RUnlock()
		protocol := "gossip"
		if state == Swim {
			protocol = "ping"
		}
		suspicion := "nosuspect"
		if suspicionMode {
			suspicion = "suspect"
		}
		*reply = fmt.Sprintf("<%s, %s>", protocol, suspicion)
	default:
		// Check for switch command with parameters
		if len(args.Command) > 6 && args.Command[:6] == "switch" {
			*reply = s.handleSwitchCommand(args.Command)
		} else {
			*reply = "Unknown command. Available commands: gossip, swim, status, list_mem, list_self, join, leave, display_suspects, display_protocol, switch(protocol, suspicion)"
		}
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

func (s *Server) leaveGroup() {
	s.lock.Lock()
	defer s.lock.Unlock()
	
	// Notify all members about voluntary leave
	infoMap := s.membership.GetInfoMap()
	for _, info := range infoMap {
		if info.State == member.Alive && info.Hostname != s.Info.Hostname {
			leaveMessage := utils.Message{
				Type:       utils.Gossip, // Use gossip to propagate leave
				SenderInfo: s.Info,
				TargetInfo: info,
				InfoMap:    map[uint64]member.Info{
					s.Id: {
						Hostname:  s.Info.Hostname,
						Port:      s.Info.Port,
						Version:   s.Info.Version,
						Timestamp: time.Now(),
						Counter:   s.Info.Counter,
						State:     member.Failed, // Mark as failed for leave
					},
				},
			}
			utils.SendMessage(leaveMessage, info.Hostname, info.Port)
		}
	}
	
	// Update local state
	s.state = Failed
	log.Printf("Voluntarily left the group")
}

func (s *Server) getSuspectedNodes() string {
	s.lock.RLock()
	defer s.lock.RUnlock()
	
	infoMap := s.membership.GetInfoMap()
	suspected := []string{}
	
	for _, info := range infoMap {
		if info.State == member.Suspected {
			suspected = append(suspected, fmt.Sprintf("ID: %d, Hostname: %s, Port: %d", 
				member.HashInfo(info), info.Hostname, info.Port))
		}
	}
	
	if len(suspected) == 0 {
		return "No suspected nodes"
	}
	
	result := "Suspected nodes:\n"
	for _, node := range suspected {
		result += node + "\n"
	}
	return result
}

func (s *Server) handleSwitchCommand(command string) string {
	// Parse switch(protocol, suspicion) command
	// Expected format: switch(gossip,suspect) or switch(ping,nosuspect)
	
	// Remove "switch(" and ")"
	if len(command) < 8 || command[:6] != "switch" {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}
	
	// Find the parameters inside parentheses
	start := 6
	if command[start] != '(' {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}
	
	end := len(command) - 1
	if command[end] != ')' {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}
	
	params := command[start+1:end]
	parts := strings.Split(params, ",")
	if len(parts) != 2 {
		return "Invalid switch command format. Use: switch(protocol, suspicion)"
	}
	
	protocol := strings.TrimSpace(parts[0])
	suspicion := strings.TrimSpace(parts[1])
	
	// Switch protocol and suspicion mode
	s.lock.Lock()
	if protocol == "gossip" {
		s.state = Gossip
		s.sendSwitchMessage(utils.UseGossip, "")
	} else if protocol == "ping" {
		s.state = Swim
		s.sendSwitchMessage(utils.UseSwim, "")
	} else {
		s.lock.Unlock()
		return "Invalid protocol. Use 'gossip' or 'ping'"
	}
	
	// Switch suspicion mode
	if suspicion == "suspect" {
		s.suspicionMode = true
	} else if suspicion == "nosuspect" {
		s.suspicionMode = false
	} else {
		s.lock.Unlock()
		return "Invalid suspicion mode. Use 'suspect' or 'nosuspect'"
	}
	s.lock.Unlock()
	
	return fmt.Sprintf("Switched to %s protocol with %s suspicion", protocol, suspicion)
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
			size, err := utils.SendMessage(switchMessage, info.Hostname, info.Port)
			if err != nil {
				log.Printf("Failed to send switch message to %s: %s", info.String(), err.Error())
			} else {
				s.OutFlow.Add(size)
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
	switch message.Type {
	case utils.UseGossip:
		newState = Gossip
	case utils.UseSwim:
		newState = Swim
	default:
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
		size, err := utils.SendMessage(message, hostname, utils.DEFAULT_PORT)
		if err != nil {
			log.Printf("Failed to send probe message to %s:%d: %s", hostname, utils.DEFAULT_PORT, err.Error())
		} else {
			s.OutFlow.Add(size)
		}
	}
	// wait for probe ack
	buffer := make([]byte, 4096)
	for {
		size, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Failed to get probe ack, starting the group alone with gossip mode")
			s.state = Gossip
			break
		}
		s.InFlow.Add(int64(size))
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
		size, _, err := conn.ReadFromUDP(data)
		if err != nil {
			log.Printf("UDP Read error: %s", err.Error())
			continue
		}
		s.InFlow.Add(int64(size))
		message, err := utils.Deserialize(data)
		if err != nil {
			log.Printf("Failed to deserialize message: %s", err.Error())
		} else {
			switch message.Type {
			case utils.Gossip:
				s.gossip.HandleIncomingMessage(message)
			case utils.Ping, utils.Pong, utils.PingReq:
				size := s.swim.HandleIncomingMessage(message, s.Info)
				s.OutFlow.Add(size)
			case utils.Probe:
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
				size, err := utils.SendMessage(ackMessage, message.SenderInfo.Hostname, message.SenderInfo.Port)
				if err != nil {
					log.Printf("Failed to send probe ack message to %s: %s", message.SenderInfo.String(), err.Error())
				} else {
					s.OutFlow.Add(size)
					log.Printf("Welcome, send probe ack message to %s:%d: %v", message.SenderInfo.Hostname, message.SenderInfo.Port, ackMessage)
				}
			case utils.UseSwim, utils.UseGossip:
				// Handle algorithm switch messages
				s.handleSwitchMessage(message)
			default:
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
	for range ticker.C {
		s.lock.RLock()
		suspicionMode := s.suspicionMode
		s.lock.RUnlock()
		
		switch s.state {
		case Swim:
			size := s.swim.SwimStep(s.Id, s.Info, s.K, s.TpingFail, s.TpingReqFail, s.Tcleanup, suspicionMode)
			s.OutFlow.Add(size)
		case Gossip:
			size := s.gossip.GossipStep(s.Id, s.Tfail, s.Tsuspect, s.Tcleanup, suspicionMode)
			s.OutFlow.Add(size)
		}
		// TODO: change period
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
		InfoMap: map[uint64]member.Info{
			myId: myInfo,
		},
		Members: []uint64{myId},
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
		suspicionMode: true, // suspicion enabled by default
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
