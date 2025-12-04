package detector

import (
	"cs425/mp4/internal/flow"
	"cs425/mp4/internal/member"
	"cs425/mp4/internal/utils"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const ProbeTimeout time.Duration = 2 * time.Second

type FDState int

const (
	Alive FDState = iota
	Stop  
)

type FD struct {
	Tround            time.Duration
	Tsuspect          time.Duration
	Tfail             time.Duration
	Tcleanup          time.Duration
	Id                uint64
	Info              member.Info
	Membership        *member.Membership
	State             FDState   
	IsLeader          bool
	LeaderHost        string
	LeaderPort        int
	flow              *flow.FlowCounter
}

func GetNewDetector(hostname string, udpPort int, stage int, stageID int, isLeader bool, leaderHost string, leaderPort int, flow *flow.FlowCounter) *FD {
	Tround := time.Second / 10
	Tsuspect := Tround * 20
	Tfail := Tround * 20
	Tcleanup := Tround * 100 
	currentTime := time.Now()

	myInfo := member.Info{
		Hostname:  hostname,
		Port:      udpPort,
		Stage: stage,
		StageID: stageID,
		Version:   currentTime,
		Timestamp: currentTime,
		Counter:   0,
		State:     member.Alive,
	}
	myId := member.HashInfo(myInfo)
	myInfo.Id = myId // Set the Id field to match the computed hash
	log.Printf("[FD] Starting, ID: %d, Hostname: %s, Port: %d", myId, myInfo.Hostname, myInfo.Port)

	// create membership object
	membership := &member.Membership{
		InfoMap: map[uint64]member.Info{
			myId: myInfo,
		},
		Members:       []uint64{myId},
	}

	return &FD{
		Tround: Tround,
		Tsuspect: Tsuspect,
		Tfail: Tfail,
		Tcleanup: Tcleanup,
		Id: myId,
		Info: myInfo,
		Membership: membership,
		State: Alive,
		IsLeader: isLeader,
		LeaderHost: leaderHost,
		LeaderPort: leaderPort,
		flow: flow,
	}
}

func (f *FD) joinGroup() { 

	// Create a UDP listener
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", f.Info.Port))
	if err != nil {
		log.Fatalf("[FD] UDP ResolveAddr failed: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("[FD] UDP listen failed: %s", err.Error())
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(ProbeTimeout))

	message := utils.Message{
		Type: utils.Probe,
		SenderInfo: f.Info,
		InfoMap: f.Membership.GetInfoMap(),
	}
	// Send message to leader (introducer)
	_, err = utils.SendMessage(message, f.LeaderHost, f.LeaderPort)
	if err != nil {
		log.Printf("[FD] Failed to send probe message to %s:%d: %s", f.LeaderHost, f.LeaderPort, err.Error())
	}

	// Wait for probe ack
	buffer := make([]byte, 4096)
	for {
		_, _, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Fatalf("Failed to get probe ack, stopping")
		}

		message, err := utils.Deserialize(buffer)
		if err != nil {
			log.Printf("Failed to deserialize mess: %s", err.Error())
			continue
		}
		if message.Type == utils.ProbeAck {
			f.Membership.Merge(message.InfoMap, time.Now())
			break
		}
	}
}

func (f *FD) gossipStep() {
	currentTime := time.Now()

	// Increase heartbeat counter
	err := f.Membership.Heartbeat(f.Id, currentTime, f.flow.Get())
	if err != nil {
		log.Fatalf("[FD] Failed to heartbeat: %s", err.Error())
	}
	
	// Update member state
	f.Membership.UpdateStateGossip(currentTime, f.Tfail, f.Tsuspect, true)

	// Cleanup
	f.Membership.Cleanup(currentTime, f.Tcleanup)

	// Get gossip target info
	targetInfo, err := f.Membership.GetTarget()
	if err != nil {
		log.Fatalf("[FD] Failed to get target info: %s", err.Error())
	} else {
		// Copy member info map to send
		infoMap := f.Membership.GetInfoMap()

		// Send gossip
		message := utils.Message{
			Type:    utils.Gossip,
			InfoMap: infoMap,
		}
		_, err := utils.SendMessage(message, targetInfo.Hostname, targetInfo.Port)
		if err != nil {
			log.Printf("[FD] Failed to send gossip message to %s:%d: %s", targetInfo.Hostname, targetInfo.Port, err.Error())
		}
	}

	// Check if you can find the leader!!!
	if !f.Membership.Exists(f.LeaderHost, f.LeaderPort) {
		log.Fatalf("[FD] Partitioned from Leader!!!")
	}
}

func (f *FD) startUDPListenerLoop(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	addr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", f.Info.Port))
	if err != nil {
		log.Fatalf("[FD] UDP ResolveAddr failed: %s", err.Error())
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatalf("[FD] UDP listen failed: %s", err.Error())
	}
	defer conn.Close()
	log.Printf("[FD] UDP service listening on port %d", f.Info.Port)

	data := make([]byte, 4096)
	for {
		if f.State == Stop {
			break // Stop
		}

		_, _, err := conn.ReadFromUDP(data)
		if err != nil {
			log.Printf("[FD] UDP Read error: %s", err.Error())
			continue
		}

		message, err := utils.Deserialize(data)
		if err != nil {
			log.Printf("[FD] Failed to deserialize message: %s", err.Error())
		} else {
			
			switch message.Type {
			case utils.Gossip:
				// Merge incoming member list
				f.Membership.Merge(message.InfoMap, time.Now())
			case utils.Probe:
				// Someone wants to join the group, send some information back
				log.Printf("[FD] Receive probe message from: %s:%d", message.SenderInfo.Hostname, message.SenderInfo.Port)
				f.Membership.Merge(message.InfoMap, time.Now()) // merge new member			
				ackMessage := utils.Message{
					Type:       utils.ProbeAck,
					SenderInfo: f.Info,
					InfoMap:    f.Membership.GetInfoMap(),
				}
				_, err := utils.SendMessage(ackMessage, message.SenderInfo.Hostname, message.SenderInfo.Port)
				if err != nil {
					log.Printf("[FD] Failed to send probe ack message to %s: %s", message.SenderInfo.String(), err.Error())
				} else {
					log.Printf("[FD] Welcome, send probe ack message to %s:%d", message.SenderInfo.Hostname, message.SenderInfo.Port)
				}
			case utils.Leave:
				// Handle voluntary leave - remove the sender from membership
				senderId := member.HashInfo(message.SenderInfo)
				f.Membership.RemoveMember(senderId)
				log.Printf("[FD] Node %s voluntarily left the group", message.SenderInfo.String())
			default:
				// Ignore all other messages
			}
		}
	}
}

func (f *FD) startFailureDetectorLoop(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	// Create ticker
	ticker := time.NewTicker(f.Tround)
	defer ticker.Stop()
	log.Printf("[FD] Failure detector stared with a period of %s", f.Tround.String())

	// The main event loop
	for range ticker.C {
		if f.State == Stop {
			break
		}
		// Run gossip step
		f.gossipStep()
	}
}


func (f *FD) Start() {
	// Join group
	if !f.IsLeader {
		f.joinGroup()
	}
	waitGroup := new(sync.WaitGroup)
	// Start UDP listener
	waitGroup.Add(1)
	go f.startUDPListenerLoop(waitGroup)
	// Start failure detector
	waitGroup.Add(1)
	go f.startFailureDetectorLoop(waitGroup)
	waitGroup.Wait()
}

func (f *FD) StopAndLeave() {
	// Notify all members about voluntary leave
	infoMap := f.Membership.GetInfoMap()
	for id, info := range infoMap {
		if info.State == member.Alive && id != f.Id {
			leaveMessage := utils.Message{
				Type:       utils.Leave,
				SenderInfo: infoMap[id],
				InfoMap:    make(map[uint64]member.Info), // Empty map
			}
			utils.SendMessage(leaveMessage, info.Hostname, info.Port)
		}
	}

	// Update local state
	f.State = Stop
	log.Printf("Voluntarily left the group")
}