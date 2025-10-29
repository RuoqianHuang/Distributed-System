package detector

import (
	"cs425/mp3/internal/flow"
	"cs425/mp3/internal/member"
	"cs425/mp3/internal/utils"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const ProbeTimeout time.Duration = 2 * time.Second

type FD struct {
	Tround            time.Duration
	Tsuspect          time.Duration
	Tfail             time.Duration
	Tcleanup          time.Duration
	Id                uint64
	Info              member.Info
	Membership        *member.Membership
	InFlow            *flow.FlowCounter    // input network counter
	OutFlow           *flow.FlowCounter    // output network counter
}


func (f *FD) joinGroup() { // The initial join

	// If you are a leader, just start
	for _, leader := range utils.Leaders {
		if leader.Hostname == f.Info.Hostname && leader.Port == f.Info.Port {
			log.Printf("[FD] I am a leader. Starting failure detector!")
			return
		}
	}

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
	for _, leader := range utils.Leaders {
		size, err := utils.SendMessage(message, leader.Hostname, leader.Port)
		if err != nil {
			log.Printf("[FD] Failed to send probe message to %s:%d: %s", leader.Hostname, leader.Port, err.Error())
		} else {
			f.OutFlow.Add(size)
		}
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
	err := f.Membership.Heartbeat(f.Id, currentTime)
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
		size, err := utils.SendMessage(message, targetInfo.Hostname, targetInfo.Port)
		if err != nil {
			log.Printf("[FD] Failed to send gossip message to %s:%d: %s", targetInfo.Hostname, targetInfo.Port, err.Error())
		} else {
			f.OutFlow.Add(size)
		}
	}

	// If you are leader, send gossip to other leaders to prevent partition
	leaderIdx := utils.IsLeader(f.Info.Hostname, f.Info.Port)
	if leaderIdx != -1 { // I am a leader
		infoMap := f.Membership.GetInfoMap()
		for i, leader := range utils.Leaders {
			if i == leaderIdx {
				continue // Don't send to your self
			}
			message := utils.Message{
				Type:    utils.Gossip,
				InfoMap: infoMap,
			}
			size, err := utils.SendMessage(message, leader.Hostname, leader.Port)
			if err != nil {
				log.Printf("[FD] Failed to send gossage to leader %d: %s", i, err.Error())
			} else {
				f.OutFlow.Add(size)
			}
		}
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
		size, _, err := conn.ReadFromUDP(data)
		if err != nil {
			log.Printf("[FD] UDP Read error: %s", err.Error())
			continue
		}

		message, err := utils.Deserialize(data)
		if err != nil {
			log.Printf("[FD] Failed to deserialize message: %s", err.Error())
		} else {
			
			// Flow counter
			f.InFlow.Add(int64(size))

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
				size, err := utils.SendMessage(ackMessage, message.SenderInfo.Hostname, message.SenderInfo.Port)
				if err != nil {
					log.Printf("[FD] Failed to send probe ack message to %s: %s", message.SenderInfo.String(), err.Error())
				} else {
					f.OutFlow.Add(size)
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

		// Check if any leader in the info map
		isolated := true
		for _, leader := range utils.Leaders {
			if f.Membership.Exists(leader.Hostname, leader.Port) {
				isolated = false
				break
			}
		}
		if isolated {
			log.Fatal("[FD] Isolated from the leaders, restarting")
		}
		// Run gossip step
		f.gossipStep()
	}
}


func (f *FD) Start() {
	// Join group
	f.joinGroup()
	waitGroup := new(sync.WaitGroup)
	// Start UDP listener
	waitGroup.Add(1)
	go f.startUDPListenerLoop(waitGroup)
	// Start failure detector
	waitGroup.Add(1)
	go f.startFailureDetectorLoop(waitGroup)
	waitGroup.Wait()
}