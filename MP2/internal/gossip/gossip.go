package gossip

import (
	"fmt"
	"log"
	"time"
	"errors"
	"cs425/mp2/internal/utils"
	"cs425/mp2/internal/member"
)


type Gossip struct {
	Membership *member.Membership  // membership
}

func (g *Gossip) HandleRequest(message utils.Message) {
	// merge incoming membership info
	anyChanged := g.Membership.Merge(message.Info, time.Now())
	if anyChanged {
		log.Printf("Membership updated: %s\n", g.Membership.String())
	}
}

func (g *Gossip) GossipStep(
	myId int64,
	Tfail time.Duration,
	Tsuspect time.Duration,
	Tcleanup time.Duration) error {
	currentTime := time.Now()
	
	// increase heartbeat counter
	err := g.Membership.Heartbeat(myId, currentTime)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to heartbeat: %s", err.Error()))
	}
	
	// update state
	g.Membership.UpdateState(currentTime, Tfail, Tsuspect)

	// cleanup
	g.Membership.Cleanup(currentTime, Tcleanup)

	// get gossip target info
	targetInfo, err := g.Membership.GetTarget()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to get target info: %s", err.Error()))
	}

	// copy member info map to send
	infoMap := g.Membership.GetInfoMap()

	// Send Gossip 
	message := utils.Message{
		Type: utils.Gossip,
		Info: infoMap,
	}
	utils.SendInfo(message, targetInfo.Hostname, targetInfo.Port)

	return nil
}