package gossip

import (
	"fmt"
	"log"
	"time"
	"sync"
	"errors"
	"cs425/mp2/internal/utils"
	"cs425/mp2/internal/member"
)


type Gossip struct {
	Tfail time.Duration           // fail detection timeout
	Tgossip time.Duration         // gossip interval
	Tsuspect time.Duration        // suspect timeout
	Tcleanup time.Duration		  // cleanup timeout
	NextGossip int                // index of next member to gossip to
	Membership *member.Membership  // membership
	lock sync.RWMutex            // mutex for nextGossip
}

func (g *Gossip) NextTarget() (int64, error) {
	// get next gossip target from member list
	gossipTarget, err := g.Membership.GetMemberId(g.NextGossip)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("Error founding next target: %s", err.Error()))
	}
	g.lock.Lock()
	g.NextGossip++
	if g.NextGossip == len(g.Membership.Members) { 
		// finishi this round, random permutation and start next round
		g.Membership.RandomPermutation()
		g.NextGossip = 0
	}
	g.lock.Unlock()
	return gossipTarget, nil
}

func (g *Gossip) HandleRequest(info map[int64]member.Info) {
	// merge incoming membership info
	anyChanged := g.Membership.Merge(info, time.Now())
	if anyChanged {
		log.Printf("Membership updated: %s\n", g.Membership.String())
		g.lock.Lock()
		g.NextGossip = 0 // reset nextGossip
		g.lock.Unlock()
	}
}

func (g *Gossip) GossipStep(myId int64) error {
	currentTime := time.Now()
	
	// increase heartbeat counter
	err := g.Membership.Heartbeat(myId, currentTime)
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to heartbeat: %s", err.Error()))
	}
	
	// update state
	g.Membership.UpdateState(currentTime, g.Tfail, g.Tsuspect)

	// Cleanup
	g.Membership.Cleanup(currentTime, g.Tcleanup)

	// get next gossip target
	target, err := g.NextTarget()
	if err != nil {
		return errors.New(fmt.Sprintf("Failed to get next target: %s", err.Error()))
	}
	if target == myId {
		target, err = g.NextTarget()
		if err != nil {
			return errors.New(fmt.Sprintf("Failed to get next target: %s", err.Error()))
		}
	}

	// get target info
	targetInfo, err := g.Membership.GetInfo(target)
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