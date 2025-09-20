package member


import (
	"fmt"
	"time"
	"sync"
	"crypto/sha256"
	"encoding/binary"
	"cs425/mp2/internal/utils"
)

// member states
type MemberState int

const (
	Alive MemberState = iota
	Failed
	Suspected
)

var stateName = map[ServerState]string{
	Alive: "Alive",
	Failed: "Failed",
	Suspected: "Suspected",
}

type Info struct {
	hostname string      // hostname
	port int             // port number
	timestamp time.Time  // local time stamp
	counter int64        // heartbeat counter
	state MemberState    // member state
}

type Membership struct {
	lock *sync.RWMutex       // read write mutex
	members map[int64]Info   // member map
	memberIds []int64        // random permutation of member ids for gossip or pinging
}

func (m *Membership) Merge(memberInfo Membership, currentTime time.Time) bool {
	// merge memberInfo into m
	m.lock.Lock()
	defer m.lock.Unlock()
	memberChanged := false
	for id, info := range memberInfo.members {
		if member, ok := m.members[id]; ok {
			if info.state == Failed {
				if member.state != Failed {
					info.timestamp = currentTime
					m.members[id] = info // update to failed state
				}
			}else if info.counter > member.counter {
				// update member info with higher counter
				info.timestamp = currentTime
				m.members[id] = info
			}
		}else {
			info.timestamp = currentTime
			m.members[id] = info // add new member
			memberChanged = true
		}
	}
	if memberChanged { 
		m.memberIds = make([]int64, 0, len(m.members))
		for id, info := range m.members {
			if info.state != Failed {
				m.memberIds = append(m.memberIds, id)
			}
		}
		utils.RandomPermutation(&m.memberIds) // randomize member ids for gossip or pinging
	}
	return memberChanged
}

func (m *Membership) UpdateState(currentTime time.Time, Tfail time.Duration, Tsuspect time.Duration) bool {
	// check member states based on timestamps and thresholds
	m.lock.Lock()
	defer m.lock.Unlock()
	anyFailed := false
	for id, info := range m.members {
		elapsed := currentTime.Sub(info.timestamp)
		if info.state == Alive && elapsed > Tsuspect {
			info.state = Suspected
			info.timestamp = currentTime
			m.members[id] = info
		}else if info.state == Suspected && elapsed > Tfail {
			info.state = Failed
			info.timestamp = currentTime
			m.members[id] = info
		}
	}
	if anyFailed {
		m.memberIds = make([]int64, 0, len(m.members))
		for id, info := range m.members {
			if info.state != Failed {
				m.memberIds = append(m.memberIds, id)
			}
		}
		utils.RandomPermutation(&m.memberIds) // randomize member ids for gossip or pinging
	}
	return anyFailed
}	

func (m *Membership) Cleanup(currentTime time.Time, Tcleanup time.Duration) {
	// remove failed members
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, info := range m.members {
		if info.state == Failed && currentTime.Sub(info.timestamp) > Tcleanup {
			delete(m.members, id) // remove failed members
		}
	}
}

func (m *Membership) String() string {
	res := "Membership:\n"
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, id := range m.memberIds {
		info := m.members[id]
		res += fmt.Sprintf("ID: %d, Hostname: %s, Port: %d, Timestamp: %s, Counter: %d, State: %s\n",
			id, info.hostname, info.port, info.timestamp.String(), info.counter, stateName[info.state])
	}
	return res
}

func hashInfo(info Info) int64 {
	// hash hostname, port, and timestamp to 64 bit integer for map lookup
	hash := sha256.Sum256([]byte fmt.Sprintf("%s:%d:%s", info.hostname, info.port, info.timestamp.String()))
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

