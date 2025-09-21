package member


import (
	"fmt"
	"time"
	"sync"
	"errors"
	"math/rand"
	"crypto/sha256"
	"encoding/binary"
)

// member states
type MemberState int

const (
	Alive MemberState = iota
	Failed
	Suspected
)

var stateName = map[MemberState]string{
	Alive: "Alive",
	Failed: "Failed",
	Suspected: "Suspected",
}

type Info struct {
	Hostname string      // hostname
	Port int             // port number
	Version time.Time    // version timestamp
	Timestamp time.Time  // local timestamp
	Counter int64        // heartbeat counter
	State MemberState    // member state
}

type Membership struct {
	lock sync.RWMutex         // read write mutex
	Members []int64           // list of member Ids that is randomly permuted for gossip or pinging
	InfoMap map[int64]Info    // member info map (might contain info of failed members)
	roundRobinIndex int       // random permutation round robin index
}

func (i *Info) String() string {
	return fmt.Sprintf("Hostname: %s, Port: %d, Version: %s", i.Hostname, i.Port, i.Version.String())
}

func (m *Membership) Merge(memberInfo map[int64]Info, currentTime time.Time) bool {
	// merge memberInfo into m
	m.lock.Lock()
	defer m.lock.Unlock()
	memberChanged := false
	for id, info := range memberInfo {
		if member, ok := m.InfoMap[id]; ok {
			if info.State == Failed {
				if member.State != Failed {
					info.Timestamp = currentTime
					m.InfoMap[id] = info // update to failed state
				}
			}else if info.Counter > member.Counter {
				// update member info with higher counter
				info.Timestamp = currentTime
				m.InfoMap[id] = info
			}
		}else {
			info.Timestamp = currentTime
			m.InfoMap[id] = info // add new member
			memberChanged = true
		}
	}
	if memberChanged { 
		m.Members = make([]int64, 0, len(m.InfoMap))
		for id, info := range m.InfoMap {
			if info.State != Failed {
				m.Members = append(m.Members, id)
			}
		}
		RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
		m.roundRobinIndex = 0 // reset round robin index
	}
	return memberChanged
}

func (m *Membership) UpdateStateGossip(currentTime time.Time, Tfail time.Duration, Tsuspect time.Duration) bool {
	// check member states based on timestamps and thresholds
	m.lock.Lock()
	defer m.lock.Unlock()
	anyFailed := false
	for id, info := range m.InfoMap {
		elapsed := currentTime.Sub(info.Timestamp)
		if info.State == Alive && elapsed > Tsuspect {
			info.State = Suspected
			info.Timestamp = currentTime
			m.InfoMap[id] = info
		}else if info.State == Suspected && elapsed > Tfail {
			info.State = Failed
			info.Timestamp = currentTime
			m.InfoMap[id] = info
			anyFailed = true
		}
	}
	if anyFailed {
		m.Members = make([]int64, 0, len(m.InfoMap))
		for id, info := range m.InfoMap {
			if info.State != Failed {
				m.Members = append(m.Members, id)
			}
		}
		RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
		m.roundRobinIndex = 0 // reset round robin index
	}
	return anyFailed
}	

func (m *Membership) UpdateStateSwim(currentTime time.Time, id int64, state MemberState) bool {
	// update state for a specific id, return true if any member beceom Failed.
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.InfoMap[id]
	if ok && info.State != Failed {
		info.Timestamp = currentTime
		info.State = state
		m.InfoMap[id] = info
		return state == Failed
	}
	return false
}

func (m *Membership) Cleanup(currentTime time.Time, Tcleanup time.Duration) {
	// remove failed members
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, info := range m.InfoMap {
		if info.State == Failed && currentTime.Sub(info.Timestamp) > Tcleanup {
			delete(m.InfoMap, id) // remove info about failed members
		}
	}
}

func (m *Membership) String() string {
	res := "Membership:\n"
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, id := range m.Members {
		info := m.InfoMap[id]
		res += fmt.Sprintf("ID: %d, Hostname: %s, Port: %d, Version: %s, Timestamp: %s, Counter: %d, State: %s\n",
			id, info.Hostname, info.Port, info.Version.String(), info.Timestamp.String(), info.Counter, stateName[info.State])
	}
	return res
}

func (m *Membership) GetTarget() (Info, error) {
	// Round Robin with random permutation
	// TODO: maybe not to send message to self
	m.lock.Lock()
	defer m.lock.Unlock()
	targetInfo, ok := m.InfoMap[m.Members[m.roundRobinIndex]]
	if !ok {
		return Info{}, errors.New(fmt.Sprintf("Inconsistent membership!!!"))
	}
	m.roundRobinIndex++
	if m.roundRobinIndex == len(m.Members) {
		RandomPermutation(&m.Members)
		m.roundRobinIndex = 0
	}
	return targetInfo, nil
}

func (m *Membership) GetInfoMap() map[int64]Info {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.InfoMap
}

func (m *Membership) Heartbeat(id int64, currentTime time.Time) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.InfoMap[id]
	if ok {
		info.Timestamp = currentTime
		info.Counter++
		m.InfoMap[id] = info
	}
	return errors.New(fmt.Sprintf("No such ID: %d, the node might fail!!!", id))
}

func HashInfo(info Info) int64 {
	// hash hostname, port, and timestamp to 64 bit integer for map lookup
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", info.Hostname, info.Port, info.Version.String())))
	return int64(binary.BigEndian.Uint64(hash[:8]))
}

func RandomPermutation(arr *[]int64)  {
	rng := rand.NewSource(time.Now().UnixNano())
	n := len(*arr)
	for i := 0; i < n; i++ {
		j := rng.Int63() % int64(i + 1)
		(*arr)[i], (*arr)[j] = (*arr)[j], (*arr)[i] // swap elements
	}
}