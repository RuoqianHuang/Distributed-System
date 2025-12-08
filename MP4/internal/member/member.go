package member

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// member states
type MemberState int

const (
	Alive MemberState = iota
	Failed
	Suspected
)

var stateName = map[MemberState]string{
	Alive:     "Alive",
	Failed:    "Failed",
	Suspected: "Suspected",
}

type Info struct {
	Id        uint64      // Id
	Hostname  string      // hostname
	RPCPort   int         // rpc port number
	UDPPort   int         // udp port number

	Stage     int         // Stage number, 0 for leader
	StageID   int         // In stage id, ranging from 0, ...	
	Pid       int         // pid

	Version   time.Time   // version timestamp
	Timestamp time.Time   // local timestamp
	Counter   uint64      // heartbeat counter
	State     MemberState // member state

	Flow      float64     // flow counter
}

type Membership struct {
	lock            sync.RWMutex    // read write mutex
	Members         []uint64        // list of member Ids that is randomly permuted for gossip or pinging
	InfoMap         map[uint64]Info // member info map (might contain info of failed members)
	roundRobinIndex int             // random permutation round robin index
}

func (i *Info) String() string {
	return fmt.Sprintf("%d (Stage: %d, StageID: %d, Hostname: %s, Port: %d)", i.Id, i.Stage, i.StageID, i.Hostname, i.RPCPort)
}

// functions for failure detector
func (m *Membership) updateMember() {
	m.Members = make([]uint64, 0, len(m.InfoMap))
	for id, info := range m.InfoMap {
		if info.State != Failed {
			m.Members = append(m.Members, id)
		}
	}
	RandomPermutation(&m.Members) // randomize member ids for gossip
	m.roundRobinIndex = 0         // reset round robin index
}

func (m *Membership) Merge(memberInfo map[uint64]Info, currentTime time.Time) bool {
	// merge memberInfo into m
	m.lock.Lock()
	defer m.lock.Unlock()
	memberChanged := false
	for id, info := range memberInfo {
		if info.UDPPort == 0 || info.RPCPort == 0 || info.Hostname == "" {
			continue
		}
		if member, ok := m.InfoMap[id]; ok {
			if info.State == Failed {
				if member.State != Failed {
					info.Timestamp = currentTime
					m.InfoMap[id] = info // update to failed state
					log.Printf("FAILED: Node %s failed\n", info.String())
					memberChanged = true
				}
			} else if info.Counter > member.Counter {
				// update member info with higher counter
				info.Timestamp = currentTime
				m.InfoMap[id] = info
			}
		} else if info.State != Failed {
			info.Timestamp = currentTime
			m.InfoMap[id] = info // add new member
			memberChanged = true
		}
	}
	if memberChanged {
		m.updateMember()
	}
	return memberChanged
}

func (m *Membership) UpdateStateGossip(currentTime time.Time, Tfail time.Duration, Tsuspect time.Duration, suspicionEnabled bool) bool {
	// check member states based on timestamps and thresholds
	m.lock.Lock()
	defer m.lock.Unlock()
	anyFailed := false
	for id, info := range m.InfoMap {
		elapsed := currentTime.Sub(info.Timestamp)
		oldState := info.State
		if oldState == Alive && elapsed > Tsuspect {
			if suspicionEnabled {
				info.State = Suspected
				info.Timestamp = currentTime
				m.InfoMap[id] = info

				if oldState != Suspected {
					log.Printf("[FD] SUSPECTED: Node %s is now suspected\n", info.String())
				}
			} else {
				// Skip suspicion, go directly to failed
				log.Printf("[FD] FAILED: Node %s failed\n", info.String())
				info.State = Failed
				info.Timestamp = currentTime
				m.InfoMap[id] = info
				anyFailed = true
			}
		} else if oldState == Suspected && elapsed > Tfail {
			log.Printf("[FD] FAILED: Node %s failed\n", info.String())
			info.State = Failed
			info.Timestamp = currentTime
			m.InfoMap[id] = info
			anyFailed = true
		}
	}
	if anyFailed {
		m.updateMember()
	}
	return anyFailed
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

func (m *Membership) GetTarget() (Info, error) {
	// Round Robin with random permutation
	// TODO: maybe not to send message to self
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.Members) == 0 {
		return Info{}, fmt.Errorf("no existing member")
	}
	targetInfo, ok := m.InfoMap[m.Members[m.roundRobinIndex]]
	if !ok {
		return Info{}, fmt.Errorf("inconsistent membership")
	}
	m.roundRobinIndex++
	if m.roundRobinIndex == len(m.Members) {
		RandomPermutation(&m.Members)
		m.roundRobinIndex = 0
	}
	return targetInfo, nil
}

func (m *Membership) Exists(hostname string, udpPort int, rpcPort int) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, info := range m.InfoMap {
		if info.Hostname == hostname && info.UDPPort == udpPort && info.RPCPort == rpcPort {
			return true
		}
	}
	return false
}

func (m *Membership) GetInfo(id uint64) (Info, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	info, ok := m.InfoMap[id]
	if !ok {
		return Info{}, fmt.Errorf("id not exist")
	}
	return info, nil
}

func (m *Membership) GetInfoMap() map[uint64]Info {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// Create a new map to hold the copy.
	infoMapCopy := make(map[uint64]Info, len(m.InfoMap))

	// Copy the data from the internal map to the new one.
	for id, info := range m.InfoMap {
		infoMapCopy[id] = info
	}

	return infoMapCopy // Return the safe copy
}

func (m *Membership) Heartbeat(id uint64, currentTime time.Time, curentFlow float64) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.InfoMap[id]
	if ok {
		if info.State == Failed {
			return fmt.Errorf("node failed")
		}
		info.Timestamp = currentTime
		info.Counter++
		info.Flow = curentFlow
		m.InfoMap[id] = info
		return nil
	}
	return fmt.Errorf("no such id: %d, the node might fail", id)
}

func (m *Membership) RemoveMember(id uint64) {
	// remove a member completely (for voluntary leave)
	m.lock.Lock()
	defer m.lock.Unlock()

	// Remove from InfoMap
	delete(m.InfoMap, id)
	
	// update member list and sorted member list
	m.updateMember()
}

func (m *Membership) GetAliveMembersInStage(targetStage int) map[int]Info {
	m.lock.RLock()
	defer m.lock.RUnlock()

	targets := make(map[int]Info)
	for _, info := range m.InfoMap {
		if info.State == Alive && info.Stage == targetStage {
			targets[info.StageID] = info
		}
	}
	return targets
}

func HashInfo(info Info) uint64 {
	// hash hostname, port, and timestamp to 64 bit integer for map lookup
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%d:%d", info.Hostname, info.RPCPort, info.Version.Format(time.RFC3339Nano), info.Stage, info.StageID)))
	return uint64(binary.BigEndian.Uint64(hash[:8]))
}

func RandomPermutation(arr *[]uint64) {
	rng := rand.NewSource(time.Now().UnixNano())
	n := len(*arr)
	for i := 0; i < n; i++ {
		j := rng.Int63() % int64(i+1)
		if int64(i) != j {
			(*arr)[i], (*arr)[j] = (*arr)[j], (*arr)[i] // swap elements
		}
	}
}

func CreateTable(infoMap map[uint64]Info) (string, []uint64) {
	type Pair struct {
		Id       uint64
		Stage    int
		StageID  int
	}
	infoList := make([]Pair, 0, 16)
	for id, info := range infoMap {
		infoList = append(infoList, Pair{
			Id:       id,
			Stage: 	  info.Stage,
			StageID:  info.StageID,
		})
	}
	// sort by stage, then stageID
	sort.Slice(infoList, func(i, j int) bool {
		if infoList[i].Stage != infoList[j].Stage {
			return infoList[i].Stage < infoList[j].Stage
		}
		return infoList[i].StageID < infoList[j].StageID
	})
	sortedId := make([]uint64, 0, len(infoList))
	for _, pair := range infoList {
		sortedId = append(sortedId, pair.Id)
	}
	// ---------------------------------------------------------------------------------------------
	// | ID    |  Hostname | PID | RPC Port | Stage | StageID | Timestamp | Counter | Flow | State |
	// | ID    |  Hostname | PID | RPC Port | Stage | StageID | Timestamp | Counter | Flow | State |
	// ---------------------------------------------------------------------------------------------
	maxLengths := map[string]int{
		"ID":        2,
		"Hostname":  8,
		"PID":       3,
		"RPC Port":  8,
		"Stage":     5,
		"Stage ID":  8,
		"Timestamp": 9,
		"Counter":   7,
		"Flow":      4,
		"State":     5,
	}
	for _, i := range infoList {
		info := infoMap[i.Id]
		lengths := map[string]int{
			"ID":        len(fmt.Sprintf("%d", i.Id)),
			"Hostname":  len(info.Hostname),
			"PID":       len(fmt.Sprintf("%d", info.Pid)),
			"RPC Port":  len(fmt.Sprintf("%d", info.RPCPort)),
			"Stage":     len(fmt.Sprintf("%d", info.Stage)),
			"Stage ID":  len(fmt.Sprintf("%d", info.StageID)),
			"Timestamp": len(info.Timestamp.Format(time.RFC3339Nano)),
			"Counter":   len(fmt.Sprintf("%d", info.Counter)),
			"Flow":      len(fmt.Sprintf("%f", info.Flow)),
			"State":     len(stateName[info.State]),
		}
		for key, value := range lengths {
			if maxLengths[key] < value {
				maxLengths[key] = value
			}
		}
	}
	Columns := []string{
		"ID", "Hostname", "PID", "RPC Port", "Stage", "Stage ID", "Timestamp", "Counter", "Flow", "State",
	}
	
	totalLength := 3 * len(Columns) + 1
	for _, v := range maxLengths {
		totalLength = totalLength + v
	}
	res := strings.Repeat("-", totalLength) + "\n"

	// column headers
	header := "| "
	for i, col := range Columns {
		s := col
		if len(col) < maxLengths[s] {
			s = s + strings.Repeat(" ", maxLengths[s] - len(s))
		}
		if i == len(Columns) - 1 {
			header = header + s + " |" 
		} else {
			header = header + s + " | " 
		}
	}

	res = res + header + "\n"
	res = res + strings.Repeat("-", totalLength) + "\n"

	for _, i := range infoList {
		info := infoMap[i.Id]
		line := "| "

		// Id
		s := fmt.Sprintf("%d", i.Id)
		if len(s) < maxLengths["ID"] {
			s = s + strings.Repeat(" ", maxLengths["ID"]-len(s))
		}
		line = line + s + " | "

		// Hostname
		s = info.Hostname
		if len(s) < maxLengths["Hostname"] {
			s = s + strings.Repeat(" ", maxLengths["Hostname"]-len(s))
		}
		line = line + s + " | "

		// PID
		s = fmt.Sprintf("%d", info.Pid)
		if len(s) < maxLengths["PID"] {
			s = s + strings.Repeat(" ", maxLengths["PID"]-len(s))
		}
		line = line + s + " | "

		// RPC Port
		s = fmt.Sprintf("%d", info.RPCPort)
		if len(s) < maxLengths["RPC Port"] {
			s = s + strings.Repeat(" ", maxLengths["RPC Port"]-len(s))
		}
		line = line + s + " | "

		// Stage
		s = fmt.Sprintf("%d", info.Stage)
		if len(s) < maxLengths["Stage"] {
			s = s + strings.Repeat(" ", maxLengths["Stage"]-len(s))
		}
		line = line + s + " | "

		// Stage ID
		s = fmt.Sprintf("%d", info.StageID)
		if len(s) < maxLengths["Stage ID"] {
			s = s + strings.Repeat(" ", maxLengths["Stage ID"]-len(s))
		}
		line = line + s + " | "

		// Timestamp
		s = info.Timestamp.Format(time.RFC3339Nano)
		if len(s) < maxLengths["Timestamp"] {
			s = s + strings.Repeat(" ", maxLengths["Timestamp"]-len(s))
		}
		line = line + s + " | "

		// Counter
		s = fmt.Sprintf("%d", info.Counter)
		if len(s) < maxLengths["Counter"] {
			s = s + strings.Repeat(" ", maxLengths["Counter"]-len(s))
		}
		line = line + s + " | "

		// Flow
		s = fmt.Sprintf("%f", info.Flow)
		if len(s) < maxLengths["Flow"] {
			s = s + strings.Repeat(" ", maxLengths["Flow"]-len(s))
		}
		line = line + s + " | "

		// State
		s = stateName[info.State]
		if len(s) < maxLengths["State"] {
			s = s + strings.Repeat(" ", maxLengths["State"]-len(s))
		}
		line = line + s + " |"

		res = res + line + "\n"
	}
	res = res + strings.Repeat("-", totalLength) + "\n"
	return res, sortedId
}