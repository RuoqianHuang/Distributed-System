package leader

import (
	"cs425/mp4/internal/detector"
	"cs425/mp4/internal/flow"
	"cs425/mp4/internal/hydfs"
	"cs425/mp4/internal/member"
	"cs425/mp4/internal/stream"
	"cs425/mp4/internal/utils"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	RAINSTORM_LEADER_PORT_UDP	= 9000
	RAINSTORM_LEADER_PORT_RPC   = 9001
	RAINSTORM_WORKER_BASE_PORT	= 6666
	WorkDir                     = "/cs425/mp4"
)

type OpStage struct {
	Exe  string   // Path of the executable binary
	Args string   // argument for the execution
	Type string   // operator type: filter, transform, aggregate
}

type Leader struct {
	TaskName string
	WorkerBinaryPath string
	MyHost   string
	MyUDPPort   int
	MyRPCPort   int

	NumStages        int
	NumTasksPerStage int
	NumTasksStages   []int
	Stages           []OpStage

	HydfsFileSource string
	HydfsFileDest   string

	WaitedTuples    map[string]string
	muWaitedTuples  sync.RWMutex
	DoneTuples      map[string]bool
	muDoneTuples    sync.RWMutex

	AutoScale     bool
	InputRate     int
	LowWatermark  int
	HighWatermark int

	hydfsClient   *hydfs.HYDFS
	fd            *detector.FD
	flow          *flow.FlowCounter

	vmRRIndex     int
	VMWorkerCount map[string]int
}


func GetNewLeader(
	taskName string,
	workerBinaryPath string,
	myHost string,
	myUDPPort int,
	myRPCPort int,
	numStages int,
	numTasksPerStage int,
	stages []OpStage,
	hydfsFileSource string,
	hydfsFileDest string,
	autoScale bool,
	inputRate int,
	lowWatermark int,
	highWatermark int) (*Leader, error) {
	
	flow := flow.NewFlowCounter()
	fd := detector.GetNewDetector(
		myHost, 
		myUDPPort, 
		myRPCPort, 
		0, 0, true, // stage, stageID, isLeader
		myHost, 
		myRPCPort, 
		myUDPPort, 
		flow,
	)
	
	numTasksStages := make([]int, numStages)
	for i := 0; i < numStages; i++ {
		numTasksStages[i] = numTasksPerStage
	}
	VMWorkerCount := make(map[string]int)
	for _, host := range utils.HOSTS {
		VMWorkerCount[host] = 0
	}
	
	hydfsClient, err := hydfs.NewClient()
	if err != nil {
		return nil, fmt.Errorf("fail to get hydfs client: %v", err)
	}

	leader := &Leader{
		TaskName:         taskName,
		WorkerBinaryPath: workerBinaryPath,
		MyHost:           myHost,
		MyUDPPort:        myUDPPort,
		MyRPCPort:        myRPCPort,
		NumStages:        numStages,
		NumTasksPerStage: numTasksPerStage,
		NumTasksStages:   numTasksStages,
		Stages:           stages,
		HydfsFileSource:  hydfsFileSource,
		HydfsFileDest:    hydfsFileDest,
		WaitedTuples:     make(map[string]string),
		DoneTuples:       make(map[string]bool),
		AutoScale:        autoScale,
		InputRate:        inputRate,
		LowWatermark:     lowWatermark,
		HighWatermark:    highWatermark,
		hydfsClient:      hydfsClient,
		fd: 			  fd,
		flow:             flow,
		vmRRIndex:        0,
		VMWorkerCount:    VMWorkerCount,
	}
	return leader, nil
}

func (l *Leader) Init() error {
	// Start the failure detector
	log.Printf("[Leader] Starting failure detector...")
	go l.fd.Start()

	// Register RPC
	rpc.Register(l)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", l.MyRPCPort))
	if err != nil {
		return fmt.Errorf("leader rpc listen failed: %v", err)
	}
	log.Printf("[Leader] RPC listening on %d", l.MyRPCPort)
	go rpc.Accept(listener)

	time.Sleep(time.Second)
	
	// Start workers for each stage
	// Note: We start from Stage 1. Stage 0 is the Source (Leader).
	for i := 1; i <= l.NumStages; i++ {
		l.NumTasksStages[i - 1] = l.NumTasksPerStage
		for j := 0; j < l.NumTasksPerStage; j++ {
			err := l.startWorker(i, j)
			if err != nil {
				return fmt.Errorf("failed to start worker for stage %d id %d: %v", i, j, err)
			}
		}
	}

	time.Sleep(time.Second)
	// Start Resource Monitor (Autoscaling and fail restart)
	go l.ResourceMonitor()

	return nil
}

func (l *Leader) getNextVM() member.Info {
	vmAddr := utils.HOSTS[l.vmRRIndex]
	l.vmRRIndex = (l.vmRRIndex + 1) % len(utils.HOSTS)

	// 2 port per worker, udpPort and RPC port
	// Calculate ports: Base + (Count * 2)
	// Example: Base=6666. Worker 0 -> UDP=6666, TCP=6667
	// Worker 1 -> UDP=6668, TCP=6669
	port_base := RAINSTORM_WORKER_BASE_PORT + l.VMWorkerCount[vmAddr] * 2
	l.VMWorkerCount[vmAddr]++

	return member.Info{
		Hostname: vmAddr,
		UDPPort: port_base,
		RPCPort: port_base + 1,
	}
}

func (l *Leader) stopWorker(stage int, stageID int) error {
	satgeWorkers := l.fd.Membership.GetAliveMembersInStage(stage)
	
	info, ok := satgeWorkers[stageID]
	if ok {
		log.Printf("[Leader] Stopping worker %s-%d-%d, at %s", l.TaskName, stage, stageID, info.Hostname)
		
		// Stop call
		reply := new(bool)
		utils.RemoteCall("Worker.Stop", info.Hostname, info.RPCPort, false, reply)
	}
	return nil
}

func (l *Leader) startWorker(stage int, stageID int) error {
	// TODO: spawn a new worker
	taskName := l.TaskName
	workerExe := l.Stages[stage - 1].Exe
	workerArgs := l.Stages[stage - 1].Args

	workerAddr := l.getNextVM()
	log.Printf("[Leader] Starting Worker %s [Stage %d, ID %d] on %s:(udp: %d, rpc: %d)", taskName, stage, stageID, workerAddr.Hostname, workerAddr.UDPPort, workerAddr.RPCPort)

	isLastStage := (stage == l.NumStages)
	isNextStageScalable := false
	numNextTasks := l.NumTasksPerStage

	if !isLastStage && l.AutoScale && l.Stages[stage].Type != "aggregate" {
		isNextStageScalable = true
	}

	// "ssh <host> mkdir -p <WorkDir>"
	cmdMkdir := exec.Command("ssh", workerAddr.Hostname, "mkdir", "-p", WorkDir)
	if err := cmdMkdir.Run(); err != nil {
		log.Printf("Warning: mkdir failed on %s: %v", workerAddr.Hostname, err)
		// Continue anyway, maybe it exists
	}

	remoteBinary := fmt.Sprintf("%s/worker", WorkDir)
	
	// For simplicity, we just copy.
	cmdScp := exec.Command("scp", l.WorkerBinaryPath, fmt.Sprintf("%s:%s", workerAddr.Hostname, remoteBinary))
	if err := cmdScp.Run(); err != nil {
		return fmt.Errorf("failed to scp binary: %v", err)
	}

	// Copy Operator Executable 
	remoteOp := fmt.Sprintf("%s/stage-%d", WorkDir, stage)
	if _, err := os.Stat(workerExe); err == nil {
		cmdScpOp := exec.Command("scp", workerExe, fmt.Sprintf("%s:%s/stage-%d", workerAddr.Hostname, WorkDir, stage))
		cmdScpOp.Run()
	}

	// Spawn Worker Process via SSH
	// Construct the massive command string
	// Unique Name for pkill: taskName-stage-stageID
	uniqueName := fmt.Sprintf("%s-%d-%d", l.TaskName, stage, stageID)
	
	cmdStr := fmt.Sprintf(
		"nohup %s/worker -name=%s -host=%s -rpc=%d -udp=%d "+
			"-op=%s -op_args='%s' -stage=%d -stage_id=%d "+
			"-leader_host=%s -leader_udp=%d -leader_rpc=%d "+
			"-last=%t -next_scalable=%t -next_tasks=%d "+
			"> %s/worker_%s.log 2>&1 &",
		WorkDir, uniqueName, workerAddr.Hostname, workerAddr.RPCPort, workerAddr.UDPPort,
		remoteOp, workerArgs, stage, stageID,
		l.MyHost, l.MyUDPPort, l.MyRPCPort,
		isLastStage, isNextStageScalable, numNextTasks,
		WorkDir, uniqueName,
	)

	cmdSpawn := exec.Command("ssh", "-n", "-f", workerAddr.Hostname, cmdStr)
	if err := cmdSpawn.Run(); err != nil {
		return fmt.Errorf("failed to spawn worker process: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	return nil
}

func (l *Leader) DoTask() {
	
	// Download the source file
	data, err := l.hydfsClient.Get(l.HydfsFileSource)
	if err != nil {
		l.StopAllWorker()
		log.Fatalf("[Leader] Failed get the source file: %v, exiting...", err)
	}
	
	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		tuple := stream.Tuple{
			ID: fmt.Sprintf("%s:%d", l.HydfsFileSource, i),
			Key: fmt.Sprintf("%s:%d", l.HydfsFileSource, i),
			Value: line,
			SourceHost: l.MyHost,
			SourceRPCPort: l.MyRPCPort,
		}
		// update tuple
		l.muWaitedTuples.Lock()
		l.WaitedTuples[tuple.ID] = tuple.Value
		l.muWaitedTuples.Unlock()
		
		// send tuple
		err := l.sendTuple(tuple)
		if err != nil {
			log.Printf("[Leader] Error sending tuple: %v", err)
		}
		if l.AutoScale {
			time.Sleep(time.Second / time.Duration(l.InputRate + 5))
		}
	}

	for {
		l.muWaitedTuples.RLock()
		waitedSize := len(l.WaitedTuples)
		l.muWaitedTuples.RUnlock()

		if waitedSize == 0 {
			// all jobs are done
			log.Print("[Leader] All tasks are done!!!")
			break
		}
		log.Printf("[Leader] %d tuples not acked, resending", waitedSize)
		l.muWaitedTuples.RLock()
		for id, value := range l.WaitedTuples {
			tuple := stream.Tuple{
				ID: id,
				Key: id,
				Value: value,
				SourceHost: l.MyHost,
				SourceRPCPort: l.MyRPCPort,
			}
			// log.Printf("[Leader] [DEBUG] tuple %s not done, resend...", id)
			err := l.sendTuple(tuple)
			if err != nil {
				log.Printf("[Leader] Error sending tuple: %v", err)
			}
			time.Sleep(time.Millisecond * 300)
		}
		l.muWaitedTuples.RUnlock()

		time.Sleep(time.Second)
	}

	l.StopAllWorker()
	l.hydfsClient.Close()
}

func (l *Leader) sendTuple(tuple stream.Tuple) error {
	candidates := l.fd.Membership.GetAliveMembersInStage(1)
	if len(candidates) == 0 {
		return fmt.Errorf("can't find live worker for stage 1")
	}

	// hash partitioning
	h := fnv.New32a()
	h.Write([]byte(tuple.Key))

	isLastStage := (l.NumStages == 1)
	isNextStageScalable := false

	if !isLastStage && l.AutoScale && l.Stages[0].Type != "aggregate" {
		isNextStageScalable = true
	}

	if isNextStageScalable {
		targetStageID := int(h.Sum32()) % len(candidates)

		i := 0
		for _, node := range candidates {
			if targetStageID == i {
				reply := new(bool)
				err := utils.RemoteCall("Worker.HandleTuple", node.Hostname, node.RPCPort, tuple, reply)
				return err
			}
			i++
		}	
	} else {
		targetStageID := int(h.Sum32()) % l.NumTasksStages[0]
		node, ok := candidates[targetStageID]
		if ok {
			reply := new(bool)
			err := utils.RemoteCall("Worker.HandleTuple", node.Hostname, node.RPCPort, tuple, reply)
			return err
		}
	}
	return fmt.Errorf("can't find worker for stage 1")
}

func (l *Leader) HandleResult(t stream.Tuple, _ *bool) error {
	l.muDoneTuples.Lock()
	defer l.muDoneTuples.Unlock()

	if l.DoneTuples[t.ID] {
		return fmt.Errorf("tuple %s already done", t.ID)
	}
	log.Printf("Tuple %s processed: %s: %s", t.ID, t.Key, t.Value)
	
	// update done tuples
	l.DoneTuples[t.ID] = true
	
	// write to hydfs
	l.hydfsClient.Append(l.HydfsFileDest, fmt.Sprintf("%s: %s\n", t.Key, t.Value))
	return nil
} 

func (l *Leader) HandleAck(id string, _ *bool) error {
	l.muWaitedTuples.Lock()
	defer l.muWaitedTuples.Unlock()
	log.Printf("[Leader] tuple %s acked", id)
	delete(l.WaitedTuples, id)
	return nil
}

func (l *Leader) StopAllWorker() {
	log.Printf("[Leader] Exiting, stopping all workers...")
	for i := 1; i <= l.NumStages; i++ {
		for j := 0; j < l.NumTasksStages[i - 1]; j++ {
			l.stopWorker(i, j)
		}
	}
}

func (l *Leader) ResourceMonitor() {
	
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {	
		// Iterate through stages
		for i := 1; i <= l.NumStages; i++ {
			
			stageWorkers := l.fd.Membership.GetAliveMembersInStage(i)

			for j := 0; j < l.NumTasksStages[i - 1]; j++ {
				_, ok := stageWorkers[j]
				if !ok { // worker failed, restart...
					l.startWorker(i, j)
				}
			}
			

			// auto scale
			if l.AutoScale && l.Stages[i - 1].Type != "aggregate" {
				var total_flow float64 = 0.0;
				for _, worker := range stageWorkers {
					total_flow += worker.Flow
				}

				if total_flow < float64(l.LowWatermark) {
					// start a machine
					l.startWorker(i, l.NumTasksStages[i - 1])
					l.NumTasksStages[i - 1]++
				} 

				if total_flow > float64(l.HighWatermark) {
					// stop a machine 
					l.NumTasksStages[i - 1]--
					l.stopWorker(i, l.NumTasksStages[i - 1])
				}
			}
		}
	}
}

func (l *Leader) GetMember(_ bool, reply *map[uint64]member.Info) error {
	*reply = l.fd.Membership.GetInfoMap()
	return nil
}