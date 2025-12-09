package leader

import (
	"cs425/mp4/internal/detector"
	"cs425/mp4/internal/flow"
	"cs425/mp4/internal/hydfs"
	"cs425/mp4/internal/member"
	"cs425/mp4/internal/queue"
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
	RAINSTORM_LEADER_PORT_UDP  = 9000
	RAINSTORM_LEADER_PORT_RPC  = 9001
	RAINSTORM_WORKER_BASE_PORT = 6666
	WorkDir                    = "/cs425/mp4"
	WORKER_SIZE_LIMIT          = 5
)

type OpStage struct {
	Exe  string // Path of the executable binary
	Args string // argument for the execution
	Type string // operator type: filter, transform, aggregate
}

type Leader struct {
	TaskName         string
	WorkerBinaryPath string
	MyHost           string
	MyUDPPort        int
	MyRPCPort        int

	NumStages        int
	NumTasksPerStage int
	NumTasksStages   []int
	Stages           []OpStage

	HydfsFileSources []string
	HydfsFileDest    string

	WaitedTuples   map[string]string
	muWaitedTuples sync.RWMutex
	DoneTuples     map[string]bool
	muDoneTuples   sync.RWMutex
	resendQueue    *queue.Queue

	AutoScale     bool
	InputRate     int
	LowWatermark  int
	HighWatermark int

	hydfsClient *hydfs.HYDFS
	fd          *detector.FD
	flow        *flow.FlowCounter

	vmRRIndex     int
	VMWorkerCount map[string]int

	stopMonitorChan chan struct{}
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
	hydfsFileSources []string,
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
	// create empty file for result
	err = hydfsClient.CreateEmpty(hydfsFileDest)
	if err != nil {
		return nil, fmt.Errorf("fail to create empty hydfs file for results: %v", err)
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
		HydfsFileSources: hydfsFileSources,
		HydfsFileDest:    hydfsFileDest,
		WaitedTuples:     make(map[string]string),
		DoneTuples:       make(map[string]bool),
		resendQueue:      queue.NewQueue(),
		AutoScale:        autoScale,
		InputRate:        inputRate,
		LowWatermark:     lowWatermark,
		HighWatermark:    highWatermark,
		hydfsClient:      hydfsClient,
		fd:               fd,
		flow:             flow,
		vmRRIndex:        0,
		VMWorkerCount:    VMWorkerCount,
		stopMonitorChan:  make(chan struct{}),
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

	time.Sleep(300 * time.Millisecond)

	// Start workers for each stage
	// Note: We start from Stage 1. Stage 0 is the Source (Leader).
	for i := 1; i <= l.NumStages; i++ {
		l.NumTasksStages[i-1] = l.NumTasksPerStage
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
	port_base := RAINSTORM_WORKER_BASE_PORT + l.VMWorkerCount[vmAddr]*2
	l.VMWorkerCount[vmAddr]++

	return member.Info{
		Hostname: vmAddr,
		UDPPort:  port_base,
		RPCPort:  port_base + 1,
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
	workerExe := l.Stages[stage-1].Exe
	workerArgs := l.Stages[stage-1].Args

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

func (l *Leader) StreamTest(duration time.Duration) {

	delayTime := max(time.Duration(0), time.Second/time.Duration(l.InputRate)-3*time.Millisecond)

	startTime := time.Now()

	count := 0
	for i := 0; time.Since(startTime) < duration; i++ {
		line := utils.RandStringRunes(256)

		id := fmt.Sprintf("stream_test:%d", i)
		tuple := stream.Tuple{
			ID:            id,
			Key:           id,
			Value:         line,
			SourceHost:    l.MyHost,
			SourceRPCPort: l.MyRPCPort,
		}

		// update tuple
		l.muWaitedTuples.Lock()
		l.WaitedTuples[tuple.ID] = tuple.Value
		l.muWaitedTuples.Unlock()
		l.resendQueue.Push(tuple.ID)

		// send tuple
		l.sendTuple(tuple)
		count++

		time.Sleep(delayTime) // Input rate control
	}
	log.Printf("[Leader] done sending all tuples. %d tuples send", count)

	for l.resendQueue.Size() > 0 {
		v := l.resendQueue.Pop()
		id := v.(string)

		l.muWaitedTuples.RLock()
		value, ok := l.WaitedTuples[id]
		l.muWaitedTuples.RUnlock()

		if ok {
			tuple := stream.Tuple{
				ID:            id,
				Key:           id,
				Value:         value,
				SourceHost:    l.MyHost,
				SourceRPCPort: l.MyRPCPort,
			}
			l.sendTuple(tuple)
			time.Sleep(delayTime) // Input rate control
			l.resendQueue.Push(id)
		}
	}

	l.StopMonitor()
	l.StopAllWorker()
	l.hydfsClient.Close()
}

func (l *Leader) DoTask() {

	delayTime := max(time.Duration(0), time.Second/time.Duration(l.InputRate)-3*time.Millisecond)
	total_input := 0

	// Download the source file
	startTime := time.Now()
	for _, source_file := range l.HydfsFileSources {
		data, err := l.hydfsClient.Get(source_file)
		if err != nil {
			log.Printf("[Leader] Can't download file %s from hydfs: %v", source_file, err)
			continue
		}
		log.Printf("[Leader] file %s downloaded from HyDFS", source_file)

		lines := strings.Split(string(data), "\n")

		for i, line := range lines {
			id := fmt.Sprintf("%s:%d", source_file, i)
			tuple := stream.Tuple{
				ID:            id,
				Key:           id,
				Value:         line,
				SourceHost:    l.MyHost,
				SourceRPCPort: l.MyRPCPort,
			}

			// update tuple
			l.muWaitedTuples.Lock()
			l.WaitedTuples[tuple.ID] = tuple.Value
			l.muWaitedTuples.Unlock()
			l.resendQueue.Push(tuple.ID)

			// send tuple
			total_input += 1
			l.sendTuple(tuple)
			// err := l.sendTuple(tuple)
			// if err != nil {
			// 	log.Printf("[Leader] [DEBUG] Error sending tuple: %v", err)
			// } else {
			// 	log.Printf("[Leader] [DEBUG] tuple %s sent successfully.", tuple.ID)
			// }

			time.Sleep(delayTime) // Input rate control
		}

		log.Printf("[Leader] done sending all tuples of file %s. %d tuples send", source_file, len(lines))
	}

	for l.resendQueue.Size() > 0 {
		v := l.resendQueue.Pop()
		id := v.(string)

		l.muWaitedTuples.RLock()
		value, ok := l.WaitedTuples[id]
		l.muWaitedTuples.RUnlock()

		if ok {
			tuple := stream.Tuple{
				ID:            id,
				Key:           id,
				Value:         value,
				SourceHost:    l.MyHost,
				SourceRPCPort: l.MyRPCPort,
			}

			l.sendTuple(tuple)
			// err := l.sendTuple(tuple)
			// if err != nil {
			// 	log.Printf("[Leader] [DEBUG] Error resending tuple: %v", err)
			// } else {
			// 	log.Printf("[Leader] [DEBUG] tuple %s resent successfully.", tuple.ID)
			// }
			time.Sleep(delayTime) // Input rate control

			l.resendQueue.Push(id)
		}
	}

	elapseTime := time.Since(startTime)
	log.Printf("[Leader] %d tuple processed, output %d tuples", total_input, len(l.DoneTuples))
	log.Printf("[Leader] All tasks finish in %v.", elapseTime)
	log.Printf("[Leader] Average output throughput %f/s", float64(len(l.DoneTuples))/elapseTime.Seconds())

	// finishing...
	l.StopMonitor()
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
				if err == nil {
					l.flow.Add(1)
				}
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
			if err == nil {
				l.flow.Add(1)
			}
			return err
		}
	}
	return fmt.Errorf("can't find worker for stage 1")
}

func (l *Leader) HandleResult(t stream.Tuple, _ *bool) error {
	l.muDoneTuples.Lock()
	defer l.muDoneTuples.Unlock()

	_, ok := l.DoneTuples[t.ID]
	if ok {
		return fmt.Errorf("tuple %s already done", t.ID)
	}
	// log.Printf("Tuple %s processed: %s: %s", t.ID, t.Key, t.Value)

	// update done tuples
	l.DoneTuples[t.ID] = true

	// write to hydfs
	err := l.hydfsClient.Append(l.HydfsFileDest, fmt.Sprintf("%s: %s\n", t.Key, t.Value))
	if err != nil {
		log.Printf("[Leader] fail to append data to HyDFS: %v", err)
	}
	return nil
}

func (l *Leader) HandleAck(id string, _ *bool) error {
	l.muWaitedTuples.Lock()
	defer l.muWaitedTuples.Unlock()
	// log.Printf("[Leader] tuple %s acked", id)
	delete(l.WaitedTuples, id)
	return nil
}

func (l *Leader) StopAllWorker() {
	log.Printf("[Leader] Exiting, stopping all workers...")
	for i := 1; i <= l.NumStages; i++ {
		for j := 0; j < l.NumTasksStages[i-1]; j++ {
			l.stopWorker(i, j)
		}
	}
}

func (l *Leader) StopMonitor() {
	select {
	case <-l.stopMonitorChan:
		// Channel already closed, do nothing
	default:
		close(l.stopMonitorChan)
		log.Println("[Leader] Monitor stop signal sent.")
	}
}

func (l *Leader) ResourceMonitor() {

	// pendingWorkers tracks when we started a worker.
	// Key: "Stage-StageID", Value: Start Timestamp
	pendingWorkers := make(map[string]time.Time)

	// lastScaleTime tracks when we last scaled a stage to prevent oscillation
	// Key: Stage Index, Value: Last Scale Timestamp
	lastScaleTime := make(map[int]time.Time)

	// Configuration
	const BootupTimeout = 5 * time.Second  // Give workers 5s to join before restarting
	const ScaleCooldown = 10 * time.Second // Don't scale the same stage twice in short time

	for {
		select {
		case <-l.stopMonitorChan:
			log.Printf("[Leader] Resource monitor stopped")
			return // exit the loop and function

		default:
			// Iterate through stages
			for i := 1; i <= l.NumStages; i++ {

				stageWorkers := l.fd.Membership.GetAliveMembersInStage(i)

				for j := 0; j < l.NumTasksStages[i-1]; j++ {
					_, isAlive := stageWorkers[j]
					workerKey := fmt.Sprintf("%d-%d", i, j)

					if isAlive {
						// Worker is healthy, remove from pending list
						delete(pendingWorkers, workerKey)
					} else {
						// Worker is missing. Check if we just started it.
						lastStart, isPending := pendingWorkers[workerKey]

						if isPending && time.Since(lastStart) < BootupTimeout {
							// It's still booting up, wait!
							continue
						}

						log.Printf("[Leader] Worker %d at stage %d is missing/failed. Restarting...", j, i)

						// Restart and mark as pending
						l.startWorker(i, j)
						pendingWorkers[workerKey] = time.Now()
					}
				}

				// auto scale
				if l.AutoScale && l.Stages[i-1].Type != "aggregate" {

					// Check Cooldown: Don't scale if we just did
					if time.Since(lastScaleTime[i]) < ScaleCooldown {
						continue
					}

					var total_flow float64 = 0.0
					for _, worker := range stageWorkers {
						total_flow += worker.Flow
					}

					// Scale UP
					if total_flow > float64(l.HighWatermark) && l.NumTasksStages[i-1] < WORKER_SIZE_LIMIT {
						newID := l.NumTasksStages[i-1]
						log.Printf("[Leader] High Watermark %f, %f: Starting new worker %d at stage %d", total_flow, float64(l.LowWatermark), newID, i)

						// Start the worker
						l.startWorker(i, newID)

						// CRITICAL: Mark it as pending immediately so Part A doesn't restart it next loop
						pendingWorkers[fmt.Sprintf("%d-%d", i, newID)] = time.Now()

						l.NumTasksStages[i-1]++
						lastScaleTime[i] = time.Now()
					}

					// Scale DOWN
					if total_flow < float64(l.LowWatermark) && l.NumTasksStages[i-1] > 1 {
						removeID := l.NumTasksStages[i-1] - 1
						log.Printf("[Leader] Low Watermark %f, %f: Stopping worker %d at stage %d", total_flow, float64(l.HighWatermark), removeID, i)

						l.stopWorker(i, removeID)

						l.NumTasksStages[i-1]--
						lastScaleTime[i] = time.Now()
					}
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (l *Leader) GetMember(_ bool, reply *map[uint64]member.Info) error {
	*reply = l.fd.Membership.GetInfoMap()
	return nil
}
