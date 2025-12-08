package stream

import (
	"bufio"
	"bytes"
	"cs425/mp4/internal/detector"
	"cs425/mp4/internal/flow"
	"cs425/mp4/internal/hydfs"
	"cs425/mp4/internal/queue"
	"cs425/mp4/internal/utils"
	"fmt"
	"hash/fnv"
	"io"
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
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
	
)


type Tuple struct {
	ID		  	   string
	Key 	  	   string
	Value 	   	   string
	SourceHost     string
	SourceRPCPort  int
}




// OperatorRunner manages the external task process
type OperatorRunner struct {
	cmd       *exec.Cmd
	stdin     io.WriteCloser
	stdout    *bufio.Scanner
	stderr    *bytes.Buffer // Capture error logs from Python
	stderrMu  sync.Mutex    // Safety for reading stderr while writing
	exePath   string
	mu        sync.Mutex
}

// NewOperatorRunner starts the external process
func NewOperatorRunner(exePath string, args string) (*OperatorRunner, error) {
	// Prepare Command
	var cmd *exec.Cmd
	if args != "" {
		// Note: simplified splitting. For complex args, consider a better parser.
		// For this demo, we assume single argument string usually.
		cmd = exec.Command(exePath, args)
	} else {
		cmd = exec.Command(exePath)
	}

	// 1. Pipe Stdin
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdin pipe: %v", err)
	}

	// 2. Pipe Stdout
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to create stdout pipe: %v", err)
	}

	// 3. Pipe Stderr
	// We capture stderr to a buffer so we can print it if the task crashes
	stderrBuf := &bytes.Buffer{}
	cmd.Stderr = stderrBuf

	// 4. Start the Process
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start command %s: %v", exePath, err)
	}
	log.Printf("Runner on %s created", exePath)

	return &OperatorRunner{
		cmd:     cmd,
		stdin:   stdin,
		stdout:  bufio.NewScanner(stdoutPipe),
		stderr:  stderrBuf,
		exePath: exePath,
	}, nil
}

// Helper to read fields like "key: ..."
func (r *OperatorRunner) readField(prefix string) (string, error) {
	if r.stdout.Scan() {
		line := r.stdout.Text()
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix), nil
		}
		return "", fmt.Errorf("protocol mismatch: expected prefix '%s', got '%s'", prefix, line)
	}
	// If scan fails, return the error + stderr content
	return "", r.scanError()
}

// Helper to construct a detailed error message including stderr
func (r *OperatorRunner) scanError() error {
	scErr := r.stdout.Err()
	
	// Read stderr content safely
	r.stderrMu.Lock()
	stderrContent := r.stderr.String()
	r.stderrMu.Unlock()

	if scErr != nil {
		return fmt.Errorf("scanner error: %v | Stderr: %s", scErr, stderrContent)
	}
	return fmt.Errorf("process closed stdout (EOF) | Stderr: %s", stderrContent)
}

// ProcessTuple sends a tuple to the operator and waits for the response
func (r *OperatorRunner) ProcessTuple(t Tuple) ([]Tuple, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// --- STEP 1: SANITIZE INPUT ---
	// Protocol relies on \n. If Key/Value has \n, the Python script reads partial lines and crashes.
	// Replace internal newlines with spaces.
	safeKey := strings.ReplaceAll(t.Key, "\n", " ")
	safeVal := strings.ReplaceAll(t.Value, "\n", " ")

	// --- STEP 2: SEND INPUT ---
	// Format:
	// key: <key>\n
	// value: <value>\n
	_, err := fmt.Fprintf(r.stdin, "key: %s\nvalue: %s\n", safeKey, safeVal)
	// log.Printf("[OP] [DEBUG] key: %s\nvalue: %s\n", safeKey, safeVal)
	if err != nil {
		return nil, fmt.Errorf("failed to write to op stdin: %v", err)
	}

	// --- STEP 3: READ ACTION ---
	if !r.stdout.Scan() {
		return nil, r.scanError()
	}

	action := strings.TrimSpace(r.stdout.Text())

	// --- STEP 4: HANDLE ACTION ---
	var results []Tuple

	switch action {
	case "filter":
		return []Tuple{}, nil

	case "forward":
		newKey, err := r.readField("key: ")
		if err != nil {
			return nil, err
		}

		newValue, err := r.readField("value: ")
		if err != nil {
			return nil, err
		}

		results = append(results, Tuple{
			ID: t.ID,
			Key: newKey, 
			Value: newValue,
			SourceHost: t.SourceHost,
			SourceRPCPort: t.SourceRPCPort,
		})
		return results, nil

	default:
		// Debugging help
		return nil, fmt.Errorf("unknown protocol action: '%s' | Stderr: %s", action, r.stderr.String())
	}
}

// Close cleans up the process
func (r *OperatorRunner) Close() {
	if r.stdin != nil {
		r.stdin.Close()
	}
	if r.cmd != nil {
		r.cmd.Wait()
	}
}

type Worker struct {
	TaskName  			string 
	MyHost string
    MyUDPPort int
	MyRPCPort int

	Stage     			  int   
	StageID               int

	AckedTuple            map[string]bool 
	muAckedTuple          *sync.RWMutex

	processingQueue       *queue.Queue

	inProcessingQueue     map[string]bool
	muInProcessingQueue       *sync.RWMutex
	
	hydfsClient           *hydfs.HYDFS
	runner                *OperatorRunner
	flow                  *flow.FlowCounter
	fd         		      *detector.FD

	HydfsLog              string
	isLastStage           bool
	isNextStageScalable   bool
	NumTasksNextStage     int
}

func GetWorker(
	taskName string,
	hostname string,
	rpcPort int,
	udpPort int,
	opExe string, 
	opArgs string, 
	isLastStage bool,
	isNextStageScalable bool,
	numTasksNextStage int,
	stage int, stageID int,
	leaderHost string,
	leaderUDPPort int,
	leaderRPCPort int) (*Worker, error) {

	// Create operator runner
	runner, err := NewOperatorRunner(opExe, opArgs)
	if err != nil {
        log.Fatalf("[SM] Failed to start operator: %v", err)
    }
	flowCounter := flow.NewFlowCounter()
    
	fd := detector.GetNewDetector(
		hostname, udpPort, rpcPort, 
		stage, stageID, false, 
		leaderHost, leaderRPCPort, leaderUDPPort, 
		flowCounter,
	)

	hydfsClient, err := hydfs.NewClient()
	if err != nil {
		return nil, fmt.Errorf("[SM] Fail to get hydfs client: %v", err)
	}

	return &Worker{
		TaskName: taskName,
		MyHost: hostname,
		MyUDPPort: udpPort,
		MyRPCPort: rpcPort,

		Stage: stage,
		StageID: stageID,
		
		AckedTuple: make(map[string]bool),
		muAckedTuple: new(sync.RWMutex),

		processingQueue: queue.NewQueue(),
		
		inProcessingQueue: make(map[string]bool),
		muInProcessingQueue: new(sync.RWMutex),

		hydfsClient: hydfsClient,
		runner: runner,
		flow: flowCounter,
		fd: fd,

		HydfsLog: fmt.Sprintf("%s-%d-%d.log", taskName, stage, stageID),
		isLastStage: isLastStage,
		isNextStageScalable: isNextStageScalable,
		NumTasksNextStage: numTasksNextStage,
	}, nil
}


func (w *Worker) sendAck(targetHost string, targetPort int, tupleID string) {
    // We assume the upstream worker exposes a "Worker.HandleAck" RPC
    var reply bool
    // This can be fire-and-forget or retried. 
    // Since upstream retries the Tuple if it doesn't get an ACK, 
    // we can just send this once.
    go func() {
		if w.Stage == 1 {
			utils.RemoteCall("Leader.HandleAck", targetHost, targetPort, tupleID, &reply)
		} else {
			utils.RemoteCall("Worker.HandleAck", targetHost, targetPort, tupleID, &reply)
		}
	}()
}

func (w *Worker) getTargetHost(key string) (string, int, bool) {
	candidates := w.fd.Membership.GetAliveMembersInStage(w.Stage + 1)
	if len(candidates) == 0 {
		return "", 0, false
	}
	
	// hash partitioning
	h := fnv.New32a()
	h.Write([]byte(key))

	if w.isNextStageScalable {
		targetStageID := int(h.Sum32()) % len(candidates)

		i := 0
		for _, node := range candidates {
			if targetStageID == i {
				return node.Hostname, node.RPCPort, true
			}
			i++
		}
	
	} else {
		targetStageID := int(h.Sum32()) % w.NumTasksNextStage
		node, ok := candidates[targetStageID]
		if ok {
			return node.Hostname, node.RPCPort, true
		}
	}
	return "", 0, false
}

func (w *Worker) RecoverStateFromHyDFS() error {
	w.muAckedTuple.Lock()
	defer w.muAckedTuple.Unlock()

	logFileName := fmt.Sprintf("%s-%d-%d.log", w.TaskName, w.Stage, w.StageID)
	
	// first, create empty. If exist, nothing happen
	err := w.hydfsClient.CreateEmpty(logFileName)
	if err == nil {
		log.Printf("[SM] Replay file %s created, starting worker process...", logFileName)
		return nil
	}

	// download file from hydfs
	data, err := w.hydfsClient.Get(logFileName)
	if err != nil {
		return fmt.Errorf("fail to recover from hydfs: %v", err)
	}

	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "PROCESSED,") {
			tupleID := strings.TrimPrefix(line, "PROCESSED,")
			w.AckedTuple[tupleID] = true // recover from log
		}
	}
	// remove temp file
	log.Printf("[SM] Recovered %d processed tuples from log", len(w.AckedTuple))
	return nil
}

// RPC call for receiving tuple
func (w *Worker) HandleTuple(t Tuple, _ *bool) error {
	// add to queue
	w.muInProcessingQueue.Lock()
	_, inq := w.inProcessingQueue[t.ID]
	if !inq {
		w.processingQueue.Push(t)
		w.inProcessingQueue[t.ID] = true
	}
	w.muInProcessingQueue.Unlock()
	return nil
}

// RPC call for receiving ack (Optional, if using async acks)
func (w *Worker) HandleAck(id string, _ *bool) error {
	// update local state
	w.muAckedTuple.Lock()
	w.AckedTuple[id] = true
	w.muAckedTuple.Unlock()
	
	// log to HyDFS
	err := w.hydfsClient.Append(w.HydfsLog, fmt.Sprintf("PROCESSED,%s\n", id))
	if err != nil {
		log.Fatalf("[SM] Fail to append to HyDFS: %v", err)
	}
	
	// send ack back
	prev := w.fd.Membership.GetAliveMembersInStage(w.Stage - 1)
	for _, info := range prev {
		w.sendAck(info.Hostname, info.RPCPort, id)
	}
	return nil
}

func (w *Worker) StartFD() {
	w.fd.Start()
}

func (w *Worker) RunRPCServer() error {
    rpc.Register(w)
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", w.MyRPCPort))
    if err != nil {
        return err
    }
    rpc.Accept(listener)
    return nil
}

func (w *Worker) Stop(_ bool, reply *bool) error {
	w.fd.StopAndLeave()
	w.hydfsClient.Close()
	go func() {
		// terminates after 1 second
		time.Sleep(time.Second)
		os.Exit(0)
	}()
	return nil
}

func (w *Worker) LogFlowLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		log.Printf("[SM] Current flow %f/s", w.flow.Get())
	}
}

func (w *Worker) TupleProcessLoop() {
	for {
		val := w.processingQueue.Pop()
		tuple := val.(Tuple)
		
		w.muInProcessingQueue.Lock()
		delete(w.inProcessingQueue, tuple.ID)
		w.muInProcessingQueue.Unlock()

		w.flow.Add(1) // Update metrics
		// Exactly-Once Deduplication

		w.muAckedTuple.Lock()
		_, acked := w.AckedTuple[tuple.ID]
		w.muAckedTuple.Unlock()

		if acked {
			log.Printf("[SM] Tuple %s (%s: %s) rejected", tuple.ID, tuple.Key, tuple.Value)
			// Already processed, treat as success (Idempotent)
			// Critical: Even if duplicate, we MUST ack. 
			// The sender might be retrying because the previous ack was lost.
			w.sendAck(tuple.SourceHost, tuple.SourceRPCPort, tuple.ID)
			continue
		}

		outputTuples, err := w.runner.ProcessTuple(tuple)
		if err != nil {
			// Melformed operator, stop!!
			log.Fatalf("[SM] Error processing tuple %s: %v", tuple.ID, err)
		}

		if len(outputTuples) == 0 { // Filtered, send ack directly
			// log to HyDFS
			err := w.hydfsClient.Append(w.HydfsLog, fmt.Sprintf("PROCESSED,%s\n", tuple.ID))
			if err != nil {
				log.Fatalf("[SM] Fail to append to HyDFS: %v", err)
			}

			// send ack back
			w.sendAck(tuple.SourceHost, tuple.SourceRPCPort, tuple.ID)

			// update local state
			w.muAckedTuple.Lock()
			w.AckedTuple[tuple.ID] = true
			w.muAckedTuple.Unlock()
		}
			
		if w.isLastStage {
			for _, outT := range outputTuples {
				
				// log result
				log.Printf("[SM] Tuple %s processed, result: (%s: %s)", outT.ID, outT.Key, outT.Value)

				// log to HyDFS
				err := w.hydfsClient.Append(w.HydfsLog, fmt.Sprintf("PROCESSED,%s\n", tuple.ID))
				if err != nil {
					log.Fatalf("[SM] Fail to append to HyDFS: %v", err)
				}
					
				// send ack back
				w.sendAck(tuple.SourceHost, tuple.SourceRPCPort, tuple.ID)

				// notify the leader
				reply := new(bool)
				utils.RemoteCall("Leader.HandleResult", w.fd.LeaderHost, w.fd.LeaderRPCPort, outT, reply)
				log.Printf("[SM] Tuple of ID: %s done.", outT.ID)

				// update local state
				w.muAckedTuple.Lock()
				w.AckedTuple[tuple.ID] = true
				w.muAckedTuple.Unlock()
			}
		} else {
			for _, outT := range outputTuples {
				outT.SourceHost = w.MyHost
				outT.SourceRPCPort = w.MyRPCPort
				
				// If fail to get next node, just ignore it, the tuple would be resent later
				targetHost, targetPort, ok := w.getTargetHost(outT.Key)
				if ok {
					var ack bool
					utils.RemoteCall("Worker.HandleTuple", targetHost, targetPort, outT, &ack)
				}
			}
		}
	}
}