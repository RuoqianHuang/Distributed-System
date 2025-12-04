package stream

import (
	"bufio"
	"cs425/mp4/internal/flow"
	"cs425/mp4/internal/detector"
	"hash/fnv"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os/exec"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
	HYDFS_PORT         = 8788
)


type Tuple struct {
	ID		  	string
	Key 	  	string
	Value 	   	string
	SourceHost  string
	SourcePort  int
}


// HyDFS Append RPC Argument Structure
type AppendPack struct {
	Filename string
	Data     []byte
} 

// OperatorRunner manages the external task process
type OperatorRunner struct {
    cmd     *exec.Cmd
    stdin   io.WriteCloser
    stdout  *bufio.Scanner
    exeName string
}

// NewOperatorRunner starts the external process at /cs425/mp4/<exeName>
// args: command line arguments (e.g., grep pattern)
func NewOperatorRunner(exeName string, args string) (*OperatorRunner, error) {
    // 1. Construct full path
    fullPath := fmt.Sprintf("/cs425/mp4/%s", exeName)

    // 2. Prepare Command
    var cmd *exec.Cmd
    if args != "" {
        // If args exist, pass them. 
        // Note: If args are complex (e.g. multiple flags), 
        // you might need strings.Split(args, " ")
        cmd = exec.Command(fullPath, args)
    } else {
        cmd = exec.Command(fullPath)
    }

    // 3. Create Pipes
    stdin, err := cmd.StdinPipe()
    if err != nil {
        return nil, fmt.Errorf("failed to create stdin pipe: %v", err)
    }

    stdoutPipe, err := cmd.StdoutPipe()
    if err != nil {
        return nil, fmt.Errorf("failed to create stdout pipe: %v", err)
    }

    // 4. Start the Process
    if err := cmd.Start(); err != nil {
        return nil, fmt.Errorf("failed to start command %s: %v", fullPath, err)
    }

    return &OperatorRunner{
        cmd:     cmd,
        stdin:   stdin,
        stdout:  bufio.NewScanner(stdoutPipe),
        exeName: exeName,
    }, nil
}

func (r *OperatorRunner) readField(prefix string) (string, error) {
	if r.stdout.Scan() {
		line := r.stdout.Text()
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix), nil
		}
	}
	return "", fmt.Errorf("fail to read field from operator") 
}

// ProcessTuple sends a tuple to the operator and waits for the response
func (r *OperatorRunner) ProcessTuple(t Tuple) ([]Tuple, error) {
    // --- STEP 1: SEND INPUT ---
    
    // Protocol:
    // key: <key>\n
    // value: <value>\n
    // Note: We intentionally use Fprintf with explicit \n to match the protocol strictly
    _, err := fmt.Fprintf(r.stdin, "key: %s\nvalue: %s\n", t.Key, t.Value)
    if err != nil {
        return nil, fmt.Errorf("failed to write to op stdin: %v", err)
    }

    // --- STEP 2: READ ACTION ---
    
    if !r.stdout.Scan() {
        if err := r.stdout.Err(); err != nil {
            return nil, fmt.Errorf("error reading op stdout: %v", err)
        }
        return nil, fmt.Errorf("operator process closed stdout unexpectedly (EOF)")
    }
    
    // TrimSpace removes any accidental trailing \r or \n from the script output
    action := strings.TrimSpace(r.stdout.Text())

    // --- STEP 3: HANDLE ACTION ---

    var results []Tuple

    switch action {
    case "filter":
        // Logic: Ignore this tuple, return empty list
        return []Tuple{}, nil

    case "forward":
        // Logic: Expect exactly two more lines: Key and Value
        newKey, err := r.readField("key: ")
        if err != nil {
            return nil, err
        }

        newValue, err := r.readField("value: ")
        if err != nil {
            return nil, err
        }

        results = append(results, Tuple{Key: newKey, Value: newValue})
        return results, nil

    default:
        // Robustness: If the user script prints debug info or garbage, we fail here.
        // Tip: Tell user scripts to print debug info to stderr, not stdout!
        return nil, fmt.Errorf("unknown protocol action from operator: '%s'", action)
    }
}

// Close cleans up the process
func (r *OperatorRunner) Close() {
    if r.stdin != nil {
        r.stdin.Close()
    }
    if r.cmd != nil {
        // Wait allows the process to exit gracefully and cleans up zombie processes
        r.cmd.Wait()
    }
}

type Worker struct {
	TaskName  			string 
	MyHost string
    MyPort int

	Stage     			  int   
	StageID               int

	ProcessedTuple 		  map[string]Tuple // Deduplication sets
	AckedTuple            map[string]bool 
	
	runner                *OperatorRunner
	flow                  *flow.FlowCounter
	mu                    sync.Mutex
	fd         		      *detector.FD

	HydfsFileDest         string
	isLastStage           bool
	isNextStageScalable   bool
	NumTasksNextStage     int
}

func GetWorker(
	taskName string,
	hostname string,
	port int,
	udpPort int,
	opExe string, 
	opArgs string, 
	isLastStage bool,
	hydfsFileDest string,
	isNextStageScalable bool,
	numTasksNextStage int,
	stage int, stageID int,
	leaderHost string,
	leaderPort int) (*Worker, error) {

	// Create operator runner
	runner, err := NewOperatorRunner(opExe, opArgs)
	if err != nil {
        log.Fatalf("[SM] Failed to start operator: %v", err)
    }
	flowCounter := flow.NewFlowCounter()
    
	// TODO: read log and create log
	

	return &Worker{
		TaskName: taskName,
		MyHost: hostname,
		MyPort: port,

		Stage: stage,
		StageID: stageID,
		
		ProcessedTuple: make(map[string]Tuple),
		AckedTuple: make(map[string]bool),

		runner: runner,
		flow: flowCounter,
		fd: detector.GetNewDetector(hostname, udpPort, stage, stageID, false, leaderHost, leaderPort, flowCounter),

		HydfsFileDest: hydfsFileDest,
		isLastStage: isLastStage,
		isNextStageScalable: isNextStageScalable,
		NumTasksNextStage: numTasksNextStage,
	}, nil
}

func (w *Worker) RemoteCall(
	funcName string,
	hostname string,
	port int,
	args any,
	reply any,
) error {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call(funcName, args, reply)
	}()
	select {
	case err = <-callChan:
		if err != nil {
			return err
		}
	case <-time.After(CALL_TIMEOUT):
		return fmt.Errorf("%s call to server %s:%d timed out", funcName, hostname, port)
	}
	return nil
}

func (w *Worker) appendHyDFSFile(filename, content string) error {
	args := AppendPack{Filename: filename, Data: []byte(content + "\n")}
	var reply bool
	return w.RemoteCall("DistributedFiles.AppendBytes", "127.0.0.1", HYDFS_PORT, args, &reply)
}

func (w *Worker) sendAck(targetHost string, targetPort int, tupleID string) {
    // We assume the upstream worker exposes a "Worker.HandleAck" RPC
    var reply bool
    // This can be fire-and-forget or retried. 
    // Since upstream retries the Tuple if it doesn't get an ACK, 
    // we can just send this once.
    go func() {
		w.RemoteCall("Worker.HandleAck", targetHost, targetPort, tupleID, &reply)
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
				return node.Hostname, node.Port, true
			}
			i++
		}
	
	} else {
		targetStageID := int(h.Sum32()) % w.NumTasksNextStage
		node, ok := candidates[targetStageID]
		if ok {
			return node.Hostname, node.Port, true
		}
	}
	return "", 0, false
}

type Args struct {
	Command    string
	Filename   string
	FileSource string
	VMAddress  string
}

func (w *Worker) RecoverStateFromHyDFS() error {
	logFileName := fmt.Sprintf("%s-%d-%d.log", w.TaskName, w.Stage, w.StageID)
	
	// first, create empty. If exist, nothing happen
	reply := new(bool)
	w.RemoteCall("DistributedFiles.CreateEmpty", w.fd.LeaderHost, w.fd.LeaderPort, logFileName, reply)

	// download file from hydfs
	tempLogFile := fmt.Sprintf("%s-%d-%d.log.temp", w.TaskName, w.Stage, w.StageID)
	args := Args{
		Command: "get",
		Filename: logFileName,
		FileSource: tempLogFile,
	}	
	result := new(string)
	err := w.RemoteCall("Server.CLI", w.fd.LeaderHost, w.fd.LeaderPort, args, result)
	if err != nil {
		return fmt.Errorf("fail to recover from hydfs: %v", err)
	}
	
	// read file
	data, err := os.ReadFile(tempLogFile)
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
	
	log.Printf("[SM] Recovered %d processed tuples from log", len(w.ProcessedTuple))
	return nil
}

// RPC call for receiving tuple
func (w *Worker) HandleTuple(t Tuple, _ *bool) error {
	w.mu.Lock()
	// 1. Exactly-Once Deduplication
	if w.AckedTuple[t.ID] {
		w.mu.Unlock()
		// Already processed, treat as success (Idempotent)
		// Critical: Even if duplicate, we MUST ack. 
        // The sender might be retrying because the previous ack was lost.
        w.sendAck(t.SourceHost, t.SourcePort, t.ID)
		return nil
	}
	w.mu.Unlock()

	// 2. Process via External Operator
	outputTuples, err := w.runner.ProcessTuple(t)
	if err != nil {
		// Melformed operator, stop!!
		log.Fatalf("[SM] Error processing tuple %s: %v", t.ID, err)
	}

	if !w.isLastStage {
		// last stage does not need this to send ack back
		w.ProcessedTuple[t.ID] = t // update processed tuple
	}
	
	if w.isLastStage {
		for _, outT := range outputTuples {
			// TODO: notify leader
			line := fmt.Sprintf("%s: %s", outT.Key, outT.Value)
			
			// update local state
			w.mu.Lock()
			w.AckedTuple[t.ID] = true
			w.mu.Unlock()

			// log to HyDFS
			w.appendHyDFSFile(fmt.Sprintf("%s-%d-%d.log", w.TaskName, w.Stage, w.StageID), fmt.Sprintf("PROCESSED,%s", t.ID))
			
			// write result to HyDFS
			w.appendHyDFSFile(w.HydfsFileDest, line)
			
			// send ack back
			w.sendAck(t.SourceHost, t.SourcePort, t.ID)
		}
	} else {
		for _, outT := range outputTuples {
			outT.SourceHost = w.MyHost
        	outT.SourcePort = w.MyPort
			
			// If fail to get next node, just ignore it, the tuple would be resent later
			targetHost, targetPort, ok := w.getTargetHost(outT.Key)
			if ok {
				var ack bool
				go func() {
					w.RemoteCall("Worker.HandleTuple", targetHost, targetPort, outT, &ack)
				}()
			}
		}
	}

	w.flow.Add(1) // Update metrics

	return nil
}

// RPC call for receiving ack (Optional, if using async acks)
func (w *Worker) HandleAck(id string, _ *bool) error {
	// update local state
	w.mu.Lock()
	w.AckedTuple[id] = true
	w.mu.Unlock()
	
	// log to HyDFS
	w.appendHyDFSFile(fmt.Sprintf("%s-%d-%d.log", w.TaskName, w.Stage, w.StageID), fmt.Sprintf("PROCESSED,%s", id))
	
	// send ack back
	tuple, ok := w.ProcessedTuple[id]
	if ok {
		w.sendAck(tuple.SourceHost, tuple.SourcePort, tuple.ID)
	}
	return nil
}

func (w *Worker) StartFD() {
	w.fd.Start()
}

func (w *Worker) RunRPCServer() error {
    rpc.Register(w)
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", w.MyPort))
    if err != nil {
        return err
    }
    rpc.Accept(listener)
    return nil
}

func (w *Worker) Stop(_ bool, reply *bool) error {
	w.fd.StopAndLeave()
	go func() {
		// terminates after 1 second
		time.Sleep(time.Second)
		os.Exit(0)
	}()
	return nil
}