package main

import (
	"flag"
	"log"
	"os"
    "fmt"
	"cs425/mp4/internal/stream"
)

func ParseAndRunWorker() {
	taskName := flag.String("name", "", "Task Name (e.g., Task-1-0)")
	hostname := flag.String("host", "", "Worker Hostname or IP")
	rpcPort := flag.Int("rpc", 5555, "Worker RPC Port")
	udpPort := flag.Int("udp", 5556, "Worker Failure Detector UDP Port")
	
	opExe := flag.String("op", "", "Operator Executable Name (e.g., grep)")
	opArgs := flag.String("op_args", "", "Arguments for the Operator")
	
	isLastStage := flag.Bool("last", false, "Is this the last stage?")
	
	isNextScalable := flag.Bool("next_scalable", false, "Is the next stage scalable?")
	numNextTasks := flag.Int("next_tasks", 0, "Number of tasks in the next stage")
	
	stage := flag.Int("stage", 0, "Stage Number")
	stageID := flag.Int("stage_id", 0, "Task ID within the Stage")
	
	leaderHost := flag.String("leader_host", "", "Leader Hostname")
	leaderUDPPort := flag.Int("leader_udp", 1234, "Leader UDP Port")
	leaderRPCPort := flag.Int("leader_rpc", 1235, "Leader RPC Port")

	flag.Parse()

	// check arguments
	if *taskName == "" || *hostname == "" || *opExe == "" || *leaderHost == "" {
		fmt.Println("Error: Missing required arguments.")
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("[Main] Starting Worker %s [Stage %d, ID %d] on %s:(udp: %d, rpc: %d)", *taskName, *stage, *stageID, *hostname, *udpPort, *rpcPort)

	// Initialize worker
	worker, err := stream.GetWorker(
		*taskName,
		*hostname,
		*rpcPort,
		*udpPort,
		*opExe,
		*opArgs,
		*isLastStage,
		*isNextScalable,
		*numNextTasks,
		*stage,
		*stageID,
		*leaderHost,
		*leaderUDPPort,
		*leaderRPCPort,
	)

	if err != nil {
		log.Fatalf("[Main] Failed to initialize worker: %v", err)
	}

	// recover from replay
    if err := worker.RecoverStateFromHyDFS(); err != nil {
        log.Fatalf("[Main] Failed to recover: %v", err)
    }
    
	// start failure detector
    go worker.StartFD()
	go worker.LogFlowLoop()

	// start RPC server
	log.Printf("[Main] Worker %s is running...", worker.TaskName)
	if err := worker.RunRPCServer(); err != nil {
        log.Fatalf("Worker RPC Server crashed: %v", err)
    }
}

func main() {
	ParseAndRunWorker()
}	
