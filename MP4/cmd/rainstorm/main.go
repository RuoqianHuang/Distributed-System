package main

import (
	"cs425/mp4/internal/leader"
	"cs425/mp4/internal/utils"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)


type RainStormConfig struct {
	NumStages 	    	int
	NumTaskPerStage 	int
	Stages          	[]leader.OpStage
	
	HydfsFileSources 	[]string 
	HydfsFileDest  		string

	ExactlyOnce     	bool
	AutoScale       	bool
	InputRate       	int	
	LowWatermark    	int
	HighWatermark   	int

	WorkerBinaryPath 	string
}

func ParseRainStormArgs() (*RainStormConfig, error) {
	args := os.Args[1:]


	if len(args) < 3 { 
		return nil, fmt.Errorf("insufficient arguments: expected at least <workerBinaryPath> <Nstages> <Ntasks>")
	}

	rainstorm := &RainStormConfig{}
	var err error 
	idx := 0

	// Read worker binary path
	rainstorm.WorkerBinaryPath = args[idx]
	fileInfo, err := os.Stat(args[idx])
	if err != nil {
		return nil, fmt.Errorf("failed to find worker binary: %s", args[idx])
	}
	// Check if worker is executable
	if fileInfo.Mode()&0111 == 0 {
		return nil, fmt.Errorf("worker %s is not executable", args[idx])
	}
	idx++

	// Read NumStages
	rainstorm.NumStages, err = strconv.Atoi(args[idx])
	if err != nil {
		return nil, fmt.Errorf("invalid Nstages: %v", err)
	}
	idx++

	// Read NumTasksPerStage
	rainstorm.NumTaskPerStage, err = strconv.Atoi(args[idx])
	if err != nil {
		return nil, fmt.Errorf("invalid Ntasks_per_stage: %v", err)
	}
	idx++

	// Read operator and args
	expectedOpArgs := rainstorm.NumStages * 2
	if len(args[idx:]) < expectedOpArgs {
		return nil, fmt.Errorf("missing operator arguments: expected %d pairs", rainstorm.NumStages)
	}

	for i := 0; i < rainstorm.NumStages; i++ {
		stage := leader.OpStage{
			Exe:  args[idx],
			Args: args[idx+1],
			Type: args[idx+2],
		}
		// Check if file exist
		fileInfo, err := os.Stat(stage.Exe)
		if err != nil {
			return nil, fmt.Errorf("error finding the file: %v", err)
		}
		// Check if file is executable
		mode := fileInfo.Mode()
		if mode&0111 == 0 {
			return nil, fmt.Errorf("file %s is not executable", stage.Exe)
		}

		// check operator type
		if stage.Type != "filter" && stage.Type != "transform" && stage.Type != "aggregate" {
			return nil, fmt.Errorf("unknown operator type: %s", stage.Type)
		}
		
		rainstorm.Stages = append(rainstorm.Stages, stage)
		idx += 3
	}

	// read source files
	if idx >= len(args) {
		return nil, fmt.Errorf("missing num of hydfs_src argument")
	}
	m, err := strconv.Atoi(args[idx])
	if err != nil {
		return nil, fmt.Errorf("invalid num of src files: %v", err)
	}
	idx++
	
	if m <= 0 {
		return nil, fmt.Errorf("invalid num of src file %d", m)
	}
	rainstorm.HydfsFileSources = make([]string, m)
	
	for i := 0; i < m; i++ {
		if idx >= len(args) {
			return nil, fmt.Errorf("missing hydfs_src argument")
		}
		rainstorm.HydfsFileSources[i] = args[idx]
		idx++
	}

	// Read autoscale enabled
	if idx >= len(args) {
		return nil, fmt.Errorf("missing autoscale_enabled argument")
	}
	rainstorm.AutoScale, err = strconv.ParseBool(args[idx])
	if err != nil {
		// accept 0 or 1
		val, ierr := strconv.Atoi(args[idx])
		if ierr == nil {
			rainstorm.AutoScale = (val != 0)
		} else {
			return nil, fmt.Errorf("invalid autoscale_enabled (use true/false or 1/0): %v", err)
		}
	}
	idx++

	if len(args[idx:]) < 5 {
		return nil, fmt.Errorf("missing RATE/LW/HW/hydfs_dest_filename/exactly_once arguments")
	}
	
	rainstorm.InputRate, err = strconv.Atoi(args[idx])
	if err != nil { return nil, fmt.Errorf("invalid INPUT_RATE") }
	idx++

	rainstorm.LowWatermark, err = strconv.Atoi(args[idx])
	if err != nil { return nil, fmt.Errorf("invalid LW") }
	idx++

	rainstorm.HighWatermark, err = strconv.Atoi(args[idx])
	if err != nil { return nil, fmt.Errorf("invalid HW") }
	idx++

	if rainstorm.LowWatermark > rainstorm.HighWatermark {
		return nil, fmt.Errorf("LowWatermark higer than HighWatermark")
	}

	// Read Hydfs dest file
	rainstorm.HydfsFileDest = args[idx]
	idx++


	// Read exactly once
	rainstorm.ExactlyOnce, err = strconv.ParseBool(args[idx])
	if err != nil {
		// accept 0/1
		val, ierr := strconv.Atoi(args[idx])
		if ierr == nil {
			rainstorm.ExactlyOnce = (val != 0)
		} else {
			return nil, fmt.Errorf("invalid exactly_once (use true/false or 1/0)")
		}
	}

	return rainstorm, nil
}

func main() {
	rainstorm, err := ParseRainStormArgs()
	if err != nil {
		fmt.Printf("Error parsing arguments: %v\n", err)
		fmt.Println("Usage: RainStorm <workerBinaryPath> <Nstages> <Ntasks> <op1> <arg1> <type1> ... <m_src> <src1> ... <src_m> <auto> [rate lw hw] <dest> <once>")
		os.Exit(1)
	}


	fmt.Printf("Leader Starting with %d stages...\n", rainstorm.NumStages)
	fmt.Printf("Source: %v -> Dest: %s\n", rainstorm.HydfsFileSources, rainstorm.HydfsFileDest)

	taskName := fmt.Sprintf("rainstorm-%v", time.Now().Format(time.RFC3339))

	hostname, err := utils.GetHostName()
	if err != nil {
		log.Fatalf("[Main] Failed to get hostname")
	}
	
	leader, err := leader.GetNewLeader(
		taskName, rainstorm.WorkerBinaryPath, hostname, leader.RAINSTORM_LEADER_PORT_UDP, leader.RAINSTORM_LEADER_PORT_RPC,
		rainstorm.NumStages, rainstorm.NumTaskPerStage, rainstorm.Stages, rainstorm.HydfsFileSources,
		rainstorm.HydfsFileDest, rainstorm.AutoScale, rainstorm.InputRate, rainstorm.LowWatermark, rainstorm.HighWatermark)

	if err != nil {
		log.Fatalf("[Main] Failed to get leader object: %v", err)
	}

	// Start FD and Workers
	err = leader.Init()
	if err != nil {
		log.Fatalf("[Main] Failed to initialize: %v", err)
	}

	// Start the task
	leader.DoTask()
	// leader.StreamTest(30 * time.Second)
}
