package main

import (
	"cs425/mp4/internal/leader"
	"cs425/mp4/internal/member"
	"cs425/mp4/internal/utils"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "list":
		handleList()
	case "kill":
		handleKill()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  ./rainstorm_cli list")
	fmt.Println("  ./rainstorm_cli kill <Stage> <StageID>")
}

func handleList() {
	reply := new(map[uint64]member.Info)
	err := utils.RemoteCall("Leader.GetMember", "localhost", leader.RAINSTORM_LEADER_PORT_RPC, false, reply)
	if err != nil {
		log.Fatalf("Fail to get member info: %v", err)
	}
	table, _ := member.CreateTable(*reply)
	log.Printf("\n%s", table)
}

func handleKill() {
	if len(os.Args) < 4 {
		fmt.Println("Error: Missing Stage or StageID.")
		printUsage()
		os.Exit(1)
	}

	// Read Stage and StageID
	targetStage, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Invalid Stage number: %v", err)
	}
	if targetStage <= 0 {
		log.Fatalf("Invalid Stage number: %v", err)
	}

	targetStageID, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Invalid StageID number: %v", err)
	}

	// Get membership
	reply := new(map[uint64]member.Info)
	err = utils.RemoteCall("Leader.GetMember", "localhost", leader.RAINSTORM_LEADER_PORT_RPC, false, reply)
	if err != nil {
		log.Fatalf("Fail to get member info from Leader: %v", err)
	}

	// Find target Worker
	var targetHost string
	var targetPID int = -1

	found := false
	for _, info := range *reply {
		if info.Stage == targetStage && info.StageID == targetStageID {
			if info.State == member.Alive {
				targetHost = info.Hostname
				targetPID = info.Pid
				found = true
				break
			}
		}
	}

	if !found {
		log.Fatalf("Worker not found for Stage %d, ID %d (or it is not Alive)", targetStage, targetStageID)
	}

	if targetPID <= 0 {
		log.Fatalf("Found worker %s but PID is invalid (%d)", targetHost, targetPID)
	}

	// kill
	log.Printf("Found Worker: %s (PID: %d). Executing kill...", targetHost, targetPID)

	cmd := exec.Command("ssh", targetHost, "kill", "-9", strconv.Itoa(targetPID))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		log.Fatalf("Failed to kill remote process: %v", err)
	}

	log.Printf("Successfully killed Stage %d ID %d on %s", targetStage, targetStageID, targetHost)
}