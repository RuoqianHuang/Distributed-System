package main

import (
	"crypto/rand"
	"cs425/mp3/internal/member"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type Args struct {
	Command    string
	Filename   string
	FileSource string
	VMAddress  string
}

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

func CallWithTimeout(
	funcName string,
	hostname string,
	port int,
	args any,
	result any) error {

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", hostname, port), CONNECTION_TIMEOUT)
	if err != nil {
		return fmt.Errorf("failed to dial server %s:%d: %s", hostname, port, err.Error())
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	callChan := make(chan error, 1)

	go func() {
		callChan <- client.Call(funcName, args, result)
	}()
	err = <-callChan
	if err != nil {
		return fmt.Errorf("rpc call to server %s:%d failed: %s", hostname, port, err.Error())
	}
	return nil
}

func GenerateAndUploadFiles(hostname string, port int, numFiles int, fileSize int64) {
	log.Printf("Generating %d files, each file has size %d", numFiles, fileSize)

	// Create a temp directory
	tempDir, err := os.MkdirTemp("", "hydfs_test_files_*")
	if err != nil {
		log.Fatalf("Fail to create temp folder: %v", err)
	}

	// Remember to remove tempDir
	defer os.RemoveAll(tempDir)

	log.Printf("Temp folder %s created", tempDir)

	for i := 0; i < numFiles; i++ {
		// Prepare files
		hydfsFilename := fmt.Sprintf("testfile_%d_%d.dat", time.Now().UnixNano(), i)
		localFilePath := filepath.Join(tempDir, hydfsFilename)

		log.Printf("---")
		log.Printf("Generating files %d/%d: %s", (i + 1), numFiles, localFilePath)

		// generate random content
		file, err := os.Create(localFilePath)
		if err != nil {
			log.Printf("Failed to create local file %s: %v", localFilePath, err)
			continue // skip
		}

		_, err = io.CopyN(file, rand.Reader, fileSize)
		if err != nil {
			log.Printf("Failed to to write content to %s: %v", localFilePath, err)
			file.Close()
			continue // skip
		}
		file.Close()

		// Upload file
		args := Args{
			Command:    "create",
			Filename:   hydfsFilename,
			FileSource: localFilePath,
		}
		result := ""
		err = CallWithTimeout("Server.CLI", hostname, port, args, &result)
		if err != nil {
			log.Printf("Failed to upload file %s: %s", hydfsFilename, err.Error())
		} else {
			log.Print(result)
		}
	}

	log.Printf("---")
	log.Printf("All %d files generated, temp folder %s is deleted", numFiles, tempDir)
}

func getSystemFlow(hostname string, port int) (float64, time.Time) {
	infoMap := new(map[uint64]member.Info)
	CallWithTimeout("Server.Member", hostname, port, 0, infoMap)

	waitGroup := new(sync.WaitGroup)

	lock := new(sync.RWMutex)
	var totalFlow float64 = 0.0
	for _, info := range *infoMap {
		if info.State != member.Failed {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				reply := new(float64)
				CallWithTimeout("Server.GetFlow", info.Hostname, info.Port+1, 0, reply)
				lock.Lock()
				totalFlow += *reply
				lock.Unlock()
			}()
		}
	}
	waitGroup.Wait()
	return totalFlow, time.Now()
}

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: ./plota <number of file> <file size>")
	}

	numOfFile, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Can't interpret %s", os.Args[1])
	}
	fileSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Can't interpret %s", os.Args[2])
	}

	port := 8788
	hostname := "localhost"
	// Preload the system with files
	GenerateAndUploadFiles(hostname, port, numOfFile, int64(fileSize))
	log.Printf("Wait for 10s...")
	time.Sleep(10 * time.Second)

	// Fail a machine
	failureNode := "fa25-cs425-b605.cs.illinois.edu"
	cmd := exec.Command("ssh", failureNode, "-t", "sudo systemctl stop MP3_server.service")
	err = cmd.Run()
	if err != nil {
		log.Fatalf("Failed to stop the machine: %s", err.Error())
	}
	log.Printf("%s stopped, recording for 10s!\n", failureNode)

	startTime := time.Now()
	for {
		flow, cur := getSystemFlow(hostname, port)
		fmt.Printf("%d %f\n", cur.Sub(startTime).Milliseconds(), flow)
		if cur.Sub(startTime) > time.Second*10 { // measure flow for 10s
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}
