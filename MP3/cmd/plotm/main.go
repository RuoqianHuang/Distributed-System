package main

import (
	"crypto/rand"
	"cs425/mp3/internal/utils"
	"fmt"
	"io"
	"log"
	mrand "math/rand"
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

// Character for random string generation
var LETTERS = []rune("abcdefghijklmnopqrstuvwxyz")

func RandStringRunes(n int) string {
	rng := mrand.NewSource(time.Now().UnixNano())
	arr := make([]rune, n)
	for i := range arr {
		arr[i] = LETTERS[rng.Int63() % int64(len(LETTERS))]
	}
	return string(arr)
}

func GenerateAndUpload(hostname string, port int, filename string, fileSize int64) error {
	log.Printf("Generating %s of size %d", filename, fileSize)

	// Create a temp directory
	tempDir, err := os.MkdirTemp("", "hydfs_test_files_*")
	if err != nil {
		log.Fatalf("Fail to create temp folder: %v", err)
	}

	// Remember to remove tempDir
	defer os.RemoveAll(tempDir)

	log.Printf("Temp folder %s created", tempDir)


	localFilePath := filepath.Join(tempDir, filename)

	log.Printf("---")
	log.Printf("Generating %s", localFilePath)

	// generate random content
	file, err := os.Create(localFilePath)
	if err != nil {
		return fmt.Errorf("failed to create local file %s: %v", localFilePath, err)
	}

	_, err = io.CopyN(file, rand.Reader, fileSize)
	if err != nil {	
		file.Close()
		return fmt.Errorf("failed to to write content to %s: %v", localFilePath, err)
	}
	file.Close()


	// Upload file
	args := Args{
		Command:    "create",
		Filename:   filename,
		FileSource: localFilePath,
	}
	result := ""
	err = CallWithTimeout("Server.CLI", hostname, port, args, &result)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %s", filename, err.Error())
	} else {
		log.Print(result)
	}
	return nil
}

func measureMerge(filename string, hostname string, port int) time.Duration {
	// Send Merge 
	for _, remoteHost := range utils.HOSTS {
		reply := new(bool)
		CallWithTimeout("DistributedFiles.MergeFile", remoteHost, 8788, filename, reply)
	}
	startTime := time.Now()
	for {
		reply := new(bool)
		err := CallWithTimeout("DistributedFiles.CheckMergeComplete", hostname, port, filename, reply)
		if err != nil {
			continue
		}
		if *reply {
			break
		}
	}
	return time.Since(startTime)
}

func main() {
	if len(os.Args) < 5 {
		log.Fatal("Usage: ./plotb <number of concurrent append> <num of trials> <append size> <delay>")
	}
	numAppend, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Can't interpret %s", os.Args[1])
	}
	numTrials, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatalf("Can't interpret %s", os.Args[2])
	}
	appendSize, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Can't interpret %s", os.Args[3])
	}
	delay, err := strconv.ParseFloat(os.Args[4], 64)
	if err != nil {
		log.Fatalf("Can't interpret %s", os.Args[4])
	}

	// Generate and send files to remote host 
	tempDir, err := os.MkdirTemp("", "hydfs_test_files_*")
	if err != nil {
		log.Fatalf("Fail to create temp folder: %v", err)
	}
	// Remember to remove tempDir
	defer os.RemoveAll(tempDir)

	for i := 0; i < numAppend; i++ {
		filename := fmt.Sprintf("test_file_%d.dat", i)
		filepath := filepath.Join(tempDir, filename)
		log.Printf("Generating files %d/%d: %s", (i + 1), numAppend, filepath)

		// generate random content
		file, err := os.Create(filepath)
		if err != nil {
			log.Fatalf("Failed to create local file %s: %v", filepath, err)
		}

		_, err = io.CopyN(file, rand.Reader, int64(appendSize) * 1024)
		if err != nil {
			file.Close()
			log.Fatalf("Failed to to write content to %s: %v", filepath, err)
		}
		file.Close()
		

		// scp <filename> remote_host:/tmp/<filename>
		cmd := exec.Command("scp", filepath, fmt.Sprintf("%s:/tmp/%s", utils.HOSTS[i], filename))
		err = cmd.Run()
		if err != nil {
			log.Fatalf("Failed to send file to %s: %s", utils.HOSTS[i], err.Error())
		}
	}


	fileSize := 256 * 1024 // 256 KiB (initial file size)
	hostname := "localhost"
	port := 8788

	expId := RandStringRunes(5) // Unique id to prevent duplicate create

	log.Printf("Running Merge performance test for %d concurrent append", numAppend)
	for i := 0; i < numTrials; i++ {
		log.Printf("Trial [%d/%d]", i + 1, numTrials)

		filename := fmt.Sprintf("multi-append-test-%s-%d", expId, i)
		err := GenerateAndUpload(hostname, port, filename, int64(fileSize))
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Printf("Test file %s of size 256KiB generated. Wait for 3s", filename)
		time.Sleep(3 * time.Second)

		log.Printf("Run multi-append with %d concurrent appends", numAppend)

		wg := new(sync.WaitGroup)
		results := make([]string, numAppend)
		for j := 0; j < numAppend; j++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				remotePath := fmt.Sprintf("/tmp/test_file_%d.dat", idx)
				args := Args{
					Command: "append",
					Filename: filename,
					FileSource: remotePath,
				}
				result := new(string)
				appendHost := utils.HOSTS[j]
				appendPort := 8788
				err := CallWithTimeout("Server.CLI", appendHost, appendPort, args, result)
				if err != nil {
					log.Fatalf("%s", err.Error())
				} 
				log.Print(*result)
				results[idx] = fmt.Sprintf("Host %s: %s", appendHost, *result)
			}(j)
		}
		wg.Wait()
		time.Sleep(time.Duration(delay) * time.Second)

		log.Printf("%d Concurrent append done. Measuring merge performance", numAppend)
		fmt.Printf("%v\n", measureMerge(filename, hostname, port))
		time.Sleep(time.Second)
	}
}