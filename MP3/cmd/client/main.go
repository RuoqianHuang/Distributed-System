package main

import (
	"cs425/mp3/internal/files"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

type Args struct {
	Command    string
	Filename   string
	FileSource string
	VMAddress  string
}

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

func main() {
	if len(os.Args) < 2 {
		log.Println("Usage: ./client <Query> -p <Port>")
		log.Println("Example: ./client member files")
		log.Println("Available commands: member, status, files, create, append, get, ls, getfromreplica, multiappend")
		log.Println("multiappend: multiappend HyDFSfilename VM1 localfilename1 VM2 localfilename2 ...")
	}
	port := 8788
	hostname := "localhost"

	var otherArgs []string
	for i := 1; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "-p":
			if (i + 1) >= len(os.Args) {
				log.Fatal("Error: -p port requires a port number.")
			}
			num, err := strconv.Atoi(os.Args[i+1])
			port = num
			if err != nil {
				log.Fatal("Invalid port number!")
			}
			i++
		default:
			otherArgs = append(otherArgs, os.Args[i])
		}
	}

	if len(otherArgs) < 1 {
		log.Fatal("Please specify query")
	}
	args := Args{
		Command: "status",
	}
	args.Command = otherArgs[0]

	if args.Command == "create" || args.Command == "get" || args.Command == "append" {
		if len(otherArgs) < 3 {
			log.Fatal("Pleas specify file name and file source to create file")
		}
		args.Filename = otherArgs[1]
		FileSource, err := filepath.Abs(otherArgs[2])
		if err != nil {
			log.Fatalf("Can't resolve file path: %s: %s", otherArgs[2], err.Error())
		}
		args.FileSource = FileSource
	} 
	if args.Command == "ls" || args.Command == "merge" {
		if len(otherArgs) < 2 {
			log.Fatal("Please specify file name for ls command")
		}
		args.Filename = otherArgs[1]
	}
	if args.Command == "getfromreplica" {
		if len(otherArgs) < 5 {
			log.Fatal("Usage: getfromreplica VMaddress HyDFSfilename blockNumber localfilename")
		}
		VMAddress := otherArgs[1]
		filename := otherArgs[2]
		blockNumber, err := strconv.Atoi(otherArgs[3])
		if err != nil {
			log.Fatalf("Failed to parse block number: %s", otherArgs[3])
		}
		localFile, err := filepath.Abs(otherArgs[4])
		if err != nil {
			log.Fatalf("Can't resolve file path: %s: %s", otherArgs[4], err.Error())
		}
		fakeMeta := files.CreateMeta(filename, uint64(files.BLOCK_SIZE * (blockNumber + 1)))
		blockInfo, _ := fakeMeta.GetBlock(blockNumber)

		reply := new(files.BlockPackage)
		err = CallWithTimeout("FileManager.ReadBlock", VMAddress, port, blockInfo.Id, reply)
		if err != nil {
			log.Fatalf("%s", err.Error())
		}
		if reply.BlockInfo.Counter == 0 {
			log.Fatalf("Can't get file block from %s", VMAddress)
		}

		err = os.WriteFile(localFile, reply.Data, 0644)
		if err != nil {
			log.Fatalf("Failed to write file to %s: %s", localFile, err.Error())
		}
		return
	}

	if args.Command == "multiappend" {
		if len(otherArgs) < 4 || (len(otherArgs)-2)%2 != 0 {
			log.Fatal("Usage: multiappend HyDFSfilename VM1 localfilename1 VM2 localfilename2 ...")
		}
		args.Filename = otherArgs[1]
		
		type VMPair struct {
			VM        string
			LocalFile string
		}
		var pairs []VMPair
		for i := 2; i < len(otherArgs); i += 2 {
			if i+1 >= len(otherArgs) {
				log.Fatal("Usage: multiappend HyDFSfilename VM1 localfilename1 VM2 localfilename2 ...")
			}
			vmAddress := otherArgs[i]
			localFile := otherArgs[i+1]
			
			pairs = append(pairs, VMPair{
				VM:        vmAddress,
				LocalFile: localFile,
			})
		}
		
		// Launch simultaneous appends from all VMs
		var wg sync.WaitGroup
		results := make([]string, len(pairs))
		for i, pair := range pairs {
			wg.Add(1)
			go func(idx int, vm, localFile string) {
				defer wg.Done()
				vmArgs := Args{
					Command:    "append",
					Filename:   args.Filename,
					FileSource: localFile,
				}
				result := new(string)

				vmPort := 8788 // Default RPC port
				
				CallWithTimeout("Server.CLI", vm, vmPort, vmArgs, result)
				results[idx] = fmt.Sprintf("VM %s: %s", vm, *result)
			}(i, pair.VM, pair.LocalFile)
		}
		wg.Wait()
		
		log.Printf("\nMultiappend results for %s:\n", args.Filename)
		for _, result := range results {
			log.Printf("%s\n", result)
		}
		return
	}
	if args.Command == "liststore" {
		if len(otherArgs) < 2 {
			log.Fatal("Usage: liststore VMaddress")
		}
		VMAddress := otherArgs[1]
		metaMap := new(map[uint64]files.Meta)
		log.Printf("Fetching metadata from %s", VMAddress)
		err := CallWithTimeout("Server.Files", VMAddress, 8788, 0, metaMap)
		if err != nil {
			log.Fatalf("Failed to get metadata from %s: %s", VMAddress, err.Error())
		}
		table, _ := files.CreateTable(*metaMap)
		log.Printf("Metadata at %s:\n%s", VMAddress, table)

		blockMap := new(map[uint64]files.BlockInfo)
		log.Printf("Fetching blocks from %s", VMAddress)
		err = CallWithTimeout("Server.Blocks", VMAddress, 8788, 0, blockMap)
		if err != nil {
			log.Fatalf("Failed to get blocks from %s: %s", VMAddress, err.Error())
		}
		table, _ = files.CreateBlockTable(*blockMap)
		log.Printf("Blocks at %s:\n%s", VMAddress, table)
		return
	}

	result := new(string)
	err := CallWithTimeout("Server.CLI", hostname, port, args, result)
	if err != nil {
		log.Fatalf("%s", err.Error())
	}

	log.Printf("\n%s", *result)

}
