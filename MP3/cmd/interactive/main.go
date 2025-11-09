package main

import (
	"bufio"
	"cs425/mp3/internal/files"
	"cs425/mp3/internal/member"
	"cs425/mp3/internal/menu"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
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
	reply any) error {

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
	err = <-callChan
	if err != nil {
		return fmt.Errorf("rpc call to server %s:%d failed: %s", hostname, port, err.Error())
	}
	return nil
}

func readFileName() string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Please enter local filename: ")
	filename, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err.Error())
	}
	filename = strings.TrimSuffix(filename, "\n")
	filename = strings.TrimSpace(filename)
	filename, err = filepath.Abs(filename)
	if err != nil {
		log.Fatalf("Can't resolve file path: %s", err.Error())
	}
	return filename
}

func displayMember(hostname string, port int) (string, member.Info, error) {
	infoMap := new(map[uint64]member.Info)
	err := CallWithTimeout("Server.Member", hostname, port, 0, infoMap)
	if err != nil {
		log.Fatal(err.Error())
	}

	table, sortedId := member.CreateTable(*infoMap)
	options := []string{"exit", "meta", "block"}
	menuMember := menu.NewMenu("Members: ", table, sortedId, options, true)
	opt, id, err := menuMember.Display()
	return opt, (*infoMap)[id], err
}

func displayFiles(hostname string, port int) (string, files.Meta, error) {
	metaMap := new(map[uint64]files.Meta)
	err := CallWithTimeout("Server.Files", hostname, port, 0, metaMap)
	if err != nil {
		log.Fatal(err.Error())
	}
	table, sortedId := files.CreateTable(*metaMap)
	options := []string{"exit", "download", "detail"}
	menuMember := menu.NewMenu("Files: ", table, sortedId, options, true)
	opt, id, err := menuMember.Display()
	return opt, (*metaMap)[id], err
}

func displayBlocks(meta files.Meta) (string, files.BlockInfo, error) {
	blockMap := make(map[uint64]files.BlockInfo)
	for i := 0; i < meta.FileBlocks; i++ {
		blockInfo, _ := meta.GetBlock(i)
		blockMap[blockInfo.Id] = blockInfo
	}
	table, sortedId := files.CreateBlockTable(blockMap)
	options := []string{"exit", "detail"}
	menuBlock := menu.NewMenu(fmt.Sprintf("Blocks of %s", meta.FileName), table, sortedId, options, true)
	opt, id, err := menuBlock.Display()
	return opt, blockMap[id], err
}

func displayReplicas(hostname string, port int, blockInfo files.BlockInfo) (string, member.Info, error) {
	replicas := new([]member.Info)
	err := CallWithTimeout("Server.GetReplicas", hostname, port, blockInfo.Id, replicas)
	if err != nil {
		log.Fatal(err.Error())
	}
	repMap := make(map[uint64]member.Info)
	for _, replica := range *replicas {
		reply := new(files.BlockInfo)
		err := CallWithTimeout("FileManager.ReadBlockInfo", hostname, port, blockInfo.Id, reply)
		if err == nil {
			repMap[replica.Id] = replica 
		}
	}
	table, sortedId := member.CreateTable(repMap)
	prompt := fmt.Sprintf("Replicas of %s block %d:", blockInfo.FileName, blockInfo.BlockNumber)
	options := []string{"exit", "download"}
	menuRep := menu.NewMenu(prompt, table, sortedId, options, true)
	opt, id, err := menuRep.Display()
	return opt, repMap[id], err
}

func displayAllMeta(hostname string, port int) {
	metaMap := new(map[uint64]files.Meta)
	err := CallWithTimeout("FileManager.GetAllMeta", hostname, port, 0, metaMap)
	if err != nil {
		log.Fatal(err.Error())
	}
	table, sortedId := files.CreateTable(*metaMap)
	options := []string{"exit"}
	menuMember := menu.NewMenu("All Meta", table, sortedId, options, true)
	menuMember.Display()
}

func displayAllBlock(hostname string, port int) (string, files.BlockInfo, error) {
	blockMap := new(map[uint64]files.BlockInfo)
	table, sortedId := files.CreateBlockTable(*blockMap)
	options := []string{"exit", "download"}
	menuBlock := menu.NewMenu("All blocks", table, sortedId, options, true)
	opt, id, err := menuBlock.Display()
	return opt, (*blockMap)[id], err
}


func main() {
	// handle syscall SIGTERM
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	go func() {
		<-c
		log.Printf("[Main] SIGTERM, Exiting...")
		os.Exit(0)
	}()

	port := 8788
	hostname := "localhost"


	for {
		options := []string{"members", "files", "exit"}
		menuOpt := menu.NewMenu("HyDFS Viewer (press enter to select)", "", []uint64{}, options, false)
		opt, _, err := menuOpt.Display()
		if err != nil || opt == "exit" {
			break
		}
		if opt == "members" {
			opt, replica, err := displayMember(hostname, port)
			if err == nil && opt != "exit" {
				if opt == "meta" { // display meta
					displayAllMeta(replica.Hostname, replica.Port + 1)
				} else {
					for {
						opt, blockInfo, err := displayAllBlock(replica.Hostname, replica.Port + 1)
						if err != nil || opt == "exit" {
							break
						}
						filename := readFileName()
						blockPack := new(files.BlockPackage)
						err = CallWithTimeout("FileManager.ReadBlock", replica.Hostname, replica.Port + 1, blockInfo.Id, blockPack)
						if err != nil {
							log.Fatal(err.Error())
						}	
						// write block to local file
						err = os.WriteFile(filename, blockPack.Data, 0644)
						if err != nil {
							log.Fatalf("Failed to downbload %s block %d: %s", blockInfo.FileName, blockInfo.BlockNumber, err.Error())
						} else {
							log.Printf("Block %d of %s download to %s successfully!", blockInfo.BlockNumber, blockInfo.FileName, filename)
						}
						return
					}
				}
			}
		} else {
			opt, meta, err := displayFiles(hostname, port)
			if err == nil && opt != "exit" {
				if opt == "download" {
					filename := readFileName()
					args := Args{
						Command: "get",
						Filename: meta.FileName,
						FileSource: filename,
					}
					reply := new(string)
					err = CallWithTimeout("Server.CLI", hostname, port, args, reply)
					if err != nil {
						log.Fatal(err.Error())
					} else {
						log.Print(*reply)
					}
					break
				} else {
					// Detail
					for {
						opt, blockInfo, err := displayBlocks(meta)
						if err != nil || opt == "exit" {
							break
						} 
						opt, replica, _ := displayReplicas(hostname, port, blockInfo)
						if err == nil && opt == "download" {
							filename := readFileName()
							blockPack := new(files.BlockPackage)
							err := CallWithTimeout("FileManager.ReadBlock", replica.Hostname, replica.Port + 1, blockInfo.Id, blockPack)
							if err != nil {
								log.Fatal(err.Error())
							}
							// write block to local file
							err = os.WriteFile(filename, blockPack.Data, 0644)
							if err != nil {
								log.Fatalf("Failed to downbload %s block %d: %s", blockInfo.FileName, blockInfo.BlockNumber, err.Error())
							} else {
								log.Printf("Block %d of %s download to %s successfully!", blockInfo.BlockNumber, blockInfo.FileName, filename)
							}
							return
						}
					}
				}
			}
		}
	}
}