package main

import (
	"cs425/mp4/internal/leader"
	"cs425/mp4/internal/member"
	"cs425/mp4/internal/utils"
	"log"
)


func main() {
	reply := new(map[uint64]member.Info)
	err := utils.RemoteCall("Leader.GetMember", "localhost", leader.RAINSTORM_LEADER_PORT_RPC, false, reply)
	if err != nil {
		log.Fatalf("Fail to get member info: %v", err)
	}
	table, _ := member.CreateTable(*reply)
	log.Printf("\n%s", table)
}
