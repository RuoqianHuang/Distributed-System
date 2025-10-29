package utils

import (
	"bytes"
	"cs425/mp3/internal/member"
	"encoding/gob"
	"fmt"
	"net"
	"os"
)

// server hostnames
var HOSTS = []string{
	"fa25-cs425-b601.cs.illinois.edu",
	"fa25-cs425-b602.cs.illinois.edu",
	"fa25-cs425-b603.cs.illinois.edu",
	"fa25-cs425-b604.cs.illinois.edu",
	"fa25-cs425-b605.cs.illinois.edu",
	"fa25-cs425-b606.cs.illinois.edu",
	"fa25-cs425-b607.cs.illinois.edu",
	"fa25-cs425-b608.cs.illinois.edu",
	"fa25-cs425-b609.cs.illinois.edu",
	"fa25-cs425-b610.cs.illinois.edu",
}

const DEFAULT_PORT int = 8787

type Leader struct {
	Hostname string
	Port     int
}

var Leaders = []Leader{
	{Hostname: "fa25-cs425-b601.cs.illinois.edu", Port: DEFAULT_PORT},
	{Hostname: "fa25-cs425-b602.cs.illinois.edu", Port: DEFAULT_PORT},
	{Hostname: "fa25-cs425-b603.cs.illinois.edu", Port: DEFAULT_PORT},
}

func IsLeader(hostname string, port int) int {
	for i := 0; i < 3; i++ {
		if Leaders[i].Hostname == hostname && Leaders[i].Port == port {
			return i
		}
	}
	return -1
}

func GetHostName() (string, error) {
	name, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return name, nil
}

// Define message transmission tools and datatypes
type MessageType int

const (
	Gossip MessageType = iota   // gossip message
	Probe                       // message for joining
	ProbeAck                    // ack message of joining
	Leave                       // message for voluntary leave
)

// Message data type for transmission
type Message struct {
	Type          MessageType            // message type
	SenderInfo    member.Info            // sender's info (counter and timestamp here are not used!!!)
	InfoMap       map[uint64]member.Info // membership Info map
}

func Serialize(obj Message) ([]byte, error) {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func Deserialize(data []byte) (Message, error) {
	buffer := bytes.Buffer{}
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)

	result := Message{}
	err := decoder.Decode(&result)
	if err != nil {
		return Message{}, err
	}
	return result, nil
}

func SendMessage(message Message, hostname string, port int) (int64, error) {
	address := fmt.Sprintf("%s:%d", hostname, port)
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return 0, fmt.Errorf("error resolving UDP address: %s", err.Error())
	}
	//  establist a udp connection
	conn, err := net.DialUDP("udp", nil, udpAddress)
	if err != nil {
		return 0, fmt.Errorf("error creating udp connection: %s", err.Error())
	}
	defer conn.Close()

	// serialize
	serializedMessage, err := Serialize(message)
	if err != nil {
		return 0, fmt.Errorf("error serializing message: %s", err.Error())
	}
	numOfBytes := len(serializedMessage)
	// send the message
	_, err = conn.Write(serializedMessage)
	if err != nil {
		return 0, fmt.Errorf("error sending data: %s", err.Error())
	}
	return int64(numOfBytes), nil
}
