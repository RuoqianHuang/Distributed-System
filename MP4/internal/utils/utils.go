package utils

import (
	"bytes"
	"cs425/mp4/internal/member"
	"encoding/gob"
	"math/rand"
	"net/rpc"
	"time"
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

// Character for random string generation
var LETTERS = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	rng := rand.NewSource(time.Now().UnixNano())
	arr := make([]rune, n)
	for i := range arr {
		arr[i] = LETTERS[rng.Int63() % int64(len(LETTERS))]
	}
	return string(arr)
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

const (
	CONNECTION_TIMEOUT 			= 2 * time.Second
	CALL_TIMEOUT       			= 200 * time.Second
)

func RemoteCall(
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