package utils


import (
	"os"
	"fmt"
	"net"
	"bytes"
	"errors"
	"encoding/gob"
	"cs425/mp2/internal/member"
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
	Ping MessageType = iota
	Pong
	Gossip
	 
)

// Message data type for transmission
type Message struct {
	Type MessageType
	Info map[int64]member.Info
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

func SendInfo(message Message, hostname string, port int) error {
	address := fmt.Sprintf("%s:%d", hostname, port);
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return errors.New(fmt.Sprintf("Error resolving UDP address: %s", err.Error()))
	}	
	//  establist a udp connection
	conn, err := net.DialUDP("udp", nil, udpAddress)
	if err != nil {
		return errors.New(fmt.Sprintf("Error creating udp connection: %s", err.Error()))
	}
	defer conn.Close()

	// serialize 
	serializedMessage, err := Serialize(message)
	if err != nil {
		return errors.New(fmt.Sprintf("Error serializing message: %s", err.Error()))
	}

	// send the message
	_, err = conn.Write(serializedMessage)
	if err != nil {
		return errors.New(fmt.Sprintf("Error sending data: %s", err.Error()))
	}
	return nil
}