package hydfs

import (
	"bytes"
	"cs425/mp4/internal/utils"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	HYDFS_PORT = 8788
	BUFFER_SIZE = 4096
	HYDFS_HOST = "localhost"
	APPEND_INTERVAL   = 1 * time.Second 
)

// HyDFS CLI Argument
type Args struct {
	Command    string
	Filename   string
	FileSource string
	VMAddress  string
}

type bufferState struct {
	// append when time.Since() >= 1s and buffer size 
	buffer bytes.Buffer
	lastFlush time.Time
	mu     sync.RWMutex
}

type HYDFS struct {
	mu         sync.RWMutex
	buffers    map[string]*bufferState
	tempDir    string
}

func NewClient() (*HYDFS, error) {
	tempDir, err := os.MkdirTemp("", "rainstorm-hydfs")
	if err != nil {
		return nil, err
	}

	hydfs := &HYDFS{
		buffers: make(map[string]*bufferState),
		tempDir: tempDir,
	}

	// merge
	hydfs.merge()
	return hydfs, nil
}


func (h *HYDFS) flush(filename string, buf []byte) error {
	if len(buf) == 0 {
		return nil 
	}

	// write to tempfile
	tempFilePath := filepath.Join(h.tempDir, fmt.Sprintf("rainstorm-append-%s-%s", filename, time.Now().Format(time.RFC3339)))
	err := os.WriteFile(tempFilePath, buf, 0644)
	if err != nil {
		return fmt.Errorf("error writing to temp file %s: %v", tempFilePath, err)
	}

	// call server
	args := Args{
		Command: "append",
		Filename: filename,
		FileSource: tempFilePath,
	}
	result := new(string)
	err = utils.RemoteCall("Server.CLI", HYDFS_HOST, HYDFS_PORT, args, result)
	if err != nil {
		return err
	}
	log.Printf("[HYDFS] Flush %d bytes of buffer of %s: %s", len(buf), filename, *result)
	return nil
}

// Merge 
func (h *HYDFS) merge() {
	reply := new(bool)
	utils.RemoteCall("DistributedFiles.Merge", HYDFS_HOST, HYDFS_PORT, 0, reply)
}


// CreateEmpty initializes a file on HyDFS
func (h *HYDFS) CreateEmpty(filename string) error {
	reply := new(bool) // RPC call to create empty file
	return utils.RemoteCall("DistributedFiles.CreateEmpty", HYDFS_HOST, HYDFS_PORT, filename, reply)
}


// Append writes string s to the internal buffer.
func (h *HYDFS) Append(filename string, s string) error {
	// check buffer
	h.mu.Lock()
	buf, ok := h.buffers[filename]
	if !ok {
		buf = &bufferState{
			lastFlush: time.Now(),
		}
		h.buffers[filename] = buf
	}
	h.mu.Unlock()

	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.buffer.WriteString(s)
	if time.Since(buf.lastFlush) >= APPEND_INTERVAL && buf.buffer.Len() >= BUFFER_SIZE {
		data := buf.buffer.Bytes()
		err := h.flush(filename, data)
		if err != nil {
			return err
		}
		buf.buffer.Reset()
		buf.lastFlush = time.Now()
	} 
	return nil
}


func (h *HYDFS) Get(filename string) ([]byte, error) {
	tempFilePath := filepath.Join(h.tempDir, fmt.Sprintf("rainstorm-download-%s-%s", filename, time.Now().Format(time.RFC3339)))

	args := Args{
		Command: "get",
		Filename: filename,
		FileSource: tempFilePath,
	}
	result := new(string)
	err := utils.RemoteCall("Server.CLI", HYDFS_HOST, HYDFS_PORT, args, result)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(tempFilePath)
	if err != nil {
		return nil, fmt.Errorf("fail to read file %s: %v", tempFilePath, err)
	}
	
	return data, nil
}

func (h *HYDFS) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for filename, state := range h.buffers {
		data := state.buffer.Bytes()
		err := h.flush(filename, data)
		if err != nil {
			log.Printf("[HYDFS] Error flushing %s when closing: %v", filename, err)
		}
	}

	os.RemoveAll(h.tempDir)
}
