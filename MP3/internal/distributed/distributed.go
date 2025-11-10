package distributed

import (
	"bytes"
	"cs425/mp3/internal/files"
	"cs425/mp3/internal/flow"
	"cs425/mp3/internal/member"
	"cs425/mp3/internal/queue"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	CONNECTION_TIMEOUT = 1 * time.Second
	CALL_TIMEOUT       = 1 * time.Second
)

type BufferedBlock struct {
	BlockInfo    files.BlockInfo
	TempFileName string
}

type State int

const (
	Alive State = iota
	Stop
)

type DistributedFiles struct {
	Tround   time.Duration
	Hostname string
	Port     int

	FileManager *files.FileManager
	Membership  *member.Membership

	NumOfReplicas        int
	NumOfMetaWorkers     int
	NumOfBlockWorkers    int
	NumOfBufferedWorkers int
	NumOfTries           int // Number of tries uploading blocks

	// Job to merge blocks
	BlockJobs       *queue.Queue    // Merge job queue for blocks
	BlockJobMap     map[uint64]bool // Map to check if it's in queue
	lockBlockJobMap sync.RWMutex    // lock for BlockJobMap

	// Job to merge meta
	MetaJobs       *queue.Queue    // Merge job queue for metadata
	MetaJobMap     map[uint64]bool // Map to check if it's in queue
	lockMetaJobMap sync.RWMutex    // lock for MetaJobMap

	// Job to commit buffered blocks
	BufferedBlocks     *queue.Queue             // Buffered block queue
	lockBufferedBlocks sync.RWMutex             // Lock for buffered blocks
	BufferedBlockMap   map[uint64]BufferedBlock // Buffered blocks

	// Lock for atomicity
	LockedMeta     map[uint64]bool // Locked metadata (to ensure atomic operation)
	locklockedMeta sync.RWMutex    // Lock for locked metadata (to ensure atomic operation)

	// Flow counter and State
	Flow  *flow.FlowCounter
	State State
}

func PayloadSize(payload any) int64 {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(payload)
	if err != nil {
		return 0
	}
	return int64(buffer.Len())
}

func (d *DistributedFiles) RemoteCall(
	funcName string,
	hostname string,
	port int,
	args any,
	reply any,
) error {
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
	// record out flow
	d.Flow.Add(PayloadSize(args) + PayloadSize(reply))
	return nil
}

// Functions to start workers and register rpc
func (d *DistributedFiles) RegisterRPC() {
	rpc.Register(d)
}

func (d *DistributedFiles) Start() {
	waitGroup := new(sync.WaitGroup)

	waitGroup.Add(d.NumOfMetaWorkers)
	for i := 0; i < d.NumOfMetaWorkers; i++ {
		go d.workerLoopMeta(waitGroup)
	}

	waitGroup.Add(d.NumOfBlockWorkers)
	for i := 0; i < d.NumOfBlockWorkers; i++ {
		go d.workerLoopBlock(waitGroup)
	}

	waitGroup.Add(d.NumOfBufferedWorkers)
	for i := 0; i < d.NumOfBufferedWorkers; i++ {
		go d.workerLoopBuffered(waitGroup)
	}

	waitGroup.Add(1)
	go d.jobCreator(waitGroup)

	waitGroup.Wait()
}

func (d *DistributedFiles) jobCreator(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	// Create ticker
	ticker := time.NewTicker(d.Tround)
	defer ticker.Stop()

	for range ticker.C {
		// Create new merge job
		result := new(bool)
		d.Merge(0, result)
	}
}

// Functions for updating meta
func (d *DistributedFiles) sendMeta(
	hostname string,
	port int,
	meta files.Meta,
) int {
	// Call for the file version
	result := new(files.Meta)
	err := d.RemoteCall("FileManager.ReadMeta", hostname, port, meta.Id, result)
	if err != nil {
		log.Printf("[DF] Failed to get remote meta counter of %s: %s", meta.FileName, err.Error())
		return 0
	}
	// Check file version
	if result.Counter < meta.Counter {
		// Need update
		_reply := new(uint64)
		err = d.RemoteCall("FileManager.WriteMeta", hostname, port, meta, _reply)
		if err != nil {
			log.Printf("[DF] Failed to write meta of %s: %s", meta.FileName, err.Error())
			return 0
		}
	}
	return 1
}

func (d *DistributedFiles) sendBlock(
	hostname string,
	port int,
	block *files.Block,
) int {
	blockInfo := block.GetInfo()

	// Call for the file version
	result := new(files.BlockInfo)
	err := d.RemoteCall("FileManager.ReadBlockInfo", hostname, port, blockInfo.Id, result)
	if err != nil {
		log.Printf("[DF] Failed to get remote block info of %s-%d: %s", blockInfo.FileName, blockInfo.BlockNumber, err.Error())
		return 0
	}

	// Check file version
	if result.Counter < block.GetCounter() {
		// Need update
		blockPack, err := block.Read()
		if err != nil {
			log.Printf("[DF] Failed to read local block %s-%d: %s", blockInfo.FileName, blockInfo.BlockNumber, err.Error())
			return 0
		}
		reply := new(uint64)
		err = d.RemoteCall("FileManager.WriteBlock", hostname, port, blockPack, reply)
		if err != nil {
			log.Printf("[DF] Failed to write block of %s-%d: %s", blockInfo.FileName, blockInfo.BlockNumber, err.Error())
			return 0
		}
	}
	return 1
}

func (d *DistributedFiles) workerLoopMeta(waitGroup *sync.WaitGroup) {
	// Background job that distribute and balance the metadata
	defer waitGroup.Done()
	for {
		if d.State == Stop {
			break
		}
		time.Sleep(50 * time.Millisecond) // Slow down meta worker so that CPU is not fully occupied

		// Get job from queue
		val := d.MetaJobs.Pop() // Blocking call
		id := val.(uint64)

		// remove it from MetaMap
		d.lockMetaJobMap.Lock()
		delete(d.MetaJobMap, id)
		d.lockMetaJobMap.Unlock()

		// Get file meta from local file manager
		result := new(files.Meta)
		err := d.FileManager.ReadMeta(id, result)
		if err != nil {
			continue
		}

		// Find replicas
		replicas, err := d.Membership.GetReplicas(id, d.NumOfReplicas)
		if err != nil {
			log.Printf("[DF] Failed to get replicas for %d, %s: %s", id, result.FileName, err.Error())
			continue
		}

		// Try to distribute files with timeout
		cnt := 0
		needed := false
		for _, replica := range replicas {
			if replica.Hostname == d.Hostname && replica.Port == d.Port {
				needed = true
				continue
			}
			// Send this file to the replica, (using udp port + 1 as rpc port)
			cnt += d.sendMeta(replica.Hostname, replica.Port+1, *result)
		}
		if !needed && cnt >= (d.NumOfReplicas/2+1) {
			// We can delete it now
			d.FileManager.RemoveMeta(id)
		}
	}
}

func (d *DistributedFiles) workerLoopBlock(waitGroup *sync.WaitGroup) {
	// Background job that distribute and balance blocks
	defer waitGroup.Done()
	for {
		if d.State == Stop {
			break
		}
		time.Sleep(50 * time.Millisecond) // Slow down block worker so that CPU is not fully occupied

		// Get job from queue
		val := d.BlockJobs.Pop() // Blocking call
		id := val.(uint64)

		// remove it from BlockMap
		d.lockBlockJobMap.Lock()
		delete(d.BlockJobMap, id)
		d.lockBlockJobMap.Unlock()

		// Get file meta from local file manager
		block, err := d.FileManager.GetBlock(id)
		if err != nil {
			continue
		}

		// Find replicas
		replicas, err := d.Membership.GetReplicas(id, d.NumOfReplicas)
		if err != nil {
			log.Printf("[DF] Failed to get replicas for %d, %s: %s", id, block.GetInfo().FileName, err.Error())
			continue
		}

		// Try to distribute files with timeout
		cnt := 0
		needed := false
		for _, replica := range replicas {
			if replica.Hostname == d.Hostname && replica.Port == d.Port {
				needed = true
				continue
			}
			// Send this file to the replica, (using udp port + 1 as rpc port)
			cnt += d.sendBlock(replica.Hostname, replica.Port+1, block)
		}
		if !needed && cnt >= (d.NumOfReplicas/2+1) {
			// We can delete it now
			d.FileManager.RemoveBlock(id)
		}
	}
}

// Quorum read/write
func (d *DistributedFiles) getRemoteMeta(id uint64, quorum int) (files.Meta, error) {
	// This is a blocking call for retrieving remote meta
	replicas, err := d.Membership.GetReplicas(id, d.NumOfReplicas)
	if err != nil {
		return files.Meta{}, err
	}

	replyCounter := 0
	maxMeta := files.Meta{
		Counter: 0,
	}

	for _, replica := range replicas {
		if replica.Hostname == d.Hostname && replica.Port == d.Port {
			// Get Meta from local
			result := new(files.Meta)
			err := d.FileManager.ReadMeta(id, result)
			if err != nil {
				continue
			}
			replyCounter++
			if result.Counter > maxMeta.Counter {
				maxMeta = *result
			}
		} else {
			result := new(files.Meta)
			err = d.RemoteCall("FileManager.ReadMeta", replica.Hostname, replica.Port+1, id, result) // using udp port + 1 as rpc port
			if err != nil {
				continue
			}
			replyCounter++
			if result.Counter > maxMeta.Counter {
				maxMeta = *result
			}
		}
		if replyCounter >= quorum {
			break
		}
	}
	if replyCounter < quorum {
		return maxMeta, fmt.Errorf("no enough meta reply, only %d replied, expeceted at least %d", replyCounter, quorum)
	}
	return maxMeta, nil
}

func (d *DistributedFiles) getRemoteBlock(id uint64, quorum int) (files.BlockPackage, error) {
	// This is a blocking call for retrieving remote meta
	replicas, err := d.Membership.GetReplicas(id, d.NumOfReplicas)
	if err != nil {
		return files.BlockPackage{}, err
	}

	replyCounter := 0
	maxBlock := files.BlockPackage{
		BlockInfo: files.BlockInfo{
			Counter: 0,
		},
	}

	for _, replica := range replicas {
		if replica.Hostname == d.Hostname && replica.Port == d.Port {
			// Get Meta from local
			result := new(files.BlockPackage)
			err := d.FileManager.ReadBlock(id, result)
			if err != nil {
				continue
			}
			replyCounter++
			if result.BlockInfo.Counter > maxBlock.BlockInfo.Counter {
				maxBlock = *result
			}
		} else {
			result := new(files.BlockPackage)
			err = d.RemoteCall("FileManager.ReadBlock", replica.Hostname, replica.Port+1, id, result) // using udp port + 1 as rpc port
			if err != nil {
				continue
			}
			replyCounter++
			if result.BlockInfo.Counter > maxBlock.BlockInfo.Counter {
				maxBlock = *result
			}
		}
		if replyCounter >= quorum {
			break
		}
	}
	if replyCounter < quorum {
		return maxBlock, fmt.Errorf("no enough block reply, only %d buffered, expeceted at least %d", replyCounter, quorum)
	}
	return maxBlock, nil
}

// Functions for buffered blocks
func (d *DistributedFiles) workerLoopBuffered(waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()
	for {
		if d.State == Stop {
			break
		}
		time.Sleep(50 * time.Millisecond) // Slow down buffered block worker so that CPU is not fully occupied

		// Buffered block id from queue
		val := d.BufferedBlocks.Pop() // Blocking call
		id := val.(uint64)

		d.lockBufferedBlocks.RLock()
		block, ok := d.BufferedBlockMap[id]
		d.lockBufferedBlocks.RUnlock()
		if !ok {
			// Block does not exist
			continue
		}

		// Check if this block should be here
		replicas, err := d.Membership.GetReplicas(block.BlockInfo.Id, d.NumOfReplicas)
		if err != nil {
			// If failed to get replicas, try agagin later...
			d.BufferedBlocks.Push(id)
			continue
		}
		needed := false
		for _, replica := range replicas {
			if replica.Hostname == d.Hostname && replica.Port == d.Port {
				needed = true
				break
			}
		}
		if !needed {
			// This block should not be here
			d.lockBufferedBlocks.Lock()
			delete(d.BufferedBlockMap, id)
			d.lockBufferedBlocks.Unlock()
			os.Remove(block.TempFileName)
			continue
		}

		// Check if we can to remove this block
		localCounter := uint64(0)
		localblock, err := d.FileManager.GetBlock(block.BlockInfo.Id)
		if err == nil {
			localCounter = localblock.GetCounter()
		}

		if localCounter >= block.BlockInfo.Counter {
			// We don't need this buffered block anymore
			d.lockBufferedBlocks.Lock()
			delete(d.BufferedBlockMap, id)
			d.lockBufferedBlocks.Unlock()
			os.Remove(block.TempFileName)
			continue
		}

		// Check if we can commit this block
		metaId := files.GetIdFromFilename(block.BlockInfo.FileName)
		meta, err := d.getRemoteMeta(metaId, d.NumOfReplicas/2+1)
		if err != nil {
			// If failed to get remote meta, try agagin later...
			d.BufferedBlocks.Push(id)
			continue
		}
		if meta.Counter >= block.BlockInfo.Counter {
			// Commit this block
			data, err := os.ReadFile(block.TempFileName)
			if err != nil {
				// If failed to read local block data, try agagin later...
				d.BufferedBlocks.Push(id)
				continue
			}
			blockPack := files.BlockPackage{
				BlockInfo: block.BlockInfo,
				Data:      data,
			}
			result := new(uint64)
			err = d.FileManager.WriteBlock(blockPack, result)
			if err != nil {
				// If failed to commit block, try agagin later...
				d.BufferedBlocks.Push(id)
			} else {
				// Done, remove block info from map
				log.Printf("[DF] buffered block %s-%d, %d committed", block.BlockInfo.FileName, block.BlockInfo.BlockNumber, blockPack.BlockInfo.Counter)
				delete(d.BufferedBlockMap, id)
				os.Remove(block.TempFileName)
			}
		} else {
			// Wait until metadata is updated
			d.BufferedBlocks.Push(id)
		}
	}
}

func (d *DistributedFiles) ReceiveBufferedBlocks(block files.BlockPackage, _ *uint64) error {
	// Receive blocks and decide whether to commit them later
	d.lockBufferedBlocks.Lock()
	defer d.lockBufferedBlocks.Unlock()
	log.Printf("[DF] Received a buffered block %s-%d", block.BlockInfo.FileName, block.BlockInfo.BlockNumber)
	oldBlock, ok := d.BufferedBlockMap[block.BlockInfo.Id]
	if ok {
		// Only overwrite buffered block with the same block counter
		if oldBlock.BlockInfo.Counter != block.BlockInfo.Counter {
			return fmt.Errorf("existing buffered block not committed")
		}
		// Update block info
		oldBlock.BlockInfo = block.BlockInfo
		// Write to file
		err := os.WriteFile(oldBlock.TempFileName, block.Data, 0644)
		return err
	}
	// Create new tempfile
	tempFile, err := os.CreateTemp("", "block-*.temp")
	if err != nil {
		return err
	}
	bufferedBlock := BufferedBlock{
		TempFileName: tempFile.Name(),
		BlockInfo:    block.BlockInfo,
	}
	log.Printf("[DF] Write buffered block %s-%d to local file %s", block.BlockInfo.FileName, block.BlockInfo.BlockNumber, tempFile.Name())
	// Write block data to file system and enqueue it
	tempFile.Write(block.Data)
	d.BufferedBlockMap[block.BlockInfo.Id] = bufferedBlock
	d.BufferedBlocks.Push(block.BlockInfo.Id)
	return nil
}

func (d *DistributedFiles) sendBufferedBlocks(blockPack files.BlockPackage, quorum int) error {
	replicas, err := d.Membership.GetReplicas(blockPack.BlockInfo.Id, d.NumOfReplicas)
	if err != nil {
		return err
	}
	replyCounter := 0
	for _, replica := range replicas {
		if replica.Hostname == d.Hostname && replica.Port == d.Port {
			result := new(uint64)
			err = d.ReceiveBufferedBlocks(blockPack, result)
			if err != nil {
				continue
			}
			replyCounter++
		} else {
			result := new(uint64)
			err = d.RemoteCall("DistributedFiles.ReceiveBufferedBlocks", replica.Hostname, replica.Port+1, blockPack, result)
			if err != nil {
				log.Printf("[DF] Failed to sent buffered block to %s:%d: %s", replica.Hostname, replica.Port, err.Error())
				continue
			} else {
				log.Printf("[DF] Buffered block %s-%d sent to %s:%d", blockPack.BlockInfo.FileName, blockPack.BlockInfo.BlockNumber, replica.Hostname, replica.Port)
			}
			replyCounter++
		}
		if replyCounter >= quorum {
			break
		}
	}
	if replyCounter < quorum {
		return fmt.Errorf("no enough block buffered, only %d buffered, expeceted at least %d", replyCounter, quorum)
	}
	return nil
}

// Locks
func (d *DistributedFiles) GetLock(metaId uint64, reply *bool) error {
	d.locklockedMeta.Lock()
	defer d.locklockedMeta.Unlock()
	_, ok := d.LockedMeta[metaId]
	if ok {
		*reply = false
		return fmt.Errorf("this file is currently locked")
	}
	d.LockedMeta[metaId] = true
	*reply = true
	return nil
}

func (d *DistributedFiles) ReleaseLock(metaId uint64, reply *bool) error {
	d.locklockedMeta.Lock()
	defer d.locklockedMeta.Unlock()
	_, ok := d.LockedMeta[metaId]
	if ok {
		// unlock meta
		delete(d.LockedMeta, metaId)
	}
	return nil
}

// Function for CLI calls
func (d *DistributedFiles) Create(filename string, fileSource string, quorum int) ([]member.Info, error) {
	metaId := files.GetIdFromFilename(filename)

	// Lock the meta, so that no other process can write to this file
	// Use the first replica as coordinator
	replicas, err := d.Membership.GetReplicas(metaId, d.NumOfReplicas)
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas: %s", err.Error())
	}

	// Try to accquire lock for 1s
	startTime := time.Now()
	for {
		result := new(bool)
		err = d.RemoteCall("DistributedFiles.GetLock", replicas[0].Hostname, replicas[0].Port+1, metaId, result)
		if err == nil {
			break
		}
		if time.Since(startTime) > time.Second {
			return nil, fmt.Errorf("failed to acquire lock of %s from %s:%d: %s", filename, replicas[0].Hostname, replicas[0].Port, err.Error())
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer func() {
		// Release lock
		for i := 0; i < d.NumOfTries; i++ { // Try
			result := new(bool)
			err = d.RemoteCall("DistributedFiles.ReleaseLock", replicas[0].Hostname, replicas[0].Port+1, metaId, result)
			if err == nil {
				log.Printf("[DF] lock for %s released", filename)
				break
			}
		}
	}()
	log.Printf("[DF] Create: accquired lock for file %s from %s:%d", filename, replicas[0].Hostname, replicas[0].Port)

	// Check file exist
	meta, err := d.getRemoteMeta(metaId, quorum)
	if err != nil {
		return nil, fmt.Errorf("failed to read remote meta: %s", err.Error())
	}
	if meta.Counter > 0 {
		return nil, fmt.Errorf("file already exist")
	}

	// Read block from file system
	data, err := os.ReadFile(fileSource)
	if err != nil {
		return nil, fmt.Errorf("failed to read file source: %s", err.Error())
	}
	log.Printf("[DF] Create: read data from local source file %s", fileSource)
	meta = files.CreateMeta(filename, uint64(len(data)))

	log.Printf("[DF] Cteate: start create file: %s, file size: %d bytes, num of blocks: %d", filename, len(data), meta.FileBlocks)

	// Send blocks
	tries := make([]int, meta.FileBlocks)
	success := make([]int, meta.FileBlocks)
	for t := 0; t < d.NumOfTries; t++ {
		for i := 0; i < meta.FileBlocks; i++ {
			if success[i] > 0 {
				continue
			}
			blockInfo, err := meta.GetBlock(i)
			if err != nil {
				tries[i]++
			if tries[i] >= d.NumOfTries {
				return nil, fmt.Errorf("failed to send block %d for %d times, abort", i, tries[i])
			}
			}
			l := i * files.BLOCK_SIZE
			r := min((i+1)*files.BLOCK_SIZE, int(meta.FileSize))
			blockPack := files.BlockPackage{
				BlockInfo: blockInfo,
				Data:      data[l:r],
			}
			err = d.sendBufferedBlocks(blockPack, quorum)
			if err != nil {
				tries[i]++
			if tries[i] >= d.NumOfTries {
				return nil, fmt.Errorf("failed to send block %d for %d times: %s, abort", i, tries[i], err.Error())
			}
			} else {
				log.Printf("[DF] Block %d of %s sent successfully", i, filename)
				success[i] = 1
			}
		}
	}

	// Once all blocks are buffered, we can increase the counter, then all other process will commit the buffered block
	successCount := 0
	success = make([]int, d.NumOfReplicas)
	for i := 0; i < d.NumOfTries; i++ { // Try
		for j, replica := range replicas {
			if success[j] > 0 {
				continue
			}
			success[j] = d.sendMeta(replica.Hostname, replica.Port+1, meta)
			if success[j] > 0 {
				successCount++
				log.Printf("[DF] Increase file counter at %s:%d of %s successfully", replica.Hostname, replica.Port, filename)
			}
		}
	}
	if successCount >= quorum {
		// Return list of successful replicas
		successfulReplicas := make([]member.Info, 0, successCount)
		for j, s := range success {
			if s > 0 {
				successfulReplicas = append(successfulReplicas, replicas[j])
			}
		}
		return successfulReplicas, nil
	}
	return nil, fmt.Errorf("failed reach quorum, expecting %d, but got %d", quorum, successCount)
}

func (d *DistributedFiles) Get(filename string, destPath string, quorum int) error {
	id := files.GetIdFromFilename(filename)
	meta, err := d.getRemoteMeta(id, quorum)
	if err != nil {
		return fmt.Errorf("failed to read remote meta: %s", err.Error())
	}
	if meta.Counter == 0 {
		return fmt.Errorf("%s does not exits", filename)
	}
	blocks := make([]files.BlockPackage, meta.FileBlocks)
	tries := make([]int, meta.FileBlocks)
	success := make([]int, meta.FileBlocks)
	for t := 0; t < d.NumOfTries; t++ {
		for i := 0; i < meta.FileBlocks; i++ {
			if success[i] > 0 {
				continue
			}
			blockInfo, _ := meta.GetBlock(i)
			blockPack, err := d.getRemoteBlock(blockInfo.Id, quorum)
			if err != nil {
				tries[i]++
				if tries[i] >= d.NumOfTries {
					return fmt.Errorf("failed to read block %d for %d times: %s, abort", i, tries[i], err.Error())
				}
			} else {
				success[i] = 1
				blocks[i] = blockPack
			}
		}
	}
	// Merge blocks
	data := make([]byte, 0, meta.FileSize)
	for i := 0; i < meta.FileBlocks; i++ {
		data = append(data, blocks[i].Data...)
	}
	return os.WriteFile(destPath, data, 0644)
}

func (d *DistributedFiles) Append(filename string, fileSource string, quorum int) ([]member.Info, error) {
	// Read block from file system
	data, err := os.ReadFile(fileSource)
	if err != nil {
		return nil, fmt.Errorf("failed to read file source: %s", err.Error())
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("nothing to append to %s", filename)
	}

	metaId := files.GetIdFromFilename(filename)
	// Lock the meta, so that no other process can write to this file
	// Use the first replica as coordinator
	replicas, err := d.Membership.GetReplicas(metaId, d.NumOfReplicas)
	if err != nil {
		return nil, fmt.Errorf("failed to get replicas: %s", err.Error())
	}
	// Try to accquire lock for 1s
	startTime := time.Now()
	for {
		result := new(bool)
		err = d.RemoteCall("DistributedFiles.GetLock", replicas[0].Hostname, replicas[0].Port+1, metaId, result)
		if err == nil {
			break
		}
		if time.Since(startTime) > time.Second {
			return nil, fmt.Errorf("failed to acquire lock of %s from %s:%d: %s", filename, replicas[0].Hostname, replicas[0].Port, err.Error())
		}
		time.Sleep(50 * time.Millisecond)
	}

	log.Printf("[DF] Append: accquired lock for file %s from %s:%d", filename, replicas[0].Hostname, replicas[0].Port)
	defer func() {
		// Release lock
		for i := 0; i < d.NumOfTries; i++ { // Try
			result := new(bool)
			err = d.RemoteCall("DistributedFiles.ReleaseLock", replicas[0].Hostname, replicas[0].Port+1, metaId, result)
			if err == nil {
				log.Printf("[DF] lock for %s released", filename)
				break
			}
		}
	}()

	meta, err := d.getRemoteMeta(metaId, quorum)
	if err != nil {
		return nil, fmt.Errorf("failed to read remote meta: %s", err.Error())
	}
	if meta.Counter == 0 {
		return nil, fmt.Errorf("file does not exist")
	}

	// Use the meta retrived after meta is locked.
	// This ensures we have the most up-to-date file size
	log.Printf("[DF] Append: read data from local source file %s", fileSource)
	log.Printf("[DF] Append: appending %d bytes from %s", len(data), fileSource)
	newSize := meta.FileSize + uint64(len(data))
	newMeta := files.Meta{
		Id:         meta.Id,
		FileName:   meta.FileName,
		FileSize:   newSize,
		Counter:    meta.Counter + 1,
		FileBlocks: int((newSize-1)/files.BLOCK_SIZE + 1),
	}
	log.Printf("[DF] Append: new file info filename: %s, file size: %d bytes, num of blocks: %d", filename, newSize, newMeta.FileBlocks)

	residual := int(meta.FileSize % files.BLOCK_SIZE)
	lastBlock := meta.FileBlocks - 1
	startPos := 0 // Position in data[] where new blocks start (after filling last block if needed)

	log.Printf("[DF] Append: residual=%d, lastBlock=%d, dataLen=%d", residual, lastBlock, len(data))

	if residual != 0 {
		// Calculate how much data to fill into the last block
		startPos = min(files.BLOCK_SIZE-residual, len(data))
		log.Printf("[DF] Append: filling last block %d with %d bytes (residual space: %d)", lastBlock, startPos, files.BLOCK_SIZE-residual)
		// We need to get the last block and update it
		success := false
		blockPack := files.BlockPackage{}
		lastBlockInfo, _ := newMeta.GetBlock(lastBlock)
		log.Printf("[DF] Append: reading last block %d (id=%d, expected residual=%d)", lastBlock, lastBlockInfo.Id, residual)
		
		// try to get the latest block for 1s
		startTime := time.Now()
		for {
			if time.Since(startTime) > time.Second {
				break
			}
			pack, err := d.getRemoteBlock(lastBlockInfo.Id, quorum)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			} else if pack.BlockInfo.Counter < meta.Counter {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			// Use the actual block length (might be different from residual if another append completed)
			actualResidual := len(pack.Data)
			if actualResidual != residual {
				log.Printf("[DF] Append: WARNING - last block %d has length %d, expected %d (residual). Another append may have completed.", lastBlock, actualResidual, residual)
				// Recalculate startPos based on actual residual
				startPos = min(files.BLOCK_SIZE-actualResidual, len(data))
				log.Printf("[DF] Append: adjusted startPos to %d based on actual residual %d", startPos, actualResidual)
			}
			log.Printf("[DF] Append: read last block %d: length=%d, appending %d bytes", lastBlock, actualResidual, startPos)
			blockPack = files.BlockPackage{
				BlockInfo: lastBlockInfo,
				Data:      append(pack.Data, data[:startPos]...),
			}
			log.Printf("[DF] Append: updated last block %d: new length=%d", lastBlock, len(blockPack.Data))
			success = true
			break
		}
		if !success {
			return nil, fmt.Errorf("failed to get block %d after trying for 1s", lastBlock)
		}

		success = false
		for t := 0; t < d.NumOfTries; t++ {
			err = d.sendBufferedBlocks(blockPack, quorum)
			if err != nil {
				continue
			}
			success = true
			break
		}
		if !success {
			return nil, fmt.Errorf("failed to send new block to replica after %d tries", d.NumOfTries)
		}
		log.Printf("[DF] Append: block %s-%d updated", filename, lastBlock)
	}

	if lastBlock+1 < newMeta.FileBlocks {
		// We have new blocks to update
		blocksToUpdate := newMeta.FileBlocks - lastBlock - 1
		log.Printf("[DF] Append: creating %d new blocks starting from position %d in data", blocksToUpdate, startPos)
		tries := make([]int, blocksToUpdate)
		success := make([]int, blocksToUpdate)
		for t := 0; t < d.NumOfTries; t++ {
			for i := 0; i < blocksToUpdate; i++ {
				if success[i] > 0 {
					continue
				}
				blockInfo, err := newMeta.GetBlock(lastBlock + i + 1)
				if err != nil {
					tries[i]++
					if tries[i] >= d.NumOfTries {
						return nil, fmt.Errorf("failed to send block %d for %d times: %s, abort", i+lastBlock+1, tries[i], err.Error())
					}
				}
				l := startPos + i*files.BLOCK_SIZE
				r := min(l+files.BLOCK_SIZE, len(data))
				log.Printf("[DF] Append: sending new block %d: data[%d:%d] (%d bytes)", lastBlock+i+1, l, r, r-l)
				blockPack := files.BlockPackage{
					BlockInfo: blockInfo,
					Data:      data[l:r],
				}
				err = d.sendBufferedBlocks(blockPack, quorum)
				if err != nil {
					tries[i]++
					if tries[i] >= d.NumOfTries {
						return nil, fmt.Errorf("failed to send block %d for %d times: %s, abort", i+lastBlock+1, tries[i], err.Error())
					}
				} else {
					log.Printf("[DF] Block %d of %s sent successfully", i+lastBlock+1, filename)
					success[i] = 1
				}
			}
		}
	}

	// Once all blocks are buffered, we can increase the counter, then all other process will commit the buffered block
	successCount := 0
	success := make([]int, d.NumOfReplicas)
	for i := 0; i < d.NumOfTries; i++ { // Try
		for j, replica := range replicas {
			if success[j] > 0 {
				continue
			}
			success[j] = d.sendMeta(replica.Hostname, replica.Port+1, newMeta)
			if success[j] > 0 {
				successCount++
				log.Printf("[DF] Increase file counter at %s:%d of %s successfully", replica.Hostname, replica.Port, filename)
			}
		}
	}
	if successCount >= quorum {
		// Return list of successful replicas
		successfulReplicas := make([]member.Info, 0, successCount)
		for j, s := range success {
			if s > 0 {
				successfulReplicas = append(successfulReplicas, replicas[j])
			}
		}
		return successfulReplicas, nil
	}
	return nil, fmt.Errorf("failed reach quorum, expecting %d, but got %d", quorum, successCount)
}

func (d *DistributedFiles) Merge(_ int, _ *bool) error {
	// Loop through local files and redistribute them
	metas := d.FileManager.GetMetas()
	for id := range metas {
		d.lockMetaJobMap.RLock()
		_, ok := d.MetaJobMap[id]
		d.lockMetaJobMap.RUnlock()
		if !ok {
			d.MetaJobs.Push(id) // Add local meta to merge job
			d.lockMetaJobMap.Lock()
			d.MetaJobMap[id] = true
			d.lockMetaJobMap.Unlock()
		}
	}
	blocks := d.FileManager.GetBlocks()
	for id := range blocks {
		d.lockBlockJobMap.RLock()
		_, ok := d.BlockJobMap[id]
		d.lockBlockJobMap.RUnlock()
		if !ok {
			d.BlockJobs.Push(id) // Add local block to merge job
			d.lockBlockJobMap.Lock()
			d.BlockJobMap[id] = true
			d.lockBlockJobMap.Unlock()
		}
	}
	return nil
}

func (d *DistributedFiles) MergeFile(filename string, _ *bool) error {
	// Check if the file is here
	metaId := files.GetIdFromFilename(filename)
	meta, err := d.getRemoteMeta(metaId, d.NumOfReplicas/2+1)
	if err != nil {
		return fmt.Errorf("can't read meta of %s", filename)
	}
	if meta.Counter == 0 {
		return fmt.Errorf("file does not exist")
	}


	localMeta := new(files.Meta)
	d.FileManager.ReadMeta(metaId, localMeta)
	if localMeta.Counter > 0 { // We have this meta, add to merge job
		d.lockMetaJobMap.RLock()
		_, ok := d.MetaJobMap[metaId]
		d.lockMetaJobMap.RUnlock()
		if !ok {
			d.MetaJobs.Push(metaId) // Add local meta to merge job
			d.lockMetaJobMap.Lock()
			d.MetaJobMap[metaId] = true
			d.lockMetaJobMap.Unlock()
		}
	}

	for i := 0; i < meta.FileBlocks; i++ {
		blockInfo, _ := meta.GetBlock(i)
		localInfo := new(files.BlockInfo)
		d.FileManager.ReadBlockInfo(blockInfo.Id, localInfo)
		if localInfo.Counter > 0 { // We have this block, add to merge job
			d.lockBlockJobMap.RLock()
			_, ok := d.BlockJobMap[blockInfo.Id]
			d.lockBlockJobMap.RUnlock()
			if !ok {
				d.BlockJobs.Push(blockInfo.Id) // Add local block to merge job
				d.lockBlockJobMap.Lock()
				d.BlockJobMap[blockInfo.Id] = true
				d.lockBlockJobMap.Unlock()
			}
		}
	}
	return nil
}

func (d *DistributedFiles) GetMetas(_ int, reply *map[uint64]files.Meta) error {
	metas := d.FileManager.GetMetas()
	*reply = metas
	return nil
}

func (d *DistributedFiles) CollectMeta() map[uint64]files.Meta {
	members := d.Membership.GetInfoMap()
	result := map[uint64]files.Meta{}
	dummyArg := 0
	for _, member := range members {
		reply := new(map[uint64]files.Meta)
		err := d.RemoteCall("DistributedFiles.GetMetas", member.Hostname, member.Port+1, dummyArg, reply)
		if err != nil {
			log.Printf("[DF] Failed to get metas from %s:%d: %s", member.Hostname, member.Port, err.Error())
			continue
		}
		for id, meta := range *reply {
			old, ok := result[id]
			if !ok {
				result[id] = meta
			} else {
				if old.Counter < meta.Counter {
					result[id] = meta
				}
			}
		}
	}
	return result
}

func (d *DistributedFiles) ListReplicas(filename string, quorum int) string {
	fileId := files.GetIdFromFilename(filename)

	result := fmt.Sprintf("FileName: %s, FileID: %d\n", filename, fileId)

	// Get replicas for metadata
	replicas, err := d.Membership.GetReplicas(fileId, d.NumOfReplicas)
	if err != nil {
		result += fmt.Sprintf("Failed to get replicas for metadata: %s\n", err.Error())
		return result
	}

	meta, err := d.getRemoteMeta(fileId, quorum)
	if err != nil {
		result += fmt.Sprintf("Failed to get metadata for %s: %s", filename, err.Error())
		return result
	}

	result += "Metadata:\n"
	for _, replica := range replicas {
		// Read Metadata
		reply := new(files.Meta)
		err := d.RemoteCall("FileManager.ReadMeta", replica.Hostname, replica.Port+1, fileId, reply)
		if err == nil {
			result += fmt.Sprintf("%s\n", replica.String())
		}
	}

	// Get replicas for blocks
	for i := 0; i < meta.FileBlocks; i++ {
		result += fmt.Sprintf("Block %d:\n", i)
		blockInfo, _ := meta.GetBlock(i)
		replicas, err := d.Membership.GetReplicas(blockInfo.Id, d.NumOfReplicas)
		if err != nil {
			result += fmt.Sprintf("Failed to get replicas for block %d: %s\n", i, err.Error())
			return result
		}
		for _, replica := range replicas {
			reply := new(files.BlockInfo)
			err := d.RemoteCall("FileManager.ReadBlockInfo", replica.Hostname, replica.Port+1, blockInfo.Id, reply)
			if err == nil {
				result += fmt.Sprintf("%s\n", replica.String())
			}
		}
	}

	return result
}

func (d *DistributedFiles) Stop() {
	d.State = Stop
}

// Extra function and data type for experiment
func (d *DistributedFiles) CheckMergeComplete(filename string, reply *bool) error {
	fileId := files.GetIdFromFilename(filename)

	// Get replicas for metadata
	replicas, err := d.Membership.GetReplicas(fileId, d.NumOfReplicas)
	if err != nil {
		return fmt.Errorf("failed to get replicas for metadata: %s", err.Error())
	}

	meta, err := d.getRemoteMeta(fileId, 2)
	if err != nil {
		return fmt.Errorf("failed to get metadata for %s: %s", filename, err.Error())
	}
	
	for _, replica := range replicas {
		// Read Metadata
		remoteMeta := new(files.Meta)
		err := d.RemoteCall("FileManager.ReadMeta", replica.Hostname, replica.Port+1, fileId, remoteMeta)
		if err != nil {
			*reply = false
			// return fmt.Errorf("failed to read meta for %s at %s:%d: %s", filename, replica.Hostname, replica.Port, err.Error())
			return nil
		}
		if remoteMeta.Counter < meta.Counter {
			*reply = false
			return nil
			// return fmt.Errorf("metadata counter of %s is not up to date at %s:%d", filename, replica.Hostname, replica.Port)
		}
	}

	// Get replicas for blocks
	for i := 0; i < meta.FileBlocks; i++ {
		blockInfo, _ := meta.GetBlock(i)
		replicas, err := d.Membership.GetReplicas(blockInfo.Id, d.NumOfReplicas)
		if err != nil {
			return fmt.Errorf("failed to get replicas for block %d of %s: %s", i, filename, err.Error())
		}
		for _, replica := range replicas {
			remoteBlock := new(files.BlockInfo)
			err := d.RemoteCall("FileManager.ReadBlockInfo", replica.Hostname, replica.Port+1, blockInfo.Id, remoteBlock)
			if err != nil {
				*reply = false 
				return nil
				// return fmt.Errorf("failed to read block-%d for %s at %s:%d: %s", i, filename, replica.Hostname, replica.Port, err.Error())
			}
			if remoteBlock.Counter < meta.Counter {
				*reply = false 
				// return fmt.Errorf("block %d of %s is not up to date at %s:%d", i, filename, replica.Hostname, replica.Port)
				return nil
			}
		}
	}
	*reply = true
	return nil
}
