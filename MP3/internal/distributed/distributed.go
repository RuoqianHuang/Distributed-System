package distributed

import (
	"cs425/mp3/internal/files"
	"cs425/mp3/internal/member"
	"cs425/mp3/internal/queue"
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

type DistributedFiles struct {
	Tround               time.Duration
	Hostname             string
	Port                 int
	FileManager          *files.FileManager
	Membership           *member.Membership
	NumOfReplicas        int
	NumOfMetaWorkers     int
	NumOfBlockWorkers    int
	NumOfBufferedWorkers int
	NumOfTries           int                      // Number of tries uploading blocks
	MetaJobs             *queue.Queue             // Merge job queue for metadata
	BlockJobs            *queue.Queue             // Merge job queue for blocks
	BufferedBlocks       *queue.Queue             // Buffered block queue
	lockBufferedBlocks   sync.RWMutex             // Lock for buffered blocks
	BufferedBlockMap     map[uint64]BufferedBlock // Buffered blocks
	LockedMeta           map[uint64]files.Meta    // Locked metadata (to ensure atomic operation)
	locklockedMeta       sync.RWMutex             // Lock for locked metadata (to ensure atomic operation)
}

func RemoteCall(
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
		result := new(int)
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
	err := RemoteCall("FileManager.ReadMeta", hostname, port, meta.Id, result)
	if err != nil {
		log.Printf("[DF] Failed to get remote meta counter of %s: %s", meta.FileName, err.Error())
		return 0
	}
	// Check file version
	if result.Counter < meta.Counter {
		// Need update
		_reply := new(uint64)
		err = RemoteCall("FileManager.WriteMeta", hostname, port, meta, _reply)
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
	err := RemoteCall("FileManager.ReadBlockInfo", hostname, port, blockInfo.Id, result)
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
		err = RemoteCall("FileManager.WriteBlock", hostname, port, blockPack, reply)
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
		// Get job from queue
		val := d.MetaJobs.Pop() // Blocking call
		id := val.(uint64)

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
		// Get job from queue
		val := d.BlockJobs.Pop() // Blocking call
		id := val.(uint64)

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
			err = RemoteCall("FileManager.ReadMeta", replica.Hostname, replica.Port+1, id, result) // using udp port + 1 as rpc port
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
			err = RemoteCall("FileManager.ReadBlock", replica.Hostname, replica.Port+1, id, result) // using udp port + 1 as rpc port
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
			err = RemoteCall("DistributedFiles.ReceiveBufferedBlocks", replica.Hostname, replica.Port+1, blockPack, result)
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
func (d *DistributedFiles) GetLock(meta files.Meta, reply *files.Meta) error {
	d.locklockedMeta.Lock()
	defer d.locklockedMeta.Unlock()
	lockedMeta, ok := d.LockedMeta[meta.Id]
	if ok {
		*reply = lockedMeta
		return fmt.Errorf("this file is currently locked")
	}
	// lock new meta
	d.LockedMeta[meta.Id] = meta
	return nil
}

func (d *DistributedFiles) ReleaseLock(meta files.Meta, reply *files.Meta) error {
	d.locklockedMeta.Lock()
	defer d.locklockedMeta.Unlock()
	_, ok := d.LockedMeta[meta.Id]
	if ok {
		// unlock meta
		delete(d.LockedMeta, meta.Id)
	}
	return nil
}

// Function for CLI calls
func (d *DistributedFiles) Create(filename string, fileSource string, quorum int) error {
	id := files.GetIdFromFilename(filename)
	meta, err := d.getRemoteMeta(id, quorum)
	if err != nil {
		return fmt.Errorf("failed to read remote meta: %s", err.Error())
	}
	if meta.Counter > 0 {
		return fmt.Errorf("file already exist")
	}
	// Read block from file system
	data, err := os.ReadFile(fileSource)
	if err != nil {
		return fmt.Errorf("failed to read file source: %s", err.Error())
	}
	log.Printf("[DF] Create: read data from local source file %s", fileSource)

	meta = files.CreateMeta(filename, uint64(len(data)))
	result := new(files.Meta)
	// Lock the meta, so that no other process can write to this file
	// Use the first replica as coordinator
	replicas, err := d.Membership.GetReplicas(meta.Id, d.NumOfReplicas)
	if err != nil {
		return fmt.Errorf("failed to get replicas: %s", err.Error())
	}
	err = RemoteCall("DistributedFiles.GetLock", replicas[0].Hostname, replicas[0].Port+1, meta, result)
	if err != nil {
		return fmt.Errorf("failed to acquire lock of %s from %s:%d: %s", filename, replicas[0].Hostname, replicas[0].Port, err.Error())
	}
	log.Printf("[DF] Create: accquired lock for file %s from %s:%d", filename, replicas[0].Hostname, replicas[0].Port)
	log.Printf("[DF] Cteate: start create file: %s, file size: %d bytes, num of blocks: %d", filename, len(data), meta.FileBlocks)
	defer func() {
		// Release lock
		for i := 0; i < d.NumOfTries; i++ { // Try
			err = RemoteCall("DistributedFiles.ReleaseLock", replicas[0].Hostname, replicas[0].Port+1, meta, result)
			if err == nil {
				log.Printf("[DF] lock for %s released", filename)
				break
			}
		}
	}()

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
					return fmt.Errorf("failed to send block %d for %d times, abort", i, tries[i])
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
					return fmt.Errorf("failed to send block %d for %d times: %s, abort", i, tries[i], err.Error())
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
		return nil
	}
	return fmt.Errorf("failed reach quorum, expecting %d, but got %d", quorum, successCount)
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

func (d *DistributedFiles) Append(filename string, fileSource string, quorum int) error {
	id := files.GetIdFromFilename(filename)
	meta, err := d.getRemoteMeta(id, quorum)
	if err != nil {
		return fmt.Errorf("failed to read remote meta: %s", err.Error())
	}
	if meta.Counter == 0 {
		return fmt.Errorf("file does not exist")
	}
	// Read block from file system
	data, err := os.ReadFile(fileSource)
	if err != nil {
		return fmt.Errorf("failed to read file source: %s", err.Error())
	}
	log.Printf("[DF] Append: read data from local source file %s", fileSource)

	// Lock the meta, so that no other process can write to this file
	// Use the first replica as coordinator
	result := new(files.Meta)
	replicas, err := d.Membership.GetReplicas(meta.Id, d.NumOfReplicas)
	if err != nil {
		return fmt.Errorf("failed to get replicas: %s", err.Error())
	}
	err = RemoteCall("DistributedFiles.GetLock", replicas[0].Hostname, replicas[0].Port+1, meta, result)
	if err != nil {
		return fmt.Errorf("failed to acquire lock of %s from %s:%d: %s", filename, replicas[0].Hostname, replicas[0].Port, err.Error())
	}
	log.Printf("[DF] Append: accquired lock for file %s from %s:%d", filename, replicas[0].Hostname, replicas[0].Port)
	defer func() {
		// Release lock
		for i := 0; i < d.NumOfTries; i++ { // Try
			err = RemoteCall("DistributedFiles.ReleaseLock", replicas[0].Hostname, replicas[0].Port+1, meta, result)
			if err == nil {
				log.Printf("[DF] lock for %s released", filename)
				break
			}
		}
	}()
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
	startPos := min(files.BLOCK_SIZE-residual, len(data))
	lastBlock := meta.FileBlocks - 1

	if residual != 0 {
		// We need to get the last block and update it
		success := false
		blockPack := files.BlockPackage{}
		lastBlockInfo, _ := newMeta.GetBlock(lastBlock)
		for t := 0; t < d.NumOfTries; t++ {
			pack, err := d.getRemoteBlock(lastBlockInfo.Id, quorum)
			if err != nil {
				continue
			}
			blockPack = files.BlockPackage{
				BlockInfo: lastBlockInfo,
				Data:      append(pack.Data, data[:startPos]...),
			}
			success = true
			break
		}
		if !success {
			return fmt.Errorf("failed to get block %d after %d tries", lastBlock, d.NumOfTries)
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
			return fmt.Errorf("failed to send new block to replica after %d tries", d.NumOfTries)
		}
		log.Printf("[DF] Append: block %s-%d updated", filename, lastBlock)
	}

	if lastBlock+1 < newMeta.FileBlocks {
		// We have new blocks to update
		blocksToUpdate := newMeta.FileBlocks - lastBlock - 1
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
						return fmt.Errorf("failed to send block %d for %d times: %s, abort", i+lastBlock+1, tries[i], err.Error())
					}
				}
				l := startPos + i*files.BLOCK_SIZE
				r := min(l+files.BLOCK_SIZE, len(data))
				blockPack := files.BlockPackage{
					BlockInfo: blockInfo,
					Data:      data[l:r],
				}
				err = d.sendBufferedBlocks(blockPack, quorum)
				if err != nil {
					tries[i]++
					if tries[i] >= d.NumOfTries {
						return fmt.Errorf("failed to send block %d for %d times: %s, abort", i+lastBlock+1, tries[i], err.Error())
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
		return nil
	}
	return fmt.Errorf("failed reach quorum, expecting %d, but got %d", quorum, successCount)
}

func (d *DistributedFiles) Merge(_ int, reply *int) error {
	// Loop through local files and redistribute them
	metas := d.FileManager.GetMetas()
	for id := range metas {
		d.MetaJobs.Push(id) // Add local meta to merge job
	}
	blocks := d.FileManager.GetBlocks()
	for id := range blocks {
		d.BlockJobs.Push(id) // Add local block to merge job
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
		err := RemoteCall("DistributedFiles.GetMetas", member.Hostname, member.Port+1, dummyArg, reply)
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

type FileInfo struct {
	FileMeta         files.Meta
	FileMetaReplicas []member.Info
}

func (d *DistributedFiles) GetFileInfo() {
	// TODO: list file info
}
