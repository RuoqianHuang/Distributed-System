package files

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

const FILE_SERVICE_PORT = 2121     // Port for file service
const BLOCK_SIZE = 4 * 1024 * 1024 // 4 MiB 

type File struct {
	Id         uint64   // File ID
	FileSize   uint64   // File size
	FileName   string   // File name
	FileBlocks int      // Number of blocks
	Counter    uint64   // File version counter
}

func (f *File) GetBlock(i int) (BlockInfo, error) {
	if i >= f.FileBlocks {
		return BlockInfo{}, fmt.Errorf("block does not exist")
	}
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s%d", f.FileName, i)))
	return BlockInfo{
		Id: uint64(binary.BigEndian.Uint64(hash[:8])),
		BlockNumber: i,
		Counter: f.Counter,
		FileName: f.FileName,
	}, nil
}

type BlockInfo struct {
	Id          uint64       // Block id
	BlockNumber int          // Block number
	Counter     uint64       // Block version counter
	FileName    string       // File name
}

type Block struct {
	BlockInfo   BlockInfo    // Block Info
	lock        sync.RWMutex // RW mutex for Counter and File read write
}

func (b *Block) Write(data []byte, Counter uint64) (uint64, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.BlockInfo.Counter >= Counter {
		return b.BlockInfo.Counter, nil // Ignore write of older version
	}
	filename := fmt.Sprintf("%s-%d.block", b.BlockInfo.FileName, b.BlockInfo.Id)
	err := os.WriteFile(filename, data, 0644) // Update file
	if err != nil {
		return b.BlockInfo.Counter, err
	}
	b.BlockInfo.Counter = Counter // Only update counter when the write is successful
	return b.BlockInfo.Counter, err
}

func (b *Block) Remove() error {
	b.lock.Lock()
	defer b.lock.Unlock()
	filename := fmt.Sprintf("%s-%d.block", b.BlockInfo.FileName, b.BlockInfo.Id)
	return os.Remove(filename)
}

func (b *Block) UpdateCounter(Counter uint64) uint64 {
	// Update only the counter 
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.BlockInfo.Counter >= Counter {
		return b.BlockInfo.Counter // Ignore older version
	}
	b.BlockInfo.Counter = Counter 
	return b.BlockInfo.Counter
}

func (b *Block) Read() (uint64, []byte, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	filename := fmt.Sprintf("%s-%d.block", b.BlockInfo.FileName, b.BlockInfo.Id)
	res, err := os.ReadFile(filename) // Read file
	return b.BlockInfo.Counter, res, err
}

func (b *Block) GetCounter() uint64 {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.BlockInfo.Counter
}

func (b *Block) GetInfo() BlockInfo {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.BlockInfo
}

type FileManager struct {
	localBlocks     map[uint64]*Block    // Local file blocks
	localFileMeta   map[uint64]File      // Local file metadata
	lock            sync.RWMutex         // RW mutex
}

type ArgsBlock struct {
	Block BlockInfo  // Information about the requested block
	Data  []byte     // Data of the block
	HasData bool     // If has data, read/write data from/to file system
}

func (f *FileManager) GetBlocks() map[uint64]BlockInfo {
	f.lock.RLock()
	defer f.lock.RUnlock()
	ret := make(map[uint64]BlockInfo, len(f.localBlocks))
	for id, block := range f.localBlocks {
		// Copy the data from the internal map 
		ret[id] = block.GetInfo()
	}
	return ret
}

func (f *FileManager) GetFiles() map[uint64]File {
	f.lock.RLock()
	defer f.lock.RUnlock()
	ret := make(map[uint64]File, len(f.localFileMeta))
	for id, file := range f.localFileMeta {
		// Copy the data from the internal map 
		ret[id] = file
	}
	return ret
}

func (f *FileManager) RemoveBlock(Id uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	block, ok := f.localBlocks[Id]
	if ok {
		delete(f.localBlocks, Id)
		block.Remove()
	}
}

func (f *FileManager) RemoveMeta(Id uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	delete(f.localFileMeta, Id)
}

func (f *FileManager) WriteBlock(args *ArgsBlock, reply *ArgsBlock) error {
	f.lock.Lock()
	// Update local info 
	block, ok := f.localBlocks[args.Block.Id]
	if !ok {
		f.localBlocks[args.Block.Id] = &Block{ // Insert new empty block
			BlockInfo: BlockInfo{
				Id: args.Block.Id,
				BlockNumber: args.Block.BlockNumber,
				Counter: 0,
				FileName: args.Block.FileName,
			},
		}
	}
	f.lock.Unlock()

	if args.HasData { // Write data to file system
		// Write block to local file system
		_, err := block.Write(args.Data, args.Block.Counter)
		*reply = ArgsBlock{
			Block: block.GetInfo(),
			HasData: false,
		}
		return err
	}
	// Only update the counter
	block.UpdateCounter(args.Block.Counter)
	*reply = ArgsBlock{
		Block: block.GetInfo(),
		HasData: false,
	}
	return nil
}

func (f *FileManager) ReadBlock(args *ArgsBlock, reply *ArgsBlock) error {
	f.lock.RLock()
	block, ok := f.localBlocks[args.Block.Id]
	f.lock.RUnlock()
	if !ok {
		// Block does not exist, return empty block with counter = 0
		*reply = ArgsBlock{
			Block: BlockInfo{
				Id: args.Block.Id,
				BlockNumber: args.Block.BlockNumber,
				Counter: 0,
				FileName: args.Block.FileName,
			},	
			HasData: false,
		}
		return nil
	}
	// Read from local file system
	counter, data, err := block.Read()
	if err != nil {
		return err
	}
	reply.Data = data
	reply.Block = block.GetInfo()
	reply.Block.Counter = counter // Use the counter of the data read
	reply.HasData = true
	return nil
}

func (f *FileManager) WriteMeta(file *File, reply *uint64) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	meta, ok := f.localFileMeta[file.Id]
	if !ok {
		f.localFileMeta[file.Id] = *file
	} else if meta.Counter < file.Counter {
		f.localFileMeta[file.Id] = *file
	}
	*reply = f.localFileMeta[file.Id].Counter
	return nil
}

func (f *FileManager) ReadMeta(file *File, reply *File) error {
	f.lock.RLock()
	defer f.lock.RUnlock()
	meta, ok := f.localFileMeta[file.Id]
	if !ok {
		return fmt.Errorf("no such file")
	} 
	*reply = meta
	return nil
}