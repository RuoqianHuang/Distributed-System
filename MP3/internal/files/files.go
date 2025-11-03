package files

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
)

const BLOCK_SIZE = 1024 * 1024 * 2 // 2 MiB

type Meta struct {
	Id         uint64   // File ID
	FileSize   uint64   // File size
	FileName   string   // File name
	FileBlocks int      // Number of blocks
	Counter    uint64   // File version counter
}

func GetIdFromFilename(filename string) uint64 {
	hash := sha256.Sum256([]byte(filename))
	return uint64(binary.BigEndian.Uint64(hash[:8]))
}

func CreateMeta(filename string, fileSize uint64) Meta {
	hash := sha256.Sum256([]byte(filename))
	return Meta{
		Id: uint64(binary.BigEndian.Uint64(hash[:8])),
		FileName: filename,
		FileSize:  fileSize,
		FileBlocks: int((fileSize - 1) / BLOCK_SIZE + 1), // Ceil(fileSize / BLOCK_SIZE)
		Counter: 1, // Counter + 1 on created
	}
}

func (f *Meta) GetBlock(i int) (BlockInfo, error) {
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

type BlockPackage struct {
	BlockInfo     BlockInfo  // Information about the requested block
	Data          []byte     // Data of the block
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

func (b *Block) Read() (BlockPackage, error) {
	b.lock.RLock()
	defer b.lock.RUnlock()
	filename := fmt.Sprintf("%s-%d.block", b.BlockInfo.FileName, b.BlockInfo.Id)
	res, err := os.ReadFile(filename) // Read file
	return BlockPackage{
		BlockInfo: b.BlockInfo,
		Data: res,
	}, err
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
	localFileMeta   map[uint64]Meta      // Local file metadata
	lock            sync.RWMutex         // RW mutex
}

func NewFileManager() *FileManager {
	return &FileManager{
		localBlocks: make(map[uint64]*Block),
		localFileMeta: make(map[uint64]Meta),
	}
}

// Functions for local calls
func (f *FileManager) RegisterRPC() {
	rpc.Register(f)
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

func (f *FileManager) GetBlock(id uint64) (*Block, error) {
	f.lock.RLock()
	defer f.lock.RUnlock()
	ret, ok := f.localBlocks[id]
	if ok {
		return ret, nil
	}
	return nil, fmt.Errorf("no such block")
}

func (f *FileManager) GetMetas() map[uint64]Meta {
	f.lock.RLock()
	defer f.lock.RUnlock()
	ret := make(map[uint64]Meta, len(f.localFileMeta))
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

// Functions for remote calls
func (f *FileManager) ReadBlockInfo(id uint64, reply *BlockInfo) error {
	f.lock.RLock()
	block, ok := f.localBlocks[id]
	f.lock.RUnlock()
	if ok {
		*reply = block.GetInfo()
	} else {
		*reply = BlockInfo{ // Return an empty block with counter = 0
			Id: id,
			Counter: 0,
		}
	}
	return nil
}

func (f *FileManager) WriteBlock(args BlockPackage, reply *uint64) error {
	f.lock.Lock()
	// Update local info 
	block, ok := f.localBlocks[args.BlockInfo.Id]
	if !ok {
		block = &Block{ // Insert new empty block
			BlockInfo: BlockInfo{
				Id: args.BlockInfo.Id,
				BlockNumber: args.BlockInfo.BlockNumber,
				Counter: 0,
				FileName: args.BlockInfo.FileName,
			},
		}
		f.localBlocks[args.BlockInfo.Id] = block
	}
	f.lock.Unlock()

	// Write block to local file system
	cnt, err := block.Write(args.Data, args.BlockInfo.Counter)
	*reply = cnt
	return err
}

func (f *FileManager) ReadBlock(id uint64, reply *BlockPackage) error {
	f.lock.RLock()
	block, ok := f.localBlocks[id]
	f.lock.RUnlock()
	if !ok {
		// Block does not exist, return empty block with counter = 0
		*reply = BlockPackage{
			BlockInfo: BlockInfo{
				Id: id,
				Counter: 0,
			},
		}
		return nil
	}
	// Read from local file system
	blockPack, err := block.Read()
	*reply = blockPack
	return err
}

func (f *FileManager) WriteMeta(file Meta, reply *uint64) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	meta, ok := f.localFileMeta[file.Id]
	if !ok {
		f.localFileMeta[file.Id] = file
	} else if meta.Counter < file.Counter {
		f.localFileMeta[file.Id] = file
	}
	*reply = f.localFileMeta[file.Id].Counter
	return nil
}

func (f *FileManager) ReadMeta(id uint64, reply *Meta) error {
	f.lock.RLock()
	defer f.lock.RUnlock()
	meta, ok := f.localFileMeta[id]
	if !ok {
		*reply = Meta{ // Return a empty meta with counter = 0
			Id: id,
			Counter: 0,
		}
		return nil
	} 
	*reply = meta
	return nil
}


func CreateTable(fileMap map[uint64]Meta) string {
	type Pair struct {
		Id       uint64
		Filename string
	}
	metaList := make([]Pair, 0, 16)
	for id, meta := range fileMap {
		metaList = append(metaList, Pair{
			Id:       id,
			Filename: meta.FileName,
		})
	}
	sort.Slice(metaList, func(i, j int) bool {
		return metaList[i].Filename < metaList[j].Filename
	})
	// -----------------------------------------------------------
	// | ID    | File name | File size | Num of blocks | Counter |
	// | ID    | File name | File size | Num of blocks | Counter |
	// -----------------------------------------------------------
	maxLengths := map[string]int{
		"Id":             2,
		"File name":      9,
		"File size":      9,
		"Num of blocks":  13,
		"Counter":        7,
	}
	for _, i := range metaList {
		info := fileMap[i.Id]
		lengths := map[string]int{
			"Id":            len(fmt.Sprintf("%d", i.Id)),
			"File name":     len(i.Filename),
			"File size":     len(fmt.Sprintf("%f MiB", float64(info.FileSize) / BLOCK_SIZE)),
			"Num of blocks": len(fmt.Sprintf("%d", info.FileBlocks)),
			"Counter":       len(fmt.Sprintf("%d", info.Counter)),
		}
		for key, value := range lengths {
			if maxLengths[key] < value {
				maxLengths[key] = value
			}
		}
	}
	totalLength := 16
	for _, v := range maxLengths {
		totalLength = totalLength + v
	}
	res := strings.Repeat("-", totalLength) + "\n"

	// Add column headers
	header := "| "

	// ID header
	s := "ID"
	if len(s) < maxLengths["Id"] {
		s = s + strings.Repeat(" ", maxLengths["Id"]-len(s))
	}
	header = header + s + " | "

	// File name header
	s = "File name"
	if len(s) < maxLengths["File name"] {
		s = s + strings.Repeat(" ", maxLengths["File name"]-len(s))
	}
	header = header + s + " | "

	// File size header
	s = "File size"
	if len(s) < maxLengths["File size"] {
		s = s + strings.Repeat(" ", maxLengths["File size"]-len(s))
	}
	header = header + s + " | "

	// Num of blocks header
	s = "Num of blocks"
	if len(s) < maxLengths["Num of blocks"] {
		s = s + strings.Repeat(" ", maxLengths["Num of blocks"]-len(s))
	}
	header = header + s + " | "

	// Counter header
	s = "Counter"
	if len(s) < maxLengths["Counter"] {
		s = s + strings.Repeat(" ", maxLengths["Counter"]-len(s))
	}
	header = header + s + " |"

	res = res + header + "\n"
	res = res + strings.Repeat("-", totalLength) + "\n"

	for _, i := range metaList {
		info := fileMap[i.Id]
		line := "| "

		// Id
		s := fmt.Sprintf("%d", i.Id)
		if len(s) < maxLengths["Id"] {
			s = s + strings.Repeat(" ", maxLengths["Id"]-len(s))
		}
		line = line + s + " | "

		// File name
		s = i.Filename
		if len(s) < maxLengths["File name"] {
			s = s + strings.Repeat(" ", maxLengths["File name"]-len(s))
		}
		line = line + s + " | "

		// File size
		s = fmt.Sprintf("%f MiB", float64(info.FileSize) / BLOCK_SIZE)
		if len(s) < maxLengths["File size"] {
			s = s + strings.Repeat(" ", maxLengths["File size"]-len(s))
		}
		line = line + s + " | "

		// Num of blocks
		s = fmt.Sprintf("%d", info.FileBlocks)
		if len(s) < maxLengths["Num of blocks"] {
			s = s + strings.Repeat(" ", maxLengths["Num of blocks"]-len(s))
		}
		line = line + s + " | "

		// Counter
		s = fmt.Sprintf("%d", info.Counter)
		if len(s) < maxLengths["Counter"] {
			s = s + strings.Repeat(" ", maxLengths["Counter"]-len(s))
		}
		line = line + s + " |"
		res = res + line + "\n"
	}
	res = res + strings.Repeat("-", totalLength) + "\n"
	return res
} 