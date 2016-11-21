package mmf

import (
	"fmt"
	"io"
	"reflect"
	"runtime"
	"unsafe"
)

const DefaultBlocksize = 4096

const BlockFileMagic uint32 = 0xB10CF11E
const reversedBlockFileMagic uint32 = 0x1EF10CB1 // used to check the endianes

type bfHeader struct {
	magic     uint32
	blocksize uint32
	next      uint32 // reserved
	free_len  uint32
	free_head uint32
}

var bfHeaderEntries int = 5
var bfHeaderSize int = bfHeaderEntries * 4

func init() {
	// ensure, the size of the bfHeader struct is correct
	if reflect.TypeOf(bfHeader{}).Size() != uintptr(bfHeaderSize) {
		panic("unexpected size for bfHeader struct")
	}
}

func bfHeaderFromSlice(data []byte) (*bfHeader, error) {
	if len(data) < bfHeaderSize {
		return nil, fmt.Errorf("BlockFile: slice to small for bf header")
	}
	return (*bfHeader)(unsafe.Pointer(&data[0])), nil
}

func (hdr *bfHeader) Validate() error {
	if hdr.magic == reversedBlockFileMagic {
		return fmt.Errorf("BlockFile: unable to read header: was the file generated on an other platform?")
	} else if hdr.magic != BlockFileMagic {
		return fmt.Errorf("BlockFile: unable to read header: unexpected magic number")
	} else if hdr.free_head < 0 || hdr.free_head > hdr.free_len {
		return fmt.Errorf("BlockFile: error in free-list definition")
	} else if uint32(bfHeaderSize)+hdr.free_len*4 > hdr.blocksize {
		return fmt.Errorf("BlockFile: slice to small for storing the free-list")
	}
	return nil
}

func (hdr *bfHeader) Initialize(blocksize uint32) error {
	hdr.magic = BlockFileMagic
	hdr.blocksize = blocksize
	hdr.next = 0
	hdr.free_len = (blocksize - uint32(bfHeaderSize)) / 4
	hdr.free_head = 0
	return nil
}

func createBfHeaderInSlice(data []byte) (*bfHeader, error) {
	if len(data) < bfHeaderSize {
		return nil, fmt.Errorf("BlockFile: slice to small for bf header")
	}
	hdr := (*bfHeader)(unsafe.Pointer(&data[0]))

	return hdr, nil
}

func (hdr *bfHeader) freeList() []uint32 {
	return (*[1<<31 - 1]uint32)(unsafe.Pointer(hdr))[bfHeaderEntries : bfHeaderEntries+int(hdr.free_head)]
}

func (hdr *bfHeader) freeListPush(entry uint32) error {
	cur := hdr.free_head
	if cur >= hdr.free_len {
		return io.EOF
	}
	hdr.free_head++
	hdr.freeList()[cur] = entry
	return nil
}

func (hdr *bfHeader) freeListPop() (uint32, error) {
	cur := hdr.free_head
	if cur <= 0 {
		return ^uint32(0), io.EOF
	}
	cur--
	entry := hdr.freeList()[cur]
	hdr.free_head = cur
	return entry, nil
}

type BlockFile struct {
	mapper    Mapper
	blocksize uint32
}

func OpenBlockFile(filename string) (*BlockFile, error) {
	mf, err := OpenMappedFile(filename)
	if err != nil {
		return nil, err
	}
	return OpenBlockFileFromMapper(mf)
}

func OpenBlockFileFromMapper(mapper Mapper) (*BlockFile, error) {
	var blocksize uint32
	err := mapper.Map(0, bfHeaderSize, func(data []byte) error {
		hdr, err := bfHeaderFromSlice(data)
		if err != nil {
			return err
		}
		err = hdr.Validate()
		if err != nil {
			return err
		}
		blocksize = hdr.blocksize
		return nil
	})
	if err != nil {
		return nil, err
	}
	if mapper.Size() < int(blocksize) {
		return nil, fmt.Errorf("mapper is to small for the blocksize specified in the file")
	}
	return &BlockFile{mapper: mapper, blocksize: blocksize}, nil
}

func CreateBlockFile(filename string) (*BlockFile, error) {
	return CreateBlockFileWithSize(filename, DefaultBlocksize)
}

func CreateBlockFileWithSize(filename string, blocksize uint32) (*BlockFile, error) {
	mf, err := CreateMappedFile(filename, int64(blocksize))
	if err != nil {
		return nil, err
	}
	return CreateBlockFileInMapperWithSize(mf, blocksize)
}

func CreateBlockFileInMapper(mapper Mapper) (*BlockFile, error) {
	return CreateBlockFileInMapperWithSize(mapper, DefaultBlocksize)
}

func CreateBlockFileInMapperWithSize(mapper Mapper, blocksize uint32) (*BlockFile, error) {
	bf := &BlockFile{mapper: mapper, blocksize: blocksize}
	err := bf.initHeaderBlock(0, nil)
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(bf, (*BlockFile).Close)
	return bf, nil
}

func (bf *BlockFile) Close() error {
	if bf.mapper != nil {
		closable, ok := bf.mapper.(io.Closer)
		bf.mapper = nil
		if ok {
			err := closable.Close()
			if err != nil {
				return err
			}
		}
	}
	runtime.SetFinalizer(bf, nil)
	return nil
}

func (bf *BlockFile) MapBlock(block int, handler func([]byte) error) error {
	return bf.mapper.Map(int64(block)*int64(bf.blocksize), int(bf.blocksize), handler)
}

func (bf *BlockFile) initHeaderBlock(block int, handler func(*bfHeader) error) error {
	return bf.mapper.Map(int64(block)*int64(bf.blocksize), int(bf.blocksize), func(data []byte) error {
		hdr, err := bfHeaderFromSlice(data)
		if err != nil {
			return err
		}
		err = hdr.Initialize(bf.blocksize)
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(hdr)
		}
		return nil
	})
}

func (bf *BlockFile) mapHeaderBlock(block int, handler func(*bfHeader) error) error {
	return bf.mapper.Map(int64(block)*int64(bf.blocksize), int(bf.blocksize), func(data []byte) error {
		hdr, err := bfHeaderFromSlice(data)
		if err != nil {
			return err
		}
		err = hdr.Validate()
		if err != nil {
			return err
		}
		if handler != nil {
			return handler(hdr)
		}
		return nil
	})
}

func (bf *BlockFile) AllocateBlock() (int, error) {
	var block, nextHdrIdx int = 0, 0
	err := bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
		b, err := hdr.freeListPop()
		if err != nil {
			nextHdrIdx = int(hdr.next)
			return err
		}
		block = int(b)
		return nil
	})
	if err == nil {
		return block, nil
	} else if err != io.EOF {
		return 0, err
	}
	if nextHdrIdx != 0 {
		hdrIdx := nextHdrIdx
		nextHdrIdx = 0
		err := bf.mapHeaderBlock(hdrIdx, func(hdr *bfHeader) error {
			b, err := hdr.freeListPop()
			if err != nil {
				nextHdrIdx = int(hdr.next)
				return err
			}
			block = int(b)
			return nil
		})
		if err == nil {
			return block, nil
		} else if err != io.EOF {
			return 0, err
		}
		// change next of page 0
		err = bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
			hdr.next = uint32(nextHdrIdx)
			return nil
		})
		if err != nil {
			return 0, err
		}
		return hdrIdx, nil
	}
	// allocate new block
	newBlockIndex := (int64(bf.mapper.Size()) + int64(bf.blocksize) - 1) / int64(bf.blocksize)
	err = bf.mapper.Truncate((newBlockIndex + 1) * int64(bf.blocksize))
	if err != nil {
		return 0, err
	}
	return int(newBlockIndex), nil
}

func (bf *BlockFile) AllocateBlocks(num int) ([]int, error) {
	blocks := make([]int, num)
	for n := 0; n < num; n++ {
		block, err := bf.AllocateBlock()
		if err != nil {
			return blocks[0:n], err
		}
		blocks[n] = block
	}
	return blocks, nil
}

func (bf *BlockFile) FreeBlock(block int) error {
	var nextHdrIdx int = 0
	err := bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
		err := hdr.freeListPush(uint32(block))
		if err != nil {
			nextHdrIdx = int(hdr.next)
			return err
		}
		return nil
	})
	if err == nil {
		return nil
	} else if err != io.EOF {
		return err
	}
	if nextHdrIdx != 0 {
		err := bf.mapHeaderBlock(nextHdrIdx, func(hdr *bfHeader) error {
			return hdr.freeListPush(uint32(block))
		})
		if err == nil {
			return nil
		} else if err != io.EOF {
			return err
		}
	}
	// map block to a new header
	err = bf.initHeaderBlock(block, func(hdr *bfHeader) error {
		hdr.next = uint32(nextHdrIdx)
		return nil
	})
	if err != nil {
		return err
	}
	// change next of page 0
	err = bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
		hdr.next = uint32(block)
		return nil
	})
	return err
}

func (bf *BlockFile) FreeBlocks(blocks []int) (int, error) {
	n := 0
	for _, block := range blocks {
		if err := bf.FreeBlock(block); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}
