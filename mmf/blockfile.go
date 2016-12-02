package mmf

import (
	"fmt"
	"io"
	"reflect"
	"runtime"
	"unsafe"
)

// The default block size that is used by CreateBlockFile and CreateBlockFileInMapper
const DefaultBlocksize = 4096

const BlockFileMagic uint32 = 0xB10CF11E         // the first 4 byte of a block-file
const reversedBlockFileMagic uint32 = 0x1EF10CB1 // used to check the endianes

const ContentFreeList uint32 = 0xF9337157

// Mapper is an interface that wraps basic methods for accessing memory mapped files.
type Mapper interface {
	Map(off int64, length int, handler func([]byte) error) error
	Size() int
	Truncate(size int64) error
}

type bfHeader struct {
	magic       uint32
	contentType uint32
	blocksize   uint32
	nextFree    uint32
}

var bfHeaderSize int = 16

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
	hdr := (*bfHeader)(unsafe.Pointer(&data[0]))
	if hdr.magic == reversedBlockFileMagic {
		return nil, fmt.Errorf("BlockFile: unable to read header: was the file generated on an other platform?")
	} else if hdr.magic != BlockFileMagic {
		return nil, fmt.Errorf("BlockFile: unable to read header: unexpected magic number")
	}
	return hdr, nil
}

func initBfHeaderFromSlice(data []byte, blocksize uint32) (*bfHeader, error) {
	if len(data) < bfHeaderSize {
		return nil, fmt.Errorf("BlockFile: slice to small for bf header")
	} else if len(data) < int(blocksize) {
		return nil, fmt.Errorf("BlockFile: slice to small for storing the blocksize")
	}
	hdr := (*bfHeader)(unsafe.Pointer(&data[0]))
	hdr.magic = BlockFileMagic
	hdr.contentType = 0
	hdr.blocksize = blocksize
	hdr.nextFree = 0
	return hdr, nil
}

type BlockFile struct {
	mapper    Mapper
	blocksize uint32
}

// OpenBlockFile opens an existing block-file that is given as filename.
func OpenBlockFile(filename string) (*BlockFile, error) {
	mf, err := OpenMappedFile(filename)
	if err != nil {
		return nil, err
	}
	return OpenBlockFileFromMapper(mf)
}

// OpenBlockFileFromMapper opens an existing block-file by providig a Mapper.
func OpenBlockFileFromMapper(mapper Mapper) (*BlockFile, error) {
	var blocksize uint32
	err := mapper.Map(0, bfHeaderSize, func(data []byte) error {
		hdr, err := bfHeaderFromSlice(data)
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

// CreateBlockFile creates a new block-file at the given filename with the DefaultBlocksize.
func CreateBlockFile(filename string) (*BlockFile, error) {
	return CreateBlockFileWithSize(filename, DefaultBlocksize)
}

// CreateBlockFileWithSize creates a new block-file at the given filename with the given blocksize.
func CreateBlockFileWithSize(filename string, blocksize uint32) (*BlockFile, error) {
	mf, err := CreateMappedFile(filename, int64(blocksize))
	if err != nil {
		return nil, err
	}
	return CreateBlockFileInMapperWithSize(mf, blocksize)
}

// CreateBlockFileInMapper creates a new block-file in the given Mapper with the DefaultBlocksize.
func CreateBlockFileInMapper(mapper Mapper) (*BlockFile, error) {
	return CreateBlockFileInMapperWithSize(mapper, DefaultBlocksize)
}

// CreateBlockFileInMapperWithSize creates a new block-file in the given Mapper with the given blocksize.
func CreateBlockFileInMapperWithSize(mapper Mapper, blocksize uint32) (*BlockFile, error) {
	bf := &BlockFile{mapper: mapper, blocksize: blocksize}
	err := bf.initHeaderBlock(0, nil)
	if err != nil {
		return nil, err
	}
	runtime.SetFinalizer(bf, (*BlockFile).Close)
	return bf, nil
}

// Close closes the underlying Mapper if it is a Closer
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

// BlockSize returns the size of a single block of the BlockFile.
func (bf *BlockFile) BlockSize() int {
	return int(bf.blocksize)
}

// MapBlock maps the block with the given index, and calls the handler.
// MapBlock is basically a wrapper for Mapper.Map that works with block-indices.
func (bf *BlockFile) MapBlock(block int, handler func([]byte) error) error {
	if block <= 0 {
		return fmt.Errorf("can't map block 0. This is the header-block.")
	}
	return bf.mapper.Map(int64(block)*int64(bf.blocksize), int(bf.blocksize), handler)
}

func (bf *BlockFile) initHeaderBlock(block int, handler func(*bfHeader) error) error {
	return bf.mapper.Map(int64(block)*int64(bf.blocksize), int(bf.blocksize), func(data []byte) error {
		hdr, err := initBfHeaderFromSlice(data, bf.blocksize)
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
		if handler != nil {
			return handler(hdr)
		}
		return nil
	})
}

// MapHeader maps the data section of header block (index 0), and calls the handler.
// The returned slice is a little bit smaller than the blocksize.
func (bf *BlockFile) MapHeader(handler func(data []byte, contentType uint32) error) error {
	return bf.mapper.Map(0, int(bf.blocksize), func(data []byte) error {
		hdr, err := bfHeaderFromSlice(data)
		if err != nil {
			return err
		}
		contentType := hdr.contentType
		if handler != nil {
			return handler(data[bfHeaderSize:], contentType)
		}
		return nil
	})
}

// AllocateBlock returns a new unused block-index. This either returns a block
// from an internal free-list (a block that was Freed earlier by FreeBlock), or
// allocates new space by calling Truncate on the mapper.
func (bf *BlockFile) AllocateBlock() (int, error) {
	var block int = 0
	err := bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
		block = int(hdr.nextFree)
		return nil
	})
	if err != nil {
		return 0, err
	}
	if block != 0 {
		// get the next free block
		var nextFree uint32 = 0
		err := bf.mapHeaderBlock(block, func(hdr *bfHeader) error {
			if hdr.contentType != ContentFreeList {
				return fmt.Errorf("block %d is not marked as free", block)
			}
			nextFree = hdr.nextFree
			return nil
		})
		if err != nil {
			return 0, err
		}
		// update nextFree in the header
		err = bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
			hdr.nextFree = nextFree
			return nil
		})
		if err != nil {
			return 0, err
		}
		return block, nil
	}
	// allocate new block
	newBlockIndex := (int64(bf.mapper.Size()) + int64(bf.blocksize) - 1) / int64(bf.blocksize)
	err = bf.mapper.Truncate((newBlockIndex + 1) * int64(bf.blocksize))
	if err != nil {
		return 0, err
	}
	return int(newBlockIndex), nil
}

// AllocateBlocks allocates a given number ob blocks (see AllocateBlock)
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

// FreeBlock puts the given block to an internal free-list, so that the block
// can be returned by future call to AllocateBlock.
func (bf *BlockFile) FreeBlock(block int) error {
	// get the old nextFree block
	var nextFree uint32 = 0
	err := bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
		nextFree = hdr.nextFree
		return nil
	})
	if err != nil {
		return err
	}
	// create a new free-list entry, in the free block
	err = bf.initHeaderBlock(block, func(hdr *bfHeader) error {
		hdr.contentType = ContentFreeList
		hdr.nextFree = nextFree
		return nil
	})
	if err != nil {
		return err
	}
	// update nextFree in the header
	err = bf.mapHeaderBlock(0, func(hdr *bfHeader) error {
		hdr.nextFree = uint32(block)
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

// FreeBlocks frees a given number ob blocks (see FreeBlock)
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
