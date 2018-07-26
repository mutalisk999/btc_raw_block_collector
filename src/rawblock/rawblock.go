package rawblock

import (
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"github.com/mutalisk999/bitcoin-lib/src/blob"
	"github.com/mutalisk999/bitcoin-lib/src/serialize"
	"io"
	"os"
	"strconv"
	"sync"
)

const (
	RawBlockIndexSize = 4 + 32 + 4 + 4 + 4 + 4
)

type RawBlockIndex struct {
	BlockHeight       uint32
	BlockHash         bigint.Uint256
	RawBlockSize      uint32
	RawBlockFileTag   uint32
	BlockFileStartPos uint32
	BlockFileEndPos   uint32
}

func (r RawBlockIndex) Pack(writer io.Writer) error {
	var err error
	err = serialize.PackUint32(writer, r.BlockHeight)
	if err != nil {
		return err
	}
	err = r.BlockHash.Pack(writer)
	if err != nil {
		return err
	}
	err = serialize.PackUint32(writer, r.RawBlockSize)
	if err != nil {
		return err
	}
	err = serialize.PackUint32(writer, r.RawBlockFileTag)
	if err != nil {
		return err
	}
	err = serialize.PackUint32(writer, r.BlockFileStartPos)
	if err != nil {
		return err
	}
	err = serialize.PackUint32(writer, r.BlockFileEndPos)
	if err != nil {
		return err
	}
	return nil
}

func (r *RawBlockIndex) UnPack(reader io.Reader) error {
	var err error
	r.BlockHeight, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	err = r.BlockHash.UnPack(reader)
	if err != nil {
		return err
	}
	r.RawBlockSize, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	r.RawBlockFileTag, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	r.BlockFileStartPos, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	r.BlockFileEndPos, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	return nil
}

type RawBlockIndexManager struct {
	BlockIndexFileName string
	BlockIndexFileObj  *os.File
	BlockFileIndexPos  uint32
	blockIndexMutex    *sync.RWMutex
}

func (r *RawBlockIndexManager) Init(indexDir string, indexName string) error {
	if r.blockIndexMutex == nil {
		r.blockIndexMutex = new(sync.RWMutex)
	}
	r.blockIndexMutex.Lock()
	var err error
	r.BlockIndexFileObj, err = os.OpenFile(indexDir+"/"+indexName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend)
	if err != nil {
		r.blockIndexMutex.Unlock()
		return err
	}
	r.BlockFileIndexPos = 0
	r.BlockIndexFileName = indexName
	r.blockIndexMutex.Unlock()
	return nil
}

func (r *RawBlockIndexManager) AddNewBlockIndex(newBlockIndex *RawBlockIndex) error {
	r.blockIndexMutex.Lock()
	err := newBlockIndex.Pack(r.BlockIndexFileObj)
	if err != nil {
		r.blockIndexMutex.Unlock()
		return err
	}
	r.BlockFileIndexPos = r.BlockFileIndexPos + RawBlockIndexSize
	r.blockIndexMutex.Unlock()
	return nil
}

type RawBlock struct {
	BlockHeight    uint32
	BlockHash      bigint.Uint256
	CompressedType byte
	RawBlockData   blob.Byteblob
}

func (r RawBlock) Pack(writer io.Writer) error {
	var err error
	err = serialize.PackUint32(writer, r.BlockHeight)
	if err != nil {
		return err
	}
	err = r.BlockHash.Pack(writer)
	if err != nil {
		return err
	}
	err = serialize.PackByte(writer, r.CompressedType)
	if err != nil {
		return err
	}
	err = r.RawBlockData.Pack(writer)
	if err != nil {
		return err
	}
	return nil
}

func (r RawBlock) PackSize() uint32 {
	return 4 + 32 + 1 + serialize.CompactSizeLen(uint64(len(r.RawBlockData.GetData()))) + uint32(len(r.RawBlockData.GetData()))
}

func (r *RawBlock) UnPack(reader io.Reader) error {
	var err error
	r.BlockHeight, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	err = r.BlockHash.UnPack(reader)
	if err != nil {
		return err
	}
	r.CompressedType, err = serialize.UnPackByte(reader)
	if err != nil {
		return err
	}
	err = r.RawBlockData.UnPack(reader)
	if err != nil {
		return err
	}
	return nil
}

type RawBlockManager struct {
	RawBlockFileName string
	RawBlockFileTag  uint32
	RawBlockFileObj  *os.File
	BlockHeight      uint32
	BlockFileEndPos  uint32
	rawBlockMutex    *sync.RWMutex
}

func (r *RawBlockManager) Init(dataDir string, dataNamePrefix string, fileTag uint32) error {
	if r.rawBlockMutex == nil {
		r.rawBlockMutex = new(sync.RWMutex)
	}
	r.rawBlockMutex.Lock()
	var err error
	rawBlockFileName := dataDir + "/" + dataNamePrefix + "." + strconv.Itoa(int(fileTag))
	r.RawBlockFileObj, err = os.OpenFile(rawBlockFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend)
	if err != nil {
		r.rawBlockMutex.Unlock()
		return err
	}
	r.RawBlockFileName = rawBlockFileName
	r.RawBlockFileTag = fileTag
	r.BlockHeight = 0
	r.BlockFileEndPos = 0
	r.rawBlockMutex.Unlock()
	return nil
}

func (r *RawBlockManager) AddNewBlock(newBlock *RawBlock) error {
	r.rawBlockMutex.Lock()
	err := newBlock.Pack(r.RawBlockFileObj)
	if err != nil {
		r.rawBlockMutex.Unlock()
		return err
	}
	r.BlockFileEndPos = r.BlockFileEndPos + newBlock.PackSize()
	r.rawBlockMutex.Unlock()
	return nil
}
