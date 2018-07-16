package rawblock

import (
	"os"
	"sync"
	"io"
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"github.com/mutalisk999/bitcoin-lib/src/serialize"
	"github.com/mutalisk999/bitcoin-lib/src/blob"
	"strconv"
)

const (
	RawBlockIndexSize = 4 + 32 + 4 + 4 + 4
)

type RawBlockIndex struct {
	BlockHeight        uint32
	BlockHash          bigint.Uint256
	RawBlockSize       uint32
	RawBlockFileTag    uint32
	RawBlockFileOffset uint32
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
	err = serialize.PackUint32(writer, r.RawBlockFileOffset)
	if err != nil {
		return err
	}
	return nil
}

func (r* RawBlockIndex) UnPack(reader io.Reader) error {
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
	r.RawBlockFileOffset, err = serialize.UnPackUint32(reader)
	if err != nil {
		return err
	}
	return nil
}

type RawBlockIndexManager struct {
	BlockIndexFileName string
	BlockIndexFileObj  *os.File
	blockIndexMutex    *sync.Mutex
}

func (r* RawBlockIndexManager) Init(indexDir string, indexName string) error {
	if r.blockIndexMutex == nil {
		r.blockIndexMutex = new(sync.Mutex)
	}
	r.blockIndexMutex.Lock()
	var err error
	r.BlockIndexFileObj, err = os.OpenFile(indexDir + "/" + indexName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if err != nil {
		r.blockIndexMutex.Unlock()
		return err
	}
	r.BlockIndexFileName = indexName
	r.blockIndexMutex.Unlock()
	return nil
}

func (r* RawBlockIndexManager) GetLatestIndex() (error, *RawBlockIndex) {
	r.blockIndexMutex.Lock()
	r.BlockIndexFileObj.Seek(-1 * RawBlockIndexSize, io.SeekEnd)
	ptrBlockIndex := new(RawBlockIndex)
	err := ptrBlockIndex.UnPack(r.BlockIndexFileObj)
	if err != nil {
		r.blockIndexMutex.Unlock()
		return err, nil
	}
	r.blockIndexMutex.Unlock()
	return nil, ptrBlockIndex
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
	RawBlockFileTag uint32
	RawBlockFileObj *os.File
	rawBlockMutex   *sync.Mutex
}

func (r* RawBlockManager) Init(dataDir string, dataNamePrefix string, fileTag uint32) error {
	if r.rawBlockMutex == nil {
		r.rawBlockMutex = new(sync.Mutex)
	}
	r.rawBlockMutex.Lock()
	var err error
	rawBlockFileName := dataDir + "/" + dataNamePrefix + "." + strconv.Itoa(int(fileTag))
	r.RawBlockFileObj, err = os.OpenFile(rawBlockFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if err != nil {
		r.rawBlockMutex.Unlock()
		return err
	}
	r.RawBlockFileTag = fileTag
	r.rawBlockMutex.Unlock()
	return nil
}

func (r* RawBlockManager) GetRawBlock(offset uint32) (error, *RawBlock) {
	r.rawBlockMutex.Lock()
	r.RawBlockFileObj.Seek(int64(offset), io.SeekStart)
	ptrRawBlock := new(RawBlock)
	err := ptrRawBlock.UnPack(r.RawBlockFileObj)
	if err != nil {
		r.rawBlockMutex.Unlock()
		return err, nil
	}
	r.rawBlockMutex.Unlock()
	return nil, ptrRawBlock
}
