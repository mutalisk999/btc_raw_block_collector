package rawblock

import (
	"github.com/mutalisk999/bitcoin-lib/src/bigint"
	"os"
	"sync"
)

type RawBlockIndex struct {
	BlockHeight        uint
	BlockHash          bigint.Uint256
	RawBlockSize       uint
	RawBlockFileTag    uint
	RawBlockFileOffset uint
}

type RawBlockIndexManager struct {
	BlockIndexFileName string
	BlockIndexFileObj  *os.File
	blockIndexMutex    *sync.Mutex
}

func (r* RawBlockIndexManager) Init(indexName string) error {
	if r.blockIndexMutex == nil {
		r.blockIndexMutex = new(sync.Mutex)
	}
	r.blockIndexMutex.Lock()
	var err error
	r.BlockIndexFileObj, err = os.OpenFile(indexName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if err != nil {
		r.blockIndexMutex.Unlock()
		return err
	}
	r.BlockIndexFileName = indexName
	r.blockIndexMutex.Unlock()
	return nil
}

type RawBlock struct {
	BlockHeight    uint
	BlockHash      bigint.Uint256
	CompressedType byte
	RawBlockSize   uint
	RawBlockData   []byte
}

type RawBlockManager struct {
	RawBlockFileTag uint
	RawBlockFileObj *os.File
	rawBlockMutex   *sync.Mutex
}

func (r* RawBlockManager) Init(dataNamePrefix string, fileTag uint) error {
	if r.rawBlockMutex == nil {
		r.rawBlockMutex = new(sync.Mutex)
	}
	r.rawBlockMutex.Lock()
	var err error
	rawBlockFileName := dataNamePrefix + "." + string(fileTag)
	r.RawBlockFileObj, err = os.OpenFile(rawBlockFileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModeAppend|os.ModePerm)
	if err != nil {
		r.rawBlockMutex.Unlock()
		return err
	}
	r.RawBlockFileTag = fileTag
	r.rawBlockMutex.Unlock()
	return nil
}

