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
	BlockIndexFileObj  os.File
	blockIndexMutex    sync.Mutex
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
	RawBlockFileObj os.File
	rawBlockMutex   sync.Mutex
}
