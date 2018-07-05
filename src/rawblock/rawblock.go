package rawblock

import "github.com/mutalisk999/bitcoin-lib/src/bigint"

type RawBlockIndex struct {
	BlockHeight		uint
    BlockHash       bigint.Uint256
    RawBlockSize    uint
    RawBlockFileIndex	uint
    RawBlockFileOffset	uint
}

type RawBlockIndexManager struct {

}

type RawBlock struct {
	BlockHeight     uint
	BlockHash		bigint.Uint256
	RawBlockSize	uint
	RawBlockData	[]byte
}

type RawBlockManager struct {

}
