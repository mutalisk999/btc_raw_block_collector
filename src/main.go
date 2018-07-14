package main

import "rawblock"

var blockIndexMgr *rawblock.RawBlockIndexManager
var latestRawBlockMgr *rawblock.RawBlockManager

func appRun() {
	dataDir := "block_data"
	// init raw block index manager
	blockIndexMgr = new(rawblock.RawBlockIndexManager)
	blockIndexMgr.Init(dataDir + "/" + "raw_block_index")

	// find latest raw block tag

	// init latest raw block manager
}

func main() {
	appRun()
}
