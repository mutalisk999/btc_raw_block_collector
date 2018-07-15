package main

import (
	"rawblock"
	"os"
	"errors"
)

var blockIndexMgr *rawblock.RawBlockIndexManager
var latestRawBlockMgr *rawblock.RawBlockManager

var dataDir = "block_data"
var blockIndexName = "raw_block_index"
var rawBlockFilePrefix = "raw_block"


func appInit() error {
	var err error = nil
	// init raw block index manager
	blockIndexMgr = new(rawblock.RawBlockIndexManager)
	err = blockIndexMgr.Init(dataDir, blockIndexName)
	if err != nil {
		return err
	}

	// find latest raw block tag
	var tag uint = 0
	for {
		var tagNext = tag + 1
		rawBlockFileNext := dataDir + "/" + rawBlockFilePrefix +
			"." + string(tagNext)
		_, err := os.Stat(rawBlockFileNext)
		if err != nil {
			break
		}
		tag = tagNext
	}

	// init latest raw block manager
	latestRawBlockMgr = new(rawblock.RawBlockManager)
	err = latestRawBlockMgr.Init(dataDir, rawBlockFilePrefix, tag)
	if err != nil {
		return err
	}

	// verify raw block index
	indexInfo, err := blockIndexMgr.BlockIndexFileObj.Stat()
	if err != nil {
		return err
	}
	indexSize := indexInfo.Size()
	if indexSize % 48 != 0 {
		return errors.New("invalid raw block index size")
	}

	// verify block file
	if indexSize != 0 {

	} else {

	}

	return nil
}

func appRun() error{
	return nil
}

func main() {
	var err error
	err = appInit()
	if err != nil {
		return
	}
	appRun()
}
