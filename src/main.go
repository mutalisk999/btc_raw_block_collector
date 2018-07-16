package main

import (
	"rawblock"
	"os"
	"errors"
	"fmt"
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
	var tag uint32 = 0
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
	indexInfo, err := blockIndexMgr.BlockIndexFileObj.Stat()
	if err != nil {
		return err
	}
	latestRawBlockInfo, err := latestRawBlockMgr.RawBlockFileObj.Stat()
	if err != nil {
		return err
	}

	// verify raw block and raw block index
	if indexInfo.Size() != 0 {
		indexSize := indexInfo.Size()
		if indexSize % rawblock.RawBlockIndexSize != 0 {
			return errors.New("invalid raw block index size")
		}
		err, ptrBlockIndex := blockIndexMgr.GetLatestIndex()
		if err != nil {
			return err
		}
		if ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag {
			return errors.New("ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag")
		}

	} else {
		if latestRawBlockMgr.RawBlockFileTag != 0 || latestRawBlockInfo.Size() != 0 {
			return errors.New("index is not match from raw block, need to rebuild index")
		}
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
		fmt.Println(err)
	}
	appRun()
}
