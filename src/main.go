package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"io"
	"os"
	"rawblock"
	"strconv"
	"strings"
)

var goroutineMgr *goroutine_mgr.GoroutineManager
var blockIndexMgr *rawblock.RawBlockIndexManager
var latestRawBlockMgr *rawblock.RawBlockManager

var dataDir = "block_data"
var blockIndexName = "raw_block_index"
var rawBlockFilePrefix = "raw_block"

var quitFlag = false
var quitChan chan byte
var latestBlockHeight = uint32(0)

var rpcUrl = "http://test:test@192.168.1.107:30011"

func getLatestRawBlockTag() (uint32, error) {
	var tag uint32 = 0
	for {
		var tagNext = tag + 1
		rawBlockFileNext := dataDir + "/" + rawBlockFilePrefix + "." + strconv.Itoa(int(tagNext))
		_, err := os.Stat(rawBlockFileNext)
		if err != nil {
			break
		}
		tag = tagNext
	}
	return tag, nil
}

func appInit() error {
	// init quit channel
	quitChan = make(chan byte)

	var err error = nil
	// init goroutine manager
	goroutineMgr = new(goroutine_mgr.GoroutineManager)
	goroutineMgr.Initialise("MainGoroutineManager")

	// init raw block index manager
	blockIndexMgr = new(rawblock.RawBlockIndexManager)
	err = blockIndexMgr.Init(dataDir, blockIndexName)
	if err != nil {
		return err
	}

	// find latest raw block tag
	tag, err := getLatestRawBlockTag()
	if err != nil {
		return err
	}

	// init latest raw block manager
	latestRawBlockMgr = new(rawblock.RawBlockManager)
	err = latestRawBlockMgr.Init(dataDir, rawBlockFilePrefix, tag)
	if err != nil {
		return err
	}
	indexInfo, err := os.Stat(dataDir + "/" + blockIndexName)
	if err != nil {
		return err
	}
	latestRawBlockInfo, err := os.Stat(dataDir + "/" + rawBlockFilePrefix + "." + strconv.Itoa(int(tag)))
	if err != nil {
		return err
	}

	// verify raw block and raw block index
	if indexInfo.Size() != 0 {
		indexSize := indexInfo.Size()
		if indexSize%rawblock.RawBlockIndexSize != 0 {
			return errors.New("invalid raw block index size")
		}
		// the latest block index
		IndexMgr := new(rawblock.RawBlockIndexManager)
		err = IndexMgr.Init(dataDir, blockIndexName)
		if err != nil {
			return err
		}
		_, err = IndexMgr.BlockIndexFileObj.Seek(-1*rawblock.RawBlockIndexSize, io.SeekEnd)
		if err != nil {
			return err
		}
		ptrBlockIndex := new(rawblock.RawBlockIndex)
		err := ptrBlockIndex.UnPack(IndexMgr.BlockIndexFileObj)
		if err != nil {
			return err
		}

		latestBlockHeight = ptrBlockIndex.BlockHeight
		if ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag {
			return errors.New("ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag")
		}
		if ptrBlockIndex.BlockFileEndPos != uint32(latestRawBlockInfo.Size()) {
			return errors.New("ptrBlockIndex.BlockFileEndPos != uint32(latestRawBlockInfo.Size())")
		}
	} else {
		latestBlockHeight = uint32(0)
		if latestRawBlockMgr.RawBlockFileTag != 0 || latestRawBlockInfo.Size() != 0 {
			return errors.New("index is not match from raw block, need to rebuild index")
		}
	}
	return nil
}

func appRun() error {
	startSignalHandler()
	startGatherBlock()
	return nil
}

func appCmd() error {
	var stdinReader *bufio.Reader
	stdinReader = bufio.NewReader(os.Stdin)
	var stdoutWriter *bufio.Writer
	stdoutWriter = bufio.NewWriter(os.Stdout)
	for {
		_, err := stdoutWriter.WriteString(">>>")
		if err != nil {
			return err
		}
		stdoutWriter.Flush()
		strLine, err := stdinReader.ReadString('\n')
		if err != nil {
			return err
		}
		strLine = strings.Trim(strLine, "\x0a")
		strLine = strings.Trim(strLine, "\x0d")
		strLine = strings.TrimLeft(strLine, " ")
		strLine = strings.TrimRight(strLine, " ")

		if strLine == "" {
		} else if strLine == "stop" || strLine == "quit" || strLine == "exit" {
			quitFlag = true
			break
		} else if strLine == "getblockcount" {
			fmt.Println(latestBlockHeight)
		} else {
			fmt.Println("not support command: ", strLine)
		}
	}
	<-quitChan
	return nil
}

func rebuildIndex() error {
	var err error
	var tag uint32
	// remove block index
	err = os.Remove(dataDir + "/" + blockIndexName)
	if err != nil {
		return err
	}

	// init raw block index manager
	indexMgr := new(rawblock.RawBlockIndexManager)
	err = indexMgr.Init(dataDir, blockIndexName)
	if err != nil {
		return err
	}

	// find latest raw block tag
	tag, err = getLatestRawBlockTag()
	if err != nil {
		return err
	}

	for i := 0; i <= int(tag); i++ {
		// raw block manager
		rawBlockMgr := new(rawblock.RawBlockManager)
		err = rawBlockMgr.Init(dataDir, rawBlockFilePrefix, tag)
		if err != nil {
			return err
		}
		rawBlockInfo, err := os.Stat(dataDir + "/" + rawBlockFilePrefix + "." + strconv.Itoa(int(tag)))
		if err != nil {
			return err
		}

		var offSetBefore uint32 = 0
		var offSetAfter uint32 = 0
		for {
			_, err = rawBlockMgr.RawBlockFileObj.Seek(int64(offSetBefore), io.SeekStart)
			if err != nil {
				return err
			}
			ptrRawBlock := new(rawblock.RawBlock)
			err := ptrRawBlock.UnPack(rawBlockMgr.RawBlockFileObj)
			if err != nil {
				return err
			}
			offSetAfter = offSetBefore + ptrRawBlock.PackSize()

			// add new block index
			blockIndexNew := new(rawblock.RawBlockIndex)
			blockIndexNew.BlockHeight = ptrRawBlock.BlockHeight
			blockIndexNew.BlockHash = ptrRawBlock.BlockHash
			blockIndexNew.RawBlockSize = uint32(len(ptrRawBlock.RawBlockData.GetData()))
			blockIndexNew.RawBlockFileTag = tag
			blockIndexNew.BlockFileStartPos = offSetBefore
			blockIndexNew.BlockFileEndPos = offSetAfter
			err = indexMgr.AddNewBlockIndex(blockIndexNew)
			if err != nil {
				return err
			}

			if offSetAfter == uint32(rawBlockInfo.Size()) {
				// reach the end of the raw block file
				break
			}
			offSetBefore = offSetAfter
		}
	}
	fmt.Println("rebuild index has been finished")

	return nil
}

func main() {
	var err error
	reindex := flag.Bool("reindex", false, "rebuild index")
	flag.Parse()
	// rebuild index
	if *reindex {
		err = rebuildIndex()
		if err != nil {
			fmt.Println(err)
		}
		return
	}

	err = appInit()
	if err != nil {
		fmt.Println(err)
		return
	}
	err = appRun()
	if err != nil {
		fmt.Println(err)
		return
	}
	err = appCmd()
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}
