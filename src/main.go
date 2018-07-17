package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"io"
	"os"
	"rawblock"
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
		if indexSize%rawblock.RawBlockIndexSize != 0 {
			return errors.New("invalid raw block index size")
		}
		// the latest block index
		err, ptrBlockIndex := blockIndexMgr.GetLatestIndex()
		if err != nil {
			return err
		}
		latestBlockHeight = ptrBlockIndex.BlockHeight
		if ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag {
			return errors.New("ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag")
		}
		// skip the latest raw block
		err, _ = latestRawBlockMgr.GetRawBlock(ptrBlockIndex.RawBlockFileOffset)
		if err != nil {
			return err
		}
		curOffSet, err := latestRawBlockMgr.RawBlockFileObj.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		endOffSet, err := latestRawBlockMgr.RawBlockFileObj.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
		// if reach the end of the file or not
		if curOffSet != endOffSet {
			return errors.New("curOffSet != endOffSet")
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

func main() {
	var err error
	err = appInit()
	if err != nil {
		fmt.Println(err)
	}
	err = appRun()
	if err != nil {
		fmt.Println(err)
	}
	err = appCmd()
	if err != nil {
		fmt.Println(err)
	}
}
