package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"io"
	"os"
	"strconv"
	"strings"
)

var goroutineMgr *goroutine_mgr.GoroutineManager
var blockIndexMgr *RawBlockIndexManager
var latestRawBlockMgr *RawBlockManager

var config Config

var quitFlag = false
var quitChan chan byte

var heightToHashMap = make(map[uint32]string)
var hashToHeightMap = make(map[string]uint32)

func getLatestRawBlockTag() (uint32, error) {
	var tag uint32 = 0
	for {
		var tagNext = tag + 1
		rawBlockFileNext := config.DataConfig.DataDir + "/" + config.DataConfig.RawBlockFilePrefix + "." + strconv.Itoa(int(tagNext))
		_, err := os.Stat(rawBlockFileNext)
		if err != nil {
			break
		}
		tag = tagNext
	}
	return tag, nil
}

func appInit() error {
	var err error = nil
	// init quit channel
	quitChan = make(chan byte)

	// init goroutine manager
	goroutineMgr = new(goroutine_mgr.GoroutineManager)
	goroutineMgr.Initialise("MainGoroutineManager")

	// init raw block index manager
	blockIndexMgr = new(RawBlockIndexManager)
	err = blockIndexMgr.Init(config.DataConfig.DataDir, config.DataConfig.BlockIndexName)
	if err != nil {
		return err
	}

	// find latest raw block tag
	tag, err := getLatestRawBlockTag()
	if err != nil {
		return err
	}

	// init latest raw block manager
	latestRawBlockMgr = new(RawBlockManager)
	err = latestRawBlockMgr.Init(config.DataConfig.DataDir, config.DataConfig.RawBlockFilePrefix, tag)
	if err != nil {
		return err
	}
	indexInfo, err := os.Stat(config.DataConfig.DataDir + "/" + config.DataConfig.BlockIndexName)
	if err != nil {
		return err
	}
	latestRawBlockInfo, err := os.Stat(config.DataConfig.DataDir + "/" + config.DataConfig.RawBlockFilePrefix + "." + strconv.Itoa(int(tag)))
	if err != nil {
		return err
	}

	// verify raw block and raw block index
	if indexInfo.Size() != 0 {
		indexSize := indexInfo.Size()
		if indexSize%RawBlockIndexSize != 0 {
			return errors.New("invalid raw block index size")
		}
		// the latest block index
		IndexMgr := new(RawBlockIndexManager)
		err = IndexMgr.Init(config.DataConfig.DataDir, config.DataConfig.BlockIndexName)
		if err != nil {
			return err
		}
		_, err = IndexMgr.BlockIndexFileObj.Seek(-1*RawBlockIndexSize, io.SeekEnd)
		if err != nil {
			return err
		}
		ptrBlockIndex := new(RawBlockIndex)
		err := ptrBlockIndex.UnPack(IndexMgr.BlockIndexFileObj)
		if err != nil {
			return err
		}

		latestRawBlockMgr.BlockHeight = ptrBlockIndex.BlockHeight
		latestRawBlockMgr.BlockFileEndPos = ptrBlockIndex.BlockFileEndPos
		if ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag {
			return errors.New("ptrBlockIndex.RawBlockFileTag != latestRawBlockMgr.RawBlockFileTag")
		}
		if ptrBlockIndex.BlockFileEndPos != uint32(latestRawBlockInfo.Size()) {
			fmt.Println(ptrBlockIndex.BlockFileEndPos, latestRawBlockInfo.Size())
			return errors.New("ptrBlockIndex.BlockFileEndPos != uint32(latestRawBlockInfo.Size())")
		}

		// load raw_block_index to map
		_, err = IndexMgr.BlockIndexFileObj.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		for i := 1; i <= int(latestRawBlockMgr.BlockHeight); i++ {
			ptrBlockIndex := new(RawBlockIndex)
			err := ptrBlockIndex.UnPack(IndexMgr.BlockIndexFileObj)
			if err != nil {
				return err
			}
			heightToHashMap[ptrBlockIndex.BlockHeight] = ptrBlockIndex.BlockHash.GetHex()
			hashToHeightMap[ptrBlockIndex.BlockHash.GetHex()] = ptrBlockIndex.BlockHeight
		}
	} else {
		latestRawBlockMgr.BlockHeight = uint32(0)
		latestRawBlockMgr.BlockFileEndPos = uint32(0)
		if latestRawBlockMgr.RawBlockFileTag != 0 || latestRawBlockInfo.Size() != 0 {
			return errors.New("index is not match from raw block, need to rebuild index")
		}
	}
	return nil
}

func appRun() error {
	startSignalHandler()
	startRpcServer()
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
			quitFlag = true
			break
		}
		stdoutWriter.Flush()
		strLine, err := stdinReader.ReadString('\n')
		if err != nil {
			quitFlag = true
			break
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
			fmt.Println(latestRawBlockMgr.BlockHeight)
		} else if strLine == "goroutinestatus" {
			goroutineMgr.GoroutineDump()
		} else {
			fmt.Println("not support command: ", strLine)
		}
	}
	<-quitChan

	// sync and close
	blockIndexMgr.BlockIndexFileObj.Close()
	latestRawBlockMgr.RawBlockFileObj.Close()

	return nil
}

func rebuildIndex() error {
	var err error
	var tag uint32
	// remove block index if index exist
	_, err = os.Stat(config.DataConfig.DataDir + "/" + config.DataConfig.BlockIndexName)
	if err == nil {
		err = os.Remove(config.DataConfig.DataDir + "/" + config.DataConfig.BlockIndexName)
		if err != nil {
			return err
		}
	}

	// init raw block index manager
	indexMgr := new(RawBlockIndexManager)
	err = indexMgr.Init(config.DataConfig.DataDir, config.DataConfig.BlockIndexName)
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
		rawBlockMgr := new(RawBlockManager)
		err = rawBlockMgr.Init(config.DataConfig.DataDir, config.DataConfig.RawBlockFilePrefix, uint32(i))
		if err != nil {
			return err
		}
		rawBlockInfo, err := os.Stat(config.DataConfig.DataDir + "/" + config.DataConfig.RawBlockFilePrefix + "." + strconv.Itoa(i))
		if err != nil {
			return err
		}

		var offSetBefore uint32 = 0
		var offSetAfter uint32 = 0
		for {
			ptrRawBlock := new(RawBlock)
			err := ptrRawBlock.UnPack(rawBlockMgr.RawBlockFileObj)
			if err != nil {
				return err
			}
			offSetAfter = offSetBefore + ptrRawBlock.PackSize()

			// add new block index
			blockIndexNew := new(RawBlockIndex)
			blockIndexNew.BlockHeight = ptrRawBlock.BlockHeight
			blockIndexNew.BlockHash = ptrRawBlock.BlockHash
			blockIndexNew.RawBlockSize = uint32(len(ptrRawBlock.RawBlockData.GetData()))
			blockIndexNew.RawBlockFileTag = uint32(i)
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
		var completeRate float64 = float64(i+1) * float64(100) / float64(tag+1)
		fmt.Println("reindex", config.DataConfig.RawBlockFilePrefix+"."+strconv.Itoa(i), "ok...", strconv.FormatFloat(completeRate, 'f', 2, 64)+"%")
	}
	fmt.Println("rebuild index has been finished")

	return nil
}

func lockDataDir() error {
	lockFile, err := os.OpenFile(config.DataConfig.DataDir+"/.lock", os.O_CREATE|os.O_EXCL, os.ModePerm)
	if err != nil {
		return err
	}
	lockFile.Close()
	return nil
}

func unLockDataDir() error {
	err := os.Remove(config.DataConfig.DataDir + "/.lock")
	if err != nil {
		return err
	}
	return nil
}

func main() {
	var err error
	reindex := flag.Bool("reindex", false, "rebuild index")
	flag.Parse()

	// init config
	jsonParser := new(JsonStruct)
	err = jsonParser.Load("config.json", &config)
	if err != nil {
		return
	}

	err = lockDataDir()
	if err != nil {
		fmt.Println(config.DataConfig.DataDir, "cannot obtain a lock on data directory "+config.DataConfig.DataDir+", probably collector is already running")
		return
	}

	// rebuild index
	if *reindex {
		err = rebuildIndex()
		if err != nil {
			fmt.Println("rebuildIndex", err)
		}
		unLockDataDir()
		return
	}

	err = appInit()
	if err != nil {
		fmt.Println("appInit", err)
		unLockDataDir()
		return
	}
	err = appRun()
	if err != nil {
		fmt.Println("appRun", err)
		unLockDataDir()
		return
	}
	err = appCmd()
	if err != nil {
		fmt.Println("appCmd", err)
		unLockDataDir()
		return
	}
	unLockDataDir()
	return
}
