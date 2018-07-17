package main

import (
	"fmt"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"github.com/ybbus/jsonrpc"
	"rawblock"
	"time"
)

func doHttpJsonRpcCall(method string, args ...interface{}) (*jsonrpc.RPCResponse, error) {
	rpcClient := jsonrpc.NewClient(rpcUrl)
	rpcResponse, err := rpcClient.Call(method, args)
	if err != nil {
		return nil, err
	}
	return rpcResponse, nil
}

func getBlockCountRpc() (uint32, error) {
	rpcResponse, err := doHttpJsonRpcCall("getblockcount")
	if err != nil {
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
		return 0, err
	}
	blockCount, err := rpcResponse.GetInt()
	if err != nil {
		fmt.Println("Get blockCount from rpcResponse Failed: ", err)
		return 0, err
	}
	return uint32(blockCount), nil
}

func getBlockHashRpc(blockHeight uint32) (string, error) {
	rpcResponse, err := doHttpJsonRpcCall("getblockhash", blockHeight)
	if err != nil {
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
		return "", err
	}
	blockHash, err := rpcResponse.GetString()
	if err != nil {
		fmt.Println("Get blockHash from rpcResponse Failed: ", err)
		return "", err
	}
	return blockHash, nil
}

func getRawBlock(blockHash string) (string, error) {
	rpcResponse, err := doHttpJsonRpcCall("getblock", blockHash, 0)
	if err != nil {
		fmt.Println("doHttpJsonRpcCall Failed: ", err)
		return "", err
	}
	rawBlockHex, err := rpcResponse.GetString()
	if err != nil {
		fmt.Println("Get rawBlockHex from rpcResponse Failed: ", err)
		return "", err
	}
	return rawBlockHex, nil
}

func doGatherBlock(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	for {
		if quitFlag {
			break
		}
		blockCount, err := getBlockCountRpc()
		if err != nil {
			break
		}
		if latestBlockHeight >= blockCount {
			time.Sleep(5 * 1000 * 1000 * 1000)
		} else {
			for {
				if quitFlag {
					break
				}
				latestBlockHeight += 1
				blockHash, err := getBlockHashRpc(latestBlockHeight)
				if err != nil {
					break
				}
				rawBlockData, err := getRawBlock(blockHash)
				if err != nil {
					break
				}

				// add new block data
				rawBlockNew := new(rawblock.RawBlock)
				rawBlockNew.BlockHeight = latestBlockHeight
				rawBlockNew.BlockHash.SetHex(blockHash)
				rawBlockNew.CompressedType = 0
				rawBlockNew.RawBlockData.SetHex(rawBlockData)
				blockFileInfo, err := latestRawBlockMgr.RawBlockFileObj.Stat()
				if err != nil {
					break
				}
				if blockFileInfo.Size() > 1*1024*1024*1024 {
					newRawBlockMgr := new(rawblock.RawBlockManager)
					newRawBlockMgr.Init(dataDir, rawBlockFilePrefix, latestRawBlockMgr.RawBlockFileTag+1)
					latestRawBlockMgr.RawBlockFileObj.Close()
					latestRawBlockMgr = newRawBlockMgr
					blockFileInfo, err = latestRawBlockMgr.RawBlockFileObj.Stat()
				}
				latestRawBlockMgr.AddNewBlock(rawBlockNew)

				// add new block index
				blockIndexNew := new(rawblock.RawBlockIndex)
				blockIndexNew.BlockHeight = latestBlockHeight
				blockIndexNew.BlockHash.SetHex(blockHash)
				blockIndexNew.RawBlockSize = uint32(len(rawBlockData) / 2)
				blockIndexNew.RawBlockFileTag = latestRawBlockMgr.RawBlockFileTag
				blockIndexNew.RawBlockFileOffset = uint32(blockFileInfo.Size())
				blockIndexMgr.AddNewBlockIndex(blockIndexNew)
			}
		}
	}
	quitChan <- 0x0
}

func startGatherBlock() uint64 {
	return goroutineMgr.GoroutineCreatePn("gatherblock", doGatherBlock, nil)
}
