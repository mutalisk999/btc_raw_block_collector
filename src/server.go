package main

import (
	"errors"
	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
	"github.com/mutalisk999/go-lib/src/sched/goroutine_mgr"
	"io"
	"net/http"
)

type Service struct {
}

func (s *Service) GetBlockCount(r *http.Request, args *interface{}, reply *uint32) error {
	*reply = latestRawBlockMgr.BlockHeight
	return nil
}

func (s *Service) GetBlockHash(r *http.Request, args *uint32, reply *string) error {
	blockHash, ok := heightToHashMap[*args]
	if !ok {
		return errors.New("block height not found")
	}
	*reply = blockHash
	return nil
}

func (s *Service) GetBlockHeight(r *http.Request, args *string, reply *uint32) error {
	blockHeight, ok := hashToHeightMap[*args]
	if !ok {
		return errors.New("block hash not found")
	}
	*reply = blockHeight
	return nil
}

func (s *Service) GetRawBlock(r *http.Request, args *string, reply *string) error {
	blockHeight, ok := hashToHeightMap[*args]
	if !ok {
		return errors.New("block hash not found")
	}

	var err error
	IndexMgr := new(RawBlockIndexManager)
	err = IndexMgr.Init(config.DataConfig.DataDir, config.DataConfig.BlockIndexName)
	if err != nil {
		return err
	}

	defer IndexMgr.BlockIndexFileObj.Close()
	_, err = IndexMgr.BlockIndexFileObj.Seek(int64(blockHeight-1)*RawBlockIndexSize, io.SeekStart)
	if err != nil {
		return err
	}
	ptrBlockIndex := new(RawBlockIndex)
	err = ptrBlockIndex.UnPack(IndexMgr.BlockIndexFileObj)
	if err != nil {
		return err
	}

	rawBlockMgr := new(RawBlockManager)
	err = rawBlockMgr.Init(config.DataConfig.DataDir, config.DataConfig.RawBlockFilePrefix, ptrBlockIndex.RawBlockFileTag)
	if err != nil {
		return err
	}

	defer rawBlockMgr.RawBlockFileObj.Close()
	_, err = rawBlockMgr.RawBlockFileObj.Seek(int64(ptrBlockIndex.BlockFileStartPos), io.SeekStart)
	if err != nil {
		return err
	}
	ptrRawBlock := new(RawBlock)
	err = ptrRawBlock.UnPack(rawBlockMgr.RawBlockFileObj)
	if err != nil {
		return err
	}

	*reply = ptrRawBlock.RawBlockData.GetHex()
	return nil
}

func rpcServer(goroutine goroutine_mgr.Goroutine, args ...interface{}) {
	defer goroutine.OnQuit()
	rpcServer := rpc.NewServer()
	rpcServer.RegisterCodec(json.NewCodec(), "application/json")
	rpcServer.RegisterCodec(json.NewCodec(), "application/json;charset=UTF-8")

	rpcService := new(Service)
	rpcServer.RegisterService(rpcService, "")

	urlRouter := mux.NewRouter()
	urlRouter.Handle("/", rpcServer)
	http.ListenAndServe(config.RpcServerConfig.RpcListenEndPoint, urlRouter)
}

func startRpcServer() uint64 {
	return goroutineMgr.GoroutineCreatePn("rpcserver", rpcServer, nil)
}
