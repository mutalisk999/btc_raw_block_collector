package main

import (
	"encoding/json"
	"io/ioutil"
)

type DataConfig struct {
	DataDir            string `json:"dataDir"`
	BlockIndexName     string `json:"blockIndexName"`
	RawBlockFilePrefix string `json:"rawBlockFilePrefix"`
}

type RpcClientConfig struct {
	RpcReqUrl string `json:"rpcReqUrl"`
}

type RpcServerConfig struct {
	RpcListenEndPoint string `json:"rpcListenEndPoint"`
}

type Config struct {
	DataConfig      DataConfig      `json:"dataConfig"`
	RpcClientConfig RpcClientConfig `json:"rpcClientConfig"`
	RpcServerConfig RpcServerConfig `json:"rpcServerConfig"`
}

type JsonStruct struct {
}

func (j *JsonStruct) Load(configFile string, config interface{}) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		return err
	}
	err = json.Unmarshal(data, config)
	if err != nil {
		return err
	}
	return nil
}
