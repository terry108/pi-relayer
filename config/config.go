package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/terry108/pi-relayer/log"
)

const (
	BSC_MONITOR_INTERVAL = time.Second
	PI_MONITOR_INTERVAL  = time.Second

	BSC_USEFUL_BLOCK_NUM     = 3
	PI_USEFUL_BLOCK_NUM      = 1
	DEFAULT_CONFIG_FILE_NAME = "./config.json"
	Version                  = "1.0"
)

type ServiceConfig struct {
	PiConfig        *PiConfig
	BSCConfig       *BSCConfig
	CosmosConfig    *CosmosConfig
	BoltDbPath      string
	RoutineNum      int64
	TargetContracts []map[string]map[string][]uint64
}

type BSCConfig struct {
	SideChainId         uint64
	RestURL             string
	ECCMContractAddress string
	ECCDContractAddress string
	KeyStorePath        string
	KeyStorePwdSet      map[string]string
	BlockConfig         uint64
	HeadersPerBatch     int
}

type PiConfig struct {
	RestURL string
}

type CosmosConfig struct {
	CosmosRpcAddr        string `json:"cosmos_rpc_addr"`
	CosmosWallet         string `json:"cosmos_wallet"`
	CosmosWalletPwd      string `json:"cosmos_wallet_pwd"`
	CosmosStartHeight    int64  `json:"cosmos_start_height"`
	CosmosListenInterval int    `json:"cosmos_listen_interval"`
	CosmosChainId        string `json:"cosmos_chain_id"`
	CosmosGasPrice       string `json:"cosmos_gas_price"`
	CosmosGas            uint64 `json:"cosmos_gas"`
}

func ReadFile(fileName string) ([]byte, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("ReadFile: open file %s error %s", fileName, err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Errorf("ReadFile: File %s close error %s", fileName, err)
		}
	}()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("ReadFile: ioutil.ReadAll %s error %s", fileName, err)
	}
	return data, nil
}

func NewServiceConfig(configFilePath string) *ServiceConfig {
	fileContent, err := ReadFile(configFilePath)
	if err != nil {
		log.Errorf("NewServiceConfig: failed, err: %s", err)
		return nil
	}
	servConfig := &ServiceConfig{}
	err = json.Unmarshal(fileContent, servConfig)
	if err != nil {
		log.Errorf("NewServiceConfig: failed, err: %s", err)
		return nil
	}

	for k, v := range servConfig.BSCConfig.KeyStorePwdSet {
		delete(servConfig.BSCConfig.KeyStorePwdSet, k)
		servConfig.BSCConfig.KeyStorePwdSet[strings.ToLower(k)] = v
	}

	return servConfig
}
