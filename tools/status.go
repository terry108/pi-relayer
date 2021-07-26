package tools

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tendermint/tendermint/libs/bytes"
	rpctypes "github.com/tendermint/tendermint/rpc/core/types"
	"github.com/terry108/pi-relayer/db"
	"github.com/terry108/pi-relayer/log"
)

func NewCosmosStatus() (*CosmosStatus, error) {
	m := &sync.Map{}
	_, err := RCtx.Db.LoadCosmosStatus(m)
	if err != nil {
		return nil, err
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return &CosmosStatus{
		Txs: m,
		Wg:  wg,
	}, nil
}

type CosmosStatus struct {
	Txs             *sync.Map // TODO: record a start time to test if it's not confirm for a long time, that could be tx lost
	Wg              *sync.WaitGroup
	IsBlocked       bool
	PolyEpochHeight uint32
}

func (s *CosmosStatus) AddTx(hash bytes.HexBytes, info *PolyInfo) error {
	pph := &db.PolyProofAndHeader{
		Txhash:      info.Tx.TxHash,
		Hdr:         info.Hdr,
		Proof:       info.Tx.Proof,
		CCID:        info.Tx.CCID,
		FromChainId: info.Tx.FromChainId,
	}
	if err := RCtx.Db.SetTxToCosmosStatus(hash, pph); err != nil {
		return err
	}
	s.Txs.Store(hash.String(), pph)
	return nil
}

func (s *CosmosStatus) DelTx(hash bytes.HexBytes) error {
	if err := RCtx.Db.DelTxInCosmosStatus(hash); err != nil {
		return err
	}
	s.Txs.Delete(hash.String())
	return nil
}

func (s *CosmosStatus) Len() int {
	var l int
	s.Txs.Range(func(key, value interface{}) bool {
		l++
		return true
	})
	return l
}

// TODO: 交易丢失了怎么办，会一直循环查找地！！
func (s *CosmosStatus) Check() {
	tick := time.NewTicker(time.Second)
	var resTx *rpctypes.ResultTx
	for range tick.C {
		kArr := make([]bytes.HexBytes, 0)
		vArr := make([]*db.PolyProofAndHeader, 0)
		s.Txs.Range(func(key, value interface{}) bool {
			k, _ := hex.DecodeString(key.(string))
			kArr = append(kArr, k)
			vArr = append(vArr, value.(*db.PolyProofAndHeader))
			return true
		})
		if s.IsBlocked && len(kArr) == 0 {
			s.IsBlocked = false
			s.Wg.Done()
		}
		for i, v := range kArr {
			resTx, _ = RCtx.CMRpcCli.Tx(v, false)
			if resTx == nil {
				continue
			}
			if resTx.Height > 0 {
				if resTx.TxResult.Code == 0 {
					log.Infof("[Cosmos Status] cosmso tx %s is confirmed on block (height: %d) and success. ",
						v.String(), resTx.Height)
				} else {
					if strings.Contains(resTx.TxResult.Log, CosmosTxNotInEpoch) {
						log.Debugf("[Cosmos Status] cosmso tx %s is failed and this proof %s need reprove. ",
							v.String(), vArr[i].Proof)
						if err := RCtx.Db.SetPolyTxReproving(vArr[i].Txhash, vArr[i].Proof, vArr[i].Hdr); err != nil {
							panic(err)
						}
					} else {
						if res, _ := RCtx.CMRpcCli.ABCIQuery(ProofPath, ccm.GetDoneTxKey(vArr[i].FromChainId, vArr[i].CCID)); res != nil && res.Response.GetValue() != nil {
							log.Infof("[Cosmos Status] this poly tx %s is already committed, "+
								"so delete it cosmos_txhash %s: (from_chain_id: %d, ccid: %s)",
								vArr[i].Txhash, v.String(), vArr[i].FromChainId, hex.EncodeToString(vArr[i].CCID))
						} else {
							log.Errorf("[Cosmos Status] cosmso tx %s is confirmed on block (height: %d) "+
								"and failed (Log: %s). ", v.String(), resTx.Height, resTx.TxResult.Log)
						}
					}
				}
				if err := s.DelTx(v); err != nil {
					panic(err)
				}
				if err := RCtx.Db.DelPolyTxReproving(vArr[i].Txhash); err != nil {
					panic(err)
				}
			}
		}
	}
}

func (s *CosmosStatus) Show() {
	tick := time.NewTicker(30 * time.Second)
	for range tick.C {
		str := "unconfirmed tx \n[\n%s\n] total %d tx is not confirmed"
		strs := make([]string, 0)
		l := 0
		s.Txs.Range(func(key, value interface{}) bool {
			strs = append(strs, fmt.Sprintf("( txhash: %s, poly_tx: %s )",
				key.(string), value.(*db.PolyProofAndHeader).Txhash))
			l++
			return true
		})
		if l > 0 {
			log.Infof("[Cosmos Status] %s ", fmt.Sprintf(str, strings.Join(strs, "\n"), l))
		}
	}
}
