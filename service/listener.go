package service

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/tendermint/tendermint/crypto/merkle"
	"github.com/tendermint/tendermint/rpc/client"
	"github.com/terry108/pi-relayer/context"
	"github.com/terry108/pi-relayer/log"
)

// Cosmos listen service implementation. Check the blocks of COSMOS from height
// `left` to height `right`, commit the cross-chain txs and headers to prove txs
// to chain Poly. It execute once per `ctx.Conf.CosmosListenInterval` sec. And update
// height `left` `right` after execution for next round. This function will run
// as a go-routine.
func CosmosListen() {
	left, tick, err := beforeCosmosListen()
	if err != nil {
		log.Fatalf("[ListenCosmos] failed to get start height of Cosmos: %v", err)
		panic(err)
	}
	log.Infof("[ListenCosmos] start listen Cosmos (start_height: %d, listen_interval: %d)", left+1,
		ctx.Conf.CosmosListenInterval)

	lastRight := left
	for {
		select {
		case <-tick.C:
			status, err := ctx.CMRpcCli.Status()
			switch {
			case err != nil:
				log.Errorf("[ListenCosmos] failed to get height of COSMOS, retry after %d sec: %v",
					ctx.Conf.CosmosListenInterval, err)
				continue
			case status.SyncInfo.LatestBlockHeight-1 <= lastRight:
				continue
			}
			right := status.SyncInfo.LatestBlockHeight - 1
			hdr, err := getCosmosHdr(right)
			if err != nil {
				log.Errorf("[ListenCosmos] failed to get %d header to get proof, retry after %d sec: %v",
					right, ctx.Conf.CosmosListenInterval, err)
				continue
			}
			if !bytes.Equal(hdr.Header.ValidatorsHash, hdr.Header.NextValidatorsHash) {
				log.Debugf("[ListenCosmos] header at %d is epoch switching point, so continue loop", hdr.Header.Height)
				lastRight = right
				continue
			}

			// let first element of infoArr be the info for epoch switching headers.
			infoArr := make([]*context.CosmosInfo, 1)
			infoArr[0] = &context.CosmosInfo{
				Type: context.TyHeader,
				Hdrs: make([]*cosmos.CosmosHeader, 0),
			}
			for h := left + 1; h <= right; h++ {
				infoArrTemp, err := checkCosmosHeight(h, hdr, infoArr, &right)
				if err != nil {
					// If error happen, we should check this height again.
					h--
					if strings.Contains(err.Error(), context.RightHeightUpdate) {
						// Can't get proof from height `right-1`, update right to the latest.
						log.Debugf("[ListenCosmos] %s", err.Error())
						continue
					}
					// some error happen, could be some network error or COSMOS full node error.
					log.Errorf("[ListenCosmos] failed to fetch info from COSMOS, retry after 10 sec: %v", err)
					context.SleepSecs(10)
					continue
				}
				infoArr = infoArrTemp
			}
			infoArr = reproveCosmosTx(infoArr, hdr)

			for i, v := range infoArr {
				if i == 0 && len(v.Hdrs) == 0 {
					continue
				}
				ctx.ToPoly <- v
			}
			cnt := 0
			for _, v := range infoArr {
				switch v.Type {
				case context.TyTx:
					cnt++
				}
			}
			if cnt > 0 {
				log.Debugf("[ListenCosmos] found %d cross chain tx to Poly [%d, %d]", cnt, left, right-1)
			}
			ctx.ToPoly <- &context.CosmosInfo{
				Type:   context.TyUpdateHeight,
				Height: right,
			}
			lastRight = right
			left = right
		}
	}
}

// Prepare start height and ticker when init service
func beforeCosmosListen() (int64, *time.Ticker, error) {
	val, err := ctx.Poly.GetStorage(utils.HeaderSyncContractAddress.ToHexString(),
		append([]byte(mhcomm.EPOCH_SWITCH), utils.GetUint64Bytes(ctx.Conf.SideChainId)...))
	if err != nil {
		return 0, nil, err
	}
	info := &cosmos.CosmosEpochSwitchInfo{}
	if err = info.Deserialization(common.NewZeroCopySource(val)); err != nil {
		return 0, nil, err
	}
	currHeight := info.Height
	if currHeight > 1 {
		currHeight--
	}
	log.Debugf("beforeCosmosListen, ( cosmos height on Poly: %d )", currHeight)
	if dbh := ctx.Db.GetCosmosHeight(); dbh > currHeight {
		log.Debugf("beforeCosmosListen, ( cosmos height in DB: %d )", dbh)
		currHeight = dbh
	}
	if ctx.Conf.CosmosStartHeight != 0 {
		currHeight = ctx.Conf.CosmosStartHeight
	}
	return currHeight, time.NewTicker(time.Duration(ctx.Conf.CosmosListenInterval) * time.Second), nil
}

// Fetch header at h and check tx at h-1.
//
// Put header to `hdrArr` and txs to `txArr`. Get proof from height `heightToGetProof`.
// `headersToRelay` record all hdrs need to relay. When need to update new height to
// get proof, relayer update `rightPtr` and return.
func checkCosmosHeight(h int64, hdrToVerifyProof *cosmos.CosmosHeader, infoArr []*context.CosmosInfo, rightPtr *int64) ([]*context.CosmosInfo, error) {
	query := getTxQuery(h - 1)
	res, err := ctx.CMRpcCli.TxSearch(query, true, 1, context.PerPage, "asc")
	if err != nil {
		return infoArr, err
	}

	rc, err := ctx.CMRpcCli.Commit(&h)
	if err != nil {
		return infoArr, err
	}
	if !bytes.Equal(rc.Header.ValidatorsHash, rc.Header.NextValidatorsHash) {
		vSet, err := getValidators(h)
		if err != nil {
			return infoArr, err
		}
		hdr := &cosmos.CosmosHeader{
			Header:  *rc.Header,
			Commit:  rc.Commit,
			Valsets: vSet,
		}
		val, _ := ctx.Poly.GetStorage(utils.CrossChainManagerContractAddress.ToHexString(),
			append(append([]byte(mhcomm.EPOCH_SWITCH), utils.GetUint64Bytes(ctx.Conf.SideChainId)...),
				utils.GetUint64Bytes(uint64(h))...))
		// check if this header is not committed on Poly
		if val == nil || len(val) == 0 {
			infoArr[0].Hdrs = append(infoArr[0].Hdrs, hdr)
		}
	}
	if res.TotalCount == 0 {
		return infoArr, nil
	}

	// get tx from pages
	heightToGetProof := *rightPtr - 1
	pages := ((res.TotalCount - 1) / context.PerPage) + 1
	for p := 1; p <= pages; p++ {
		// already have page 1
		if p > 1 {
			if res, err = ctx.CMRpcCli.TxSearch(query, true, p, context.PerPage, "asc"); err != nil {
				return infoArr, err
			}
		}
		// get proof for every tx, and add them to txArr prepared to commit
		for _, tx := range res.Txs {
			hash := getKeyHash(tx)
			res, _ := ctx.CMRpcCli.ABCIQueryWithOptions(context.ProofPath, ccm.GetCrossChainTxKey(hash),
				client.ABCIQueryOptions{Prove: true, Height: heightToGetProof})
			if res == nil || res.Response.GetValue() == nil {
				// If get the proof failed, that could means the header of height `heightToGetProof`
				// is already pruned. And the cosmos node already delete the data on
				// `heightToGetProof`. We need to update the height `right`, and check this height
				// `h` again
				for {
					status, err := ctx.CMRpcCli.Status()
					if err != nil {
						log.Errorf("failed to get status and could be something wrong with RPC: %v", err)
						continue
					}
					hdrToVerifyProof, err = getCosmosHdr(status.SyncInfo.LatestBlockHeight - 1)
					if err != nil {
						log.Errorf("failed to get cosmos header info and could be something wrong with RPC: %v", err)
						continue
					}
					*rightPtr = status.SyncInfo.LatestBlockHeight - 1
					if bytes.Equal(hdrToVerifyProof.Header.ValidatorsHash, hdrToVerifyProof.Header.NextValidatorsHash) {
						break
					}
					context.SleepSecs(1)
				}
				return infoArr, fmt.Errorf("%s from %d to %d", context.RightHeightUpdate, heightToGetProof+1, *rightPtr)
			}
			proof, _ := res.Response.Proof.Marshal()

			kp := merkle.KeyPath{}
			kp = kp.AppendKey([]byte(context.CosmosCrossChainModName), merkle.KeyEncodingURL)
			kp = kp.AppendKey(res.Response.Key, merkle.KeyEncodingURL)
			pv, _ := ctx.CMCdc.MarshalBinaryBare(&ccm_cosmos.CosmosProofValue{
				Kp:    kp.String(),
				Value: res.Response.GetValue(),
			})

			txParam := new(ccmc.MakeTxParam)
			_ = txParam.Deserialization(common.NewZeroCopySource(res.Response.GetValue()))

			// check if this cross-chain tx already committed on Poly
			// If we don't check, relayer will commit a new header to prove it.
			// And in the end, that header is committed for nothing because this tx
			// already committed
			val, _ := ctx.Poly.GetStorage(utils.CrossChainManagerContractAddress.ToHexString(),
				append(append([]byte(ccmc.DONE_TX), utils.GetUint64Bytes(ctx.Conf.SideChainId)...),
					txParam.CrossChainID...))
			if val != nil && len(val) != 0 {
				continue
			}
			tx.TxResult.Data = txParam.CrossChainID
			infoArr = append(infoArr, &context.CosmosInfo{
				Type: context.TyTx,
				Tx: &context.CosmosTx{
					Tx:          tx,
					ProofHeight: res.Response.Height,
					Proof:       proof,
					PVal:        pv,
				},
				Hdrs: []*cosmos.CosmosHeader{hdrToVerifyProof},
			})
		}
	}

	return infoArr, nil
}

func reproveCosmosTx(infoArr []*context.CosmosInfo, hdrToVerifyProof *cosmos.CosmosHeader) []*context.CosmosInfo {
	arr, err := ctx.Db.GetCosmosTxReproving()
	if err != nil {
		panic(fmt.Errorf("[ReProve] failed to get reproving cosmos tx: %v", err))
	}
	if arr == nil || len(arr) == 0 {
		return infoArr
	}
	log.Infof("[ReProve] total %d cosmos tx to reprove", len(arr))

	for i := 0; i < len(arr); i++ {
		tx := arr[i]
		hash := getKeyHash(tx)
		res, err := ctx.CMRpcCli.ABCIQueryWithOptions(context.ProofPath, ccm.GetCrossChainTxKey(hash),
			client.ABCIQueryOptions{Prove: true, Height: hdrToVerifyProof.Header.Height - 1})
		if err != nil || res == nil || res.Response.GetValue() == nil {
			log.Errorf("[ReProve] failed to query proof and could be something wrong with RPC: %v", err)
			return infoArr
		}

		kp := merkle.KeyPath{}
		kp = kp.AppendKey([]byte(context.CosmosCrossChainModName), merkle.KeyEncodingURL)
		kp = kp.AppendKey(res.Response.Key, merkle.KeyEncodingURL)
		pv, _ := ctx.CMCdc.MarshalBinaryBare(&ccm_cosmos.CosmosProofValue{
			Kp:    kp.String(),
			Value: res.Response.GetValue(),
		})

		proof, _ := res.Response.GetProof().Marshal()
		log.Debugf("[ReProve] repove cosmos tx %s with height %d and header %d",
			tx.Hash.String(), res.Response.Height, hdrToVerifyProof.Header.Height)

		txParam := new(ccmc.MakeTxParam)
		_ = txParam.Deserialization(common.NewZeroCopySource(res.Response.GetValue()))
		val, _ := ctx.Poly.GetStorage(utils.CrossChainManagerContractAddress.ToHexString(),
			append(append([]byte(ccmc.DONE_TX), utils.GetUint64Bytes(ctx.Conf.SideChainId)...),
				txParam.CrossChainID...))
		if val != nil && len(val) != 0 {
			if err = ctx.Db.DelCosmosTxReproving(tx.Hash); err != nil {
				panic(err)
			}
		}

		infoArr = append(infoArr, &context.CosmosInfo{
			Type: context.TyTx,
			Tx: &context.CosmosTx{
				Tx:          tx,
				ProofHeight: res.Response.Height,
				Proof:       proof,
				PVal:        pv,
			},
			Hdrs: []*cosmos.CosmosHeader{hdrToVerifyProof},
		})
		ctx.Db.SetCosmosTxTxInChan(tx.Hash)
	}
	return infoArr
}
