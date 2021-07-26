package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/cosmos/cosmos-sdk/codec"
	tb "github.com/tendermint/tendermint/libs/bytes"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
	"github.com/tendermint/tendermint/types"
)

var (
	COSMOSState     = []byte("cosmos")
	CosmosReProve   = []byte("cosmos_reprove")
	CosmosStatusKey = []byte("cosmos_status")
)

type Database struct {
	rwl                  *sync.RWMutex
	bdb                  *bolt.DB
	cosmosTxInChanMap    map[string]bool
	polyTxInChanMap      map[string]bool
	cdc                  *codec.Codec
	totalReproveCosmosTx int
}

func NewDatabase(dbPath string, cdc *codec.Codec) (*Database, error) {
	if !strings.Contains(dbPath, ".bin") {
		dbPath = path.Join(dbPath, "db.bin")
	}
	instance := new(Database)
	bdb, err := bolt.Open(dbPath, 0666, nil)
	if err != nil {
		return nil, err
	}
	instance.bdb = bdb
	instance.rwl = new(sync.RWMutex)
	instance.cdc = cdc
	instance.cosmosTxInChanMap = make(map[string]bool)
	instance.polyTxInChanMap = make(map[string]bool)
	if err = bdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(COSMOSState); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(CosmosReProve); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(CosmosStatusKey); err != nil {
			return err
		}

		bucket := tx.Bucket(CosmosReProve)
		if err = bucket.ForEach(func(k, v []byte) error {
			instance.cosmosTxInChanMap[tb.HexBytes(k).String()] = false
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}
	instance.totalReproveCosmosTx = len(instance.cosmosTxInChanMap)

	return instance, nil
}

func (db *Database) SetCosmosHeight(height int64) error {
	db.rwl.Lock()
	defer db.rwl.Unlock()

	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, uint64(height))
	return db.bdb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(COSMOSState)
		err := bucket.Put(COSMOSState, val)
		if err != nil {
			return err
		}
		return nil
	})
}

func (db *Database) GetCosmosHeight() int64 {
	db.rwl.RLock()
	defer db.rwl.RUnlock()

	var height uint64
	_ = db.bdb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(COSMOSState)
		val := bucket.Get(COSMOSState)
		if val == nil {
			height = 0
			return nil
		}
		height = binary.LittleEndian.Uint64(val)
		return nil
	})

	return int64(height)
}

func (db *Database) SetCosmosTxReproving(rTx *coretypes.ResultTx) error {
	db.rwl.Lock()
	defer db.rwl.Unlock()

	raw, err := db.cdc.MarshalJSON(rTx)
	if err != nil {
		return err
	}

	return db.bdb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(CosmosReProve)
		if err := bucket.Put(rTx.Hash, raw); err != nil {
			return fmt.Errorf("failed to put tx %s: %v", rTx.Hash.String(), err)
		}
		db.cosmosTxInChanMap[rTx.Hash.String()] = false
		db.totalReproveCosmosTx++
		return nil
	})
}

func (db *Database) GetCosmosTxReproving() ([]*coretypes.ResultTx, error) {
	db.rwl.RLock()
	defer db.rwl.RUnlock()

	if db.totalReproveCosmosTx == 0 {
		return nil, nil
	}
	arr := make([]*coretypes.ResultTx, db.totalReproveCosmosTx)
	cnt := 0
	if err := db.bdb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(CosmosReProve)
		if err := bucket.ForEach(func(k, v []byte) error {
			rTx := &coretypes.ResultTx{}
			if err := db.cdc.UnmarshalJSON(v, rTx); err != nil {
				return fmt.Errorf("failed to unmarshal %s: %v", tb.HexBytes(k).String(), err)
			}
			if db.cosmosTxInChanMap[rTx.Hash.String()] {
				return nil
			}
			arr[cnt] = rTx
			cnt++
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return arr, nil
}

func (db *Database) SetCosmosTxTxInChan(hash tb.HexBytes) {
	db.rwl.Lock()
	defer db.rwl.Unlock()

	k := hash.String()
	if _, ok := db.cosmosTxInChanMap[k]; ok {
		db.cosmosTxInChanMap[k] = true
		db.totalReproveCosmosTx--
	}
}

func (db *Database) DelCosmosTxReproving(hash tb.HexBytes) error {
	db.rwl.Lock()
	defer db.rwl.Unlock()

	if len(db.cosmosTxInChanMap) == 0 {
		return nil
	}
	return db.bdb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(CosmosReProve)
		if err := bucket.Delete(hash); err != nil {
			return fmt.Errorf("failed to delete %s: %v", hash.String(), err)
		}
		delete(db.cosmosTxInChanMap, hash.String())
		return nil
	})
}

func (db *Database) SetTxToCosmosStatus(hash tb.HexBytes, pph *PolyProofAndHeader) error {
	db.rwl.Lock()
	defer db.rwl.Unlock()

	raw, err := pph.Serialize()
	if err != nil {
		return err
	}
	return db.bdb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(CosmosStatusKey)
		if err := bucket.Put(hash, raw); err != nil {
			return err
		}
		return nil
	})
}

func (db *Database) LoadCosmosStatus(m *sync.Map) (int, error) {
	db.rwl.RLock()
	defer db.rwl.RUnlock()

	num := 0
	if err := db.bdb.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(CosmosStatusKey)
		if err := bucket.ForEach(func(k, v []byte) error {
			pph := &PolyProofAndHeader{}
			if err := pph.Deserialize(v); err != nil {
				return err
			}
			hash := make([]byte, len(k))
			copy(hash, k)
			hb := tb.HexBytes(hash)
			m.Store(hb.String(), pph)
			num++
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return num, nil
}

func (db *Database) DelTxInCosmosStatus(hash tb.HexBytes) error {
	db.rwl.Lock()
	defer db.rwl.Unlock()

	return db.bdb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(CosmosStatusKey)
		if err := bucket.Delete(hash); err != nil {
			return err
		}
		return nil
	})
}

type PolyProofAndHeader struct {
	Txhash      string
	Proof       string
	Hdr         *types.Header
	CCID        []byte
	FromChainId uint64
}

func (pph *PolyProofAndHeader) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	rawTxHash := []byte(pph.Txhash)
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(rawTxHash))); err != nil {
		return nil, err
	}
	buf.Write(rawTxHash)
	rawProof := []byte(pph.Proof)
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(rawProof))); err != nil {
		return nil, err
	}
	buf.Write(rawProof)
	// rawHdr := pph.Hdr.ToArray()
	// if err := binary.Write(&buf, binary.LittleEndian, uint32(len(rawHdr))); err != nil {
	// 	return nil, err
	// }
	// buf.Write(rawHdr)
	if err := binary.Write(&buf, binary.LittleEndian, uint32(len(pph.CCID))); err != nil {
		return nil, err
	}
	buf.Write(pph.CCID)
	if err := binary.Write(&buf, binary.LittleEndian, pph.FromChainId); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (pph *PolyProofAndHeader) Deserialize(raw []byte) error {
	r := bytes.NewReader(raw)
	var l uint32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}
	rawTxHash := make([]byte, l)
	if _, err := r.Read(rawTxHash); err != nil {
		return err
	}

	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}
	rawProof := make([]byte, l)
	if _, err := r.Read(rawProof); err != nil {
		return err
	}

	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}
	rawHdr := make([]byte, l)
	if _, err := r.Read(rawHdr); err != nil {
		return err
	}
	// hdr := &types.Header{}
	// if err := hdr.Deserialization(common.NewZeroCopySource(rawHdr)); err != nil {
	// 	return err
	// }

	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		return err
	}
	ccid := make([]byte, l)
	if _, err := r.Read(ccid); err != nil {
		return err
	}

	var id uint64
	if err := binary.Read(r, binary.LittleEndian, &id); err != nil {
		return err
	}

	pph.Txhash = string(rawTxHash)
	pph.Proof = string(rawProof)
	// pph.Hdr = hdr
	pph.CCID = ccid
	pph.FromChainId = id

	return nil
}
