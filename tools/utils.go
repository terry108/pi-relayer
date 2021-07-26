package tools

import (
	"fmt"
	"io/ioutil"

	"github.com/cosmos/cosmos-sdk/crypto/keys/mintkey"
	"github.com/cosmos/cosmos-sdk/types"
	crypto2 "github.com/tendermint/tendermint/crypto"
)

func GetCosmosPrivateKey(path string, pwd []byte) (crypto2.PrivKey, types.AccAddress, error) {
	bz, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, types.AccAddress{}, err
	}

	privKey, _, err := mintkey.UnarmorDecryptPrivKey(string(bz), string(pwd))
	if err != nil {
		return nil, types.AccAddress{}, fmt.Errorf("failed to decrypt private key: v", err)
	}

	return privKey, types.AccAddress(privKey.PubKey().Address().Bytes()), nil
}
