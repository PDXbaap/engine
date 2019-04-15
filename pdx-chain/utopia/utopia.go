/*************************************************************************
 * Copyright (C) 2016-2019 PDX Technologies, Inc. All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *************************************************************************/
package utopia

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/btcsuite/btcd/btcec"
	"github.com/golang/protobuf/proto"
	"gopkg.in/urfave/cli.v1"
	"io/ioutil"
	"math/big"
	mathRand "math/rand"
	"net/http"
	"os"
	"pdx-chain/accounts"
	"pdx-chain/accounts/keystore"
	"pdx-chain/common"
	"pdx-chain/core/types"
	"pdx-chain/crypto"
	"pdx-chain/crypto/sha3"
	"pdx-chain/log"
	"pdx-chain/pdxcc/protos"
	"pdx-chain/rlp"
	"pdx-chain/utopia/utils/client"
	"pdx-chain/utopia/utils/tcUpdate"
	"strings"
	"time"
)

var (
	Config *cli.Context
)

const (
	BlackListExpire         = 2 //10
	XChainTransferCommitNum = 2 //10
	TrustTxCommitBlockLimit = 1 //30
	TrustTxGasLimit         = 10000
)

func RecoverPlain(sighash common.Hash, R, S, Vb *big.Int, homestead bool) (common.Address, error) {
	if Vb.BitLen() > 8 {
		return common.Address{}, types.ErrInvalidSig
	}
	V := byte(Vb.Uint64() - 27)
	if !crypto.ValidateSignatureValues(V, R, S, homestead) {
		return common.Address{}, types.ErrInvalidSig
	}
	// encode the snature in uncompressed format
	r, s := R.Bytes(), S.Bytes()
	sig := make([]byte, 65)
	copy(sig[32-len(r):32], r)
	copy(sig[64-len(s):64], s)
	sig[64] = V
	// recover the public key from the snature
	pub, err := crypto.Ecrecover(sighash[:], sig)
	if err != nil {
		return common.Address{}, err
	}
	if len(pub) == 0 || pub[0] != 4 {
		return common.Address{}, errors.New("invalid public key")
	}
	var addr common.Address
	copy(addr[:], crypto.Keccak256(pub[1:])[12:])
	return addr, nil
}

//data hash
func Hash(v interface{}) [32]byte {
	//bytes, err := json.Marshal(v)
	bytes, err := rlp.EncodeToBytes(v)
	if err != nil {
		log.Info("rlp encode error :")
	}

	return sha256.Sum256(bytes)
}

//data hash
func HashBytes(data []byte) [32]byte {
	//bytes, err := json.Marshal(v)
	//if err != nil {
	//	log.Println("json error")
	//}
	return sha256.Sum256(data)
}

//create privateKey and publickKey
func NewKey() (*ecdsa.PrivateKey, error) {
	privateKeyECDSA, err := ecdsa.GenerateKey(crypto.S256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	return privateKeyECDSA, nil
}

func PubkeyToAddress(p ecdsa.PublicKey) common.Address {
	pubBytes := crypto.FromECDSAPub(&p)
	return common.BytesToAddress(crypto.Keccak256(pubBytes[1:])[12:])
}

//签名
func SignBlcok(block *types.Block, prv *ecdsa.PrivateKey) ([]byte, error) {
	h := Hash(block)
	sig, err := crypto.Sign(h[:], prv)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

//把签名转成r,s,v格式
func SignatureValues(sig []byte) (r, s, v *big.Int, err error) {
	if len(sig) != 65 {
		panic(fmt.Sprintf("wrong size for signature: got %d, want 65", len(sig)))
	}
	r = new(big.Int).SetBytes(sig[:32])
	s = new(big.Int).SetBytes(sig[32:64])
	v = new(big.Int).SetBytes([]byte{sig[64] + 27})
	return r, s, v, nil
}

//通过签名生成公钥
func Ecrecover(hash []byte, sig []byte) ([]byte, error) {

	pub, err := crypto.Ecrecover(hash, sig)
	if err != nil {
		return nil, err
	}
	return pub, err
}

func Ecrecover1(hash []byte, byte []byte) ([]byte, error) {
	//V := byte(Vb.Uint64() - 27)  //v转byte
	//r, s := R.Bytes(), S.Bytes()
	//sig := make([]byte, 65)
	//copy(sig[32-len(r):32], r)
	//copy(sig[64-len(s):64], s)
	//sig[64] = V
	//log.Println(len(byte))
	pub, err := crypto.Ecrecover(hash, byte)
	if err != nil {
		return nil, err
	}
	return pub, err
}

var (
	secp256k1N, _  = new(big.Int).SetString("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141", 16)
	secp256k1halfN = new(big.Int).Div(secp256k1N, big.NewInt(2))
)

func VerifySignature(pubkey, hash, sign []byte) bool {

	if len(sign) != 65 {
		return false
	}
	sig := &btcec.Signature{R: new(big.Int).SetBytes(sign[:32]), S: new(big.Int).SetBytes(sign[32:])}
	key, err := btcec.ParsePubKey(pubkey, btcec.S256())
	if err != nil {
		return false
	}
	// Reject malleable signatures. libsecp256k1 does this check but btcec doesn't.
	//if sig.S.Cmp(secp256k1halfN) > 0 {
	//	return false
	//}
	return sig.Verify(hash, key)
}

func CompressPubkey(pubkey *ecdsa.PublicKey) []byte {
	return (*btcec.PublicKey)(pubkey).SerializeCompressed()
}

func SendTrustTx(account accounts.Account, wallet accounts.Wallet, host, ccName, version, fcn string, params [][]byte, meta map[string][]byte) error {
	log.Info("send trust tx to " + host)

	cli, err := client.Connect(host)
	if err != nil {
		log.Error("!!!!connect error", "err", err)
		return err
	}

	owner := ""
	to := Keccak256ToAddress(owner + ":" + ccName + ":" + version)
	ctx, _ := context.WithTimeout(context.Background(), 800*time.Millisecond)
	nonce, err := cli.EthClient.NonceAt(ctx, account.Address, nil)
	if err != nil {
		log.Error("!!!!!nonce error", "err", err, "from", account.Address)
		return err
	}
	log.Info("!!!!!!nonce", "nonce", nonce)
	amount := big.NewInt(0)
	gasLimit := uint64(TrustTxGasLimit)
	gasPrice := big.NewInt(20000000000)

	inv := &protos.Invocation{
		Fcn:  fcn,
		Args: params,
		Meta: meta,
	}
	payload, err := proto.Marshal(inv)
	if err != nil {
		log.Error("proto marshal invocation error", "err", err)
		return err
	}

	ptx := &protos.Transaction{
		Type:    types.Transaction_invoke,
		Payload: payload,
	}
	data, err := proto.Marshal(ptx)
	if err != nil {
		log.Error("!!!!!!!!proto marshal error", "err", err)
		return err
	}

	tx := types.NewTransaction(nonce, to, amount, gasLimit, gasPrice, data)

	// Look up the wallet containing the requested signer
	signedTx, err := wallet.SignTx(account, tx, nil)

	txHash, err := cli.SendRawTransaction(ctx, signedTx)
	if err != nil {
		log.Error("!!!!!!send raw transaction error", "err", err)
		return err
	}

	log.Info("#####sucess send raw transaction txHash", "hash", txHash)
	return nil

}

func Keccak256ToAddress(ccName string) common.Address {
	hash := sha3.NewKeccak256()

	var buf []byte
	hash.Write([]byte(ccName))
	buf = hash.Sum(buf)


	return common.BytesToAddress(buf)
}

func GetPrivKey() *ecdsa.PrivateKey {
	ksDir := Config.String("keystore")
	pw := Config.String("password")

	var files []os.FileInfo
	var err error
	if ksDir != "" {
		files, err = ioutil.ReadDir(ksDir)
		if err != nil {
			log.Error("keystore error", "err", err)
			return nil
		}
	} else {
		dataDir := Config.String("datadir")
		ksDir = dataDir + "/keystore"
		files, err = ioutil.ReadDir(ksDir)
		if err != nil {
			log.Error("keystore error", "err", err)
			return nil
		}
	}

	addr := Config.String("etherbase")
	if strings.HasPrefix(addr, "0x") {
		addr = addr[2:]
	}
	addr = strings.ToLower(addr)
	var keystoreFile string
	for _, f := range files {
		log.Debug("key store file", "name", f.Name(), "addr", addr)
		if strings.HasSuffix(f.Name(), addr) {
			keystoreFile = f.Name()
			break
		}
	}
	if keystoreFile == "" {
		log.Error("keystore and etherbase no match")
		return nil
	}

	keyjson, err := ioutil.ReadFile(ksDir + "/" + keystoreFile)
	if err != nil {
		log.Error("keystore file read error", "err", err)
		return nil
	}

	password, err := ioutil.ReadFile(pw)
	if err != nil {
		log.Error("password file read error", "err", err)
		return nil
	}

	key, err := keystore.DecryptKey(keyjson, strings.Replace(string(password), "\n", "", -1))
	if err != nil {
		log.Error("decrypt key error", "err", err)
		return nil
	}
	log.Debug("my privatekey", "privkey", fmt.Sprintf("%x", crypto.FromECDSA(key.PrivateKey)))

	return key.PrivateKey
}

func Shuffle(hosts []string) {
	for l := len(hosts); l > 1; l-- {
		n := mathRand.Intn(l)
		hosts[l-1], hosts[n] = hosts[n], hosts[l-1]
	}
}

func GetMaxNumFromTrustChain(hosts []string, chainID string) *big.Int  {
	retryN := 0
	Shuffle(hosts)
retry:
	for i, dstHost := range hosts {
		log.Info("retry get max num", "i", i)
		if i > 4 {
			log.Error("try five times fail", "i", i)
			break
		}

		cli, err := client.Connect(dstHost)
		if err != nil {
			log.Error("!!!!connect error", "err", err)
			continue
		}

		maxNum, err := cli.GetMaxNumFromTrustChain(chainID)
		if err != nil {
			log.Error("get maxNum from trust error", "err", err)
			if retryN == 0 {
				log.Info("get new hosts")
				trustHosts := GetTrustNodeFromIaas(chainID)
				if len(trustHosts) == 0 {
					log.Error("trust nodes empty from Iaas")
					return nil
				}
				hosts = trustHosts
				retryN = 1
				goto retry
			}
			continue
		}
		return maxNum
	}

	return nil

}

//get trust chain hosts list
func GetTrustNodeFromIaas(chainID string) (hosts []string) {
	log.Info("test getNodeFromIaas", "node", Config.String("iaasServer"))
	//http://localhost:8080/rest/chain/tchosts?chainId=739
	resp, err := http.Get(Config.String("iaasServer") + "/rest/chain/tchosts?chainId=" + chainID)
	if err != nil {
		log.Error("resp error", "err", err)
		return  nil
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("read resp body error", "err", err)
		return nil
	}
	log.Info("get trust node resp", "status", resp.Status)

	type Data struct {
		ChainId string `json:"chainId"`//down layer trust chain ID
		Hosts []string `json:"hosts"`
	}

	type RspBody struct {
		Status int               `json:""`
		Meta   map[string]string `json:"meta"`
		Data   Data          `json:"data"`
	}
	var result RspBody
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Error("body unmarshal error", "err", err)
		return nil
	}

	if result.Status == 300 {
		result.Data.Hosts = []string{"300"}
	}

	//set cache
	tcUpdate.TrustHosts.Lock.Lock()
	tcUpdate.TrustHosts.CurChainID = result.Data.ChainId
	tcUpdate.TrustHosts.HostList = result.Data.Hosts
	tcUpdate.TrustHosts.Lock.Unlock()
	//copy
	hosts = make([]string, 0, len(result.Data.Hosts))
	hosts = append(hosts, result.Data.Hosts...)
	return hosts
}