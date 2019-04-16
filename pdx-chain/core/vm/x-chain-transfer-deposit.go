package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"math/big"
	"pdx-chain/common"
	"pdx-chain/common/hexutil"
	"pdx-chain/core/publicBC"
	"pdx-chain/core/types"
	"pdx-chain/crypto"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"pdx-chain/rlp"
	"pdx-chain/utopia"
	"pdx-chain/utopia/utils"
	"pdx-chain/utopia/utils/blacklist"
	"pdx-chain/utopia/utils/client"
	"time"
)

type XChainTransferDeposit struct{}

type Deposit struct {
	SrcChainID    string         `json:"src_chain_id"`
	SrcChainOwner common.Address `json:"src_chain_owner"`
	TxMsg         string         `json:"tx_msg"`
}

func (x *XChainTransferDeposit) RequiredGas(input []byte) uint64 {
	return 0
}

func (x *XChainTransferDeposit) Run(ctx *PrecompiledContractContext, input []byte, extra map[string][]byte) ([]byte, error) {
	log.Info("XChainTraXChainTransferDeposit run")

	var payload Deposit
	err := json.Unmarshal(input, &payload)
	if err != nil {
		log.Error("unmarshal payload error", "err", err)
		return nil, err
	}

	err = deposit(ctx, payload)
	if err != nil {
		log.Error("deposit error", "err", err)
		return nil, err
	}

	return nil, nil
}

func deposit(ctx *PrecompiledContractContext, payload Deposit) error {
	//check tx
	from, dstUser, dstChain, txHash, err := GenMsgFromTxMsg(payload.TxMsg)
	if err != nil {
		log.Error("gen addr from txMsg error", "err", err)
		return err
	}
	contractFrom := ctx.Contract.caller.Address()
	contractAddr := ctx.Contract.Address() //deposit contract addr
	if contractFrom != from {
		putInBlacklist(contractFrom)
		return errors.New("different sender between two tx")
	}

	//确认是否已经存款成功
	keyHash := utils.DepositKeyHash(contractFrom, txHash)
	state := ctx.Evm.StateDB.GetPDXState(contractAddr, keyHash)
	if len(state) != 0 {
		log.Error("deposit has completed, then put in blacklist")
		putInBlacklist(contractFrom)
		return errors.New("deposit has completed")
	}

	//去srcChain查询withdraw的状态
	vl, st := checkWithDrawTxStatus(ctx, txHash, payload.SrcChainID)
	if !st {
		log.Error("withDraw tx not successful")
		return errors.New("withdraw tx status is fail")
	}

	if ctx.Contract.value.Cmp(vl) != 0 {
		log.Error("value not match", "deposit", ctx.Contract.value, "withdraw", vl)
		return errors.New("value not match")
	}

	if dstUser == (common.Address{}) {
		dstUser = ctx.Contract.CallerAddress
	}
	log.Info("before AddBalance", "balance", ctx.Evm.StateDB.GetBalance(dstUser), "dstUser", dstUser.String())
	ctx.Evm.StateDB.AddBalance(dstUser, vl)

	value := fmt.Sprintf("%s:%s:%s:%s", payload.SrcChainID, dstChain, vl.String(), txHash.String())
	ctx.Evm.StateDB.SetPDXState(contractAddr, keyHash, []byte(value))
	log.Info("SetPDXState", "value", value)

	log.Info("after AddBalance", "balance", ctx.Evm.StateDB.GetBalance(dstUser))
	return nil
}

func GenMsgFromTxMsg(txMsg string) (from common.Address, dstUser common.Address, dstChain string, txHash common.Hash, err error) {
	txMsgBuf, err := hexutil.Decode(txMsg)
	if err != nil {
		log.Error("hex decode txMsg error", "err", err)
		return
	}
	transaction := new(types.Transaction)
	if err = rlp.DecodeBytes(txMsgBuf, transaction); err != nil {
		log.Error("rlp decode txMsg error", "err", err)
		return
	}
	pubkey := transaction.PublicKey()
	pub, err := crypto.DecompressPubkey(pubkey)
	if err != nil {
		fmt.Println("DecompressPubkey err", err)
		return
	}

	from = crypto.PubkeyToAddress(*pub)
	txHash = transaction.Hash()
	withdrawPayload := Withdraw{}
	payloadBuf := transaction.Data()
	if err = json.Unmarshal(payloadBuf, &withdrawPayload); err != nil {
		log.Error("unmarshal withdraw payload error", "err", err)
		return
	}
	dstChain = withdrawPayload.DstChainID
	dstUser = withdrawPayload.DstUserAddr

	log.Debug("GenAddrAndHashFromTxMsg", "from", from, "txHash", txHash)
	return
}

//返回交易的value和交易状态
func checkWithDrawTxStatus(ctx *PrecompiledContractContext, txHash common.Hash, chainID string) (value *big.Int, success bool) {
	hosts := getNodeFromConfig(ctx, chainID)
	utopia.Shuffle(hosts)
	count := 0 //最大尝试次数
	for _, host := range hosts {
		count++
		if count > 5 {
			log.Error("try five times fail", "count", count)
			break
		}

		log.Debug("host", "is", host)
		cli, err := client.Connect(host)
		if err != nil {
			log.Error("client connect error", "err", err)
			continue
		}

		c, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
		transactionMsg, err := cli.GetWithDrawTransactionByHash(c, txHash)
		if err != nil {
			log.Error("GetWithDrawTransactionByHash error", "err", err)
			continue
		}

		currentCommit := transactionMsg.CurrentCommit.ToInt()
		commitNum := transactionMsg.CommitNumber.ToInt()
		log.Info("transactionMsg", "currentCommit", currentCommit, "commitNum", commitNum, "status", transactionMsg.Status)

		if currentCommit == nil ||
			commitNum == nil ||
			new(big.Int).Sub(currentCommit, commitNum).Cmp(big.NewInt(utopia.XChainTransferCommitNum)) != +1 ||
			transactionMsg.Status == -1 { //withdraw fail
			from := ctx.Contract.caller.Address()
			putInBlacklist(from)

			log.Error("tx status not successful")
			return
		}

		return (*big.Int)(transactionMsg.RPCTx.Value), true
	}

	return

}

func putInBlacklist(from common.Address) {
	log.Info("put in blacklist!!!", "from", from.String())

	//add blacklist
	blBuf := blacklist.BlackListTxTask.GetBlackList(*ethdb.ChainDb)
	err := blacklist.BlackListTxTask.Decode(blBuf)
	if err != nil {
		log.Error("black list decode error", "err", err, "blBuf", blBuf)
		return
	}

	cNum := new(big.Int).Add(public.BC.CurrentCommit().Number(), big.NewInt(1))
	b, ok := blacklist.BlackListTxTask.Blacklist[cNum.Uint64()]
	if ok {
		b = append(b, &from)
	} else {
		var list []*common.Address
		list = append(list, &from)
		blacklist.BlackListTxTask.Blacklist[cNum.Uint64()] = list
	}

	newBL, err := blacklist.BlackListTxTask.Encode()
	if err != nil {
		log.Error("black list encode err", "err", err)
	}
	blacklist.BlackListTxTask.SetBlackList(*ethdb.ChainDb, newBL)
}

func getNodeFromConfig(ctx *PrecompiledContractContext, chainID string) []string {
	for _, v := range ctx.Evm.chainConfig.Utopia.DstChain {
		if v.ChainId == chainID {
			l := len(v.RpcHosts)
			if l > 0 {
				return v.RpcHosts
			} else {
				log.Error("genesis no config for rpc hosts error")
				return nil
			}
		}
	}

	log.Error("cant find rpc host error")
	return nil
}
