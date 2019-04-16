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
package client

import (
	"context"
	"fmt"
	"math/big"
	"pdx-chain/common"
	"pdx-chain/common/hexutil"
	"pdx-chain/core/types"
	"pdx-chain/ethclient"
	"pdx-chain/log"
	"pdx-chain/rlp"
	"pdx-chain/rpc"
	"time"
)

// Client defines typed wrappers for the Ethereum RPC API.
type Client struct {
	rpcClient *rpc.Client
	EthClient *ethclient.Client
}

// Connect creates a client that uses the given host.
func Connect(host string) (*Client, error) {
	rpcClient, err := rpc.Dial(host)

	if err != nil {
		return nil, err
	}
	ethClient := ethclient.NewClient(rpcClient)

	return &Client{rpcClient, ethClient}, nil
}

// SendRawTransaction injects a transaction into the pending pool for execution.
//
// If the transaction was a contract creation use the TransactionReceipt method to get the
// contract address after the transaction has been mined.
func (ec *Client) SendRawTransaction(ctx context.Context, tx *types.Transaction) (common.Hash, error) {
	var txHash common.Hash
	if data, err := rlp.EncodeToBytes(tx); err != nil {
		return txHash, err
	} else {
		err := ec.rpcClient.CallContext(ctx, &txHash, "eth_sendRawTransaction", common.ToHex(data))
		return txHash, err
	}
}

type RPCTransaction struct {
	BlockHash        common.Hash     `json:"blockHash"`
	BlockNumber      *hexutil.Big    `json:"blockNumber"`
	From             common.Address  `json:"from"`
	Gas              hexutil.Uint64  `json:"gas"`
	GasPrice         *hexutil.Big    `json:"gasPrice"`
	Hash             common.Hash     `json:"hash"`
	Input            hexutil.Bytes   `json:"input"`
	Nonce            hexutil.Uint64  `json:"nonce"`
	To               *common.Address `json:"to"`
	TransactionIndex hexutil.Uint    `json:"transactionIndex"`
	Value            *hexutil.Big    `json:"value"`
	V                *hexutil.Big    `json:"v"`
	R                *hexutil.Big    `json:"r"`
	S                *hexutil.Big    `json:"s"`
}

type WithdrawRPCTransaction struct {
	RPCTx         *RPCTransaction `json:"rpc_tx"`
	Status        int             `json:"status"` //0 pending 1 success 2 commitSuccess
	CommitNumber  *hexutil.Big    `json:"commitNumber"`
	CurrentCommit *hexutil.Big    `json:"currentCommit"`
	TxMsg         string          `json:"txMsg"` //交易报文
}

func (ec *Client) GetWithDrawTransactionByHash(ctx context.Context, txHash common.Hash) (WithdrawRPCTransaction, error) {
	var transaction WithdrawRPCTransaction
	err := ec.rpcClient.CallContext(ctx, &transaction, "eth_getWithDrawTransactionByHash", txHash)
	return transaction, err
}

func (ec *Client) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	receipt, err := ec.EthClient.TransactionReceipt(ctx, txHash)
	if err != nil {
		log.Error("get transaction receipt error", "err", err)
		return nil, err
	}
	return receipt, nil
}

func (ec *Client) CheckTxCompletedByHash(from common.Address, txHash common.Hash) (result bool) {
	result = true
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	ec.rpcClient.CallContext(ctx, &result, "eth_checkTxCompletedByHash", from, txHash)
	return
}

func (ec *Client) GetMaxNumFromTrustChain(chainID string) (result *big.Int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	err = ec.rpcClient.CallContext(ctx, &result, "eth_getMaxNumFromTrustChain", chainID)
	return
}

func (ec *Client) CheckTransaction(ctx context.Context, receiptChan chan *types.Receipt, txHash common.Hash, retrySeconds time.Duration) {
	// check transaction receipt
	go func() {
		fmt.Printf("Check transaction: %s\n", txHash.String())
		for {
			receipt, _ := ec.EthClient.TransactionReceipt(ctx, txHash)
			if receipt != nil {
				receiptChan <- receipt
				break
			} else {
				fmt.Printf("Retry after %d second\n", retrySeconds)
				time.Sleep(retrySeconds * time.Second)
			}
		}
	}()
}
