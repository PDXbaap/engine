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
package iaasconn

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"pdx-chain/common"
	"pdx-chain/common/hexutil"
	core_types "pdx-chain/core/types"
	"pdx-chain/log"
	"pdx-chain/p2p/discover"
	"pdx-chain/pdxcc/conf"
	"pdx-chain/utopia"
	"pdx-chain/utopia/types"
	"time"
)

type CommitExtra struct {
	NewBlockHeight *hexutil.Big `json:"newBlockHeight"`

	AcceptedBlocks []common.Hash `json:"acceptedBlocks"`

	Evidences []types.CondensedEvidence `json:"evidences"`

	// Changes on consensus nodes
	MinerAdditions []common.Address `json:"minerAdditions"`
	MinerDeletions []common.Address `json:"minerDeletions"`

	// Change on observatory nodes
	NodeAdditions []common.Address `json:"nodeAdditions"`
	NodeDeletions []common.Address `json:"nodeDeletions"`

	Quorum []string `json:"quorum"`
	CNum   uint64   `json:"cNum"`
	Island bool     `json:"island"`
}
type BlockExtra struct {
	NodeID discover.NodeID `json:"nodeID"`

	Rank uint32 `json:"rank"`

	CNumber *hexutil.Big `json:"cNumber"`

	Signature []byte `json:"signature"`

	IP   net.IP `json:"ip"`
	Port uint16 `json:"port"`

	// For extension only
	CommitExtra CommitExtra `json:"commitExtra"`
}
type Header struct {
	ParentHash  common.Hash           `json:"parentHash"       gencodec:"required"`
	UncleHash   common.Hash           `json:"sha3Uncles"       gencodec:"required"`
	Coinbase    common.Address        `json:"miner"            gencodec:"required"`
	Root        common.Hash           `json:"stateRoot"        gencodec:"required"`
	TxHash      common.Hash           `json:"transactionsRoot" gencodec:"required"`
	ReceiptHash common.Hash           `json:"receiptsRoot"     gencodec:"required"`
	Bloom       core_types.Bloom      `json:"logsBloom"        gencodec:"required"`
	Difficulty  *hexutil.Big          `json:"difficulty"       gencodec:"required"`
	Number      *hexutil.Big          `json:"number"           gencodec:"required"`
	GasLimit    hexutil.Uint64        `json:"gasLimit"         gencodec:"required"`
	GasUsed     hexutil.Uint64        `json:"gasUsed"          gencodec:"required"`
	Time        *hexutil.Big          `json:"timestamp"        gencodec:"required"`
	Extra       BlockExtra            `json:"blockExtra"        gencodec:"required"`
	MixDigest   common.Hash           `json:"mixHash"          gencodec:"required"`
	Nonce       core_types.BlockNonce `json:"nonce"            gencodec:"required"`
}
type Block struct {
	Header       *Header                 `json:"header"`
	Uncles       []*Header               `json:"uncles"`
	Transactions core_types.Transactions `json:"transactions"`
}

func SendToIaas(commitBlock *core_types.Block, blockExtra types.BlockExtra, commitExtra types.CommitExtra) {


	h := commitBlock.Header()

	var enc Header
	enc.ParentHash = h.ParentHash
	enc.UncleHash = h.UncleHash
	enc.Coinbase = h.Coinbase
	enc.Root = h.Root
	enc.TxHash = h.TxHash
	enc.ReceiptHash = h.ReceiptHash
	enc.Bloom = h.Bloom
	enc.Difficulty = (*hexutil.Big)(h.Difficulty)
	enc.Number = (*hexutil.Big)(h.Number)
	enc.GasLimit = hexutil.Uint64(h.GasLimit)
	enc.GasUsed = hexutil.Uint64(h.GasUsed)
	enc.Time = (*hexutil.Big)(h.Time)
	enc.MixDigest = h.MixDigest
	enc.Nonce = h.Nonce
	enc.Extra = BlockExtra{
		NodeID:    blockExtra.NodeID,
		Rank:      blockExtra.Rank,
		CNumber:   (*hexutil.Big)(blockExtra.CNumber),
		Signature: blockExtra.Signature,
		IP:        blockExtra.IP,
		Port:      blockExtra.Port,
		CommitExtra: CommitExtra{
			NewBlockHeight: (*hexutil.Big)(commitExtra.NewBlockHeight),
			AcceptedBlocks: commitExtra.AcceptedBlocks,
			Evidences:      commitExtra.Evidences,
			MinerAdditions: commitExtra.MinerAdditions,
			MinerDeletions: commitExtra.MinerDeletions,

			// Change on observatory nodes
			NodeAdditions: commitExtra.NodeAdditions,
			NodeDeletions: commitExtra.NodeDeletions,

			Quorum: commitExtra.Quorum,
			CNum:   commitExtra.CNum,
			Island: commitExtra.Island,
		},
	}

	block := Block{
		Header:       &enc,
		Uncles:       nil,
		Transactions: nil,
	}

	bl, err := json.Marshal(&block)
	if err != nil {
		log.Error("marshal header err", "err", err)
		return
	}

	log.Info("iaas server is:", "server", utopia.Config.String("iaasServer"))
	req, err := http.NewRequest(
		"POST",
		utopia.Config.String("iaasServer")+"/rest/chain/block?chainId="+conf.ChainId.String(),
		bytes.NewBuffer(bl),
	)
	if err != nil {
		log.Error("newRequest err", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{Transport: tr}
	client.Timeout = time.Millisecond * 800
	resp, err := client.Do(req)
	if err != nil {
		log.Error("client do", "err", err)
		return
	}
	defer resp.Body.Close()

	respBuffer, _ := ioutil.ReadAll(resp.Body)
	log.Info("send to iaas resp", "resp", string(respBuffer), "status", resp.Status)

}

func GetNodeFromIaas(chainID string) (host string) {
	log.Info("test getNodeFromIaas", "node", utopia.Config.String("iaasServer"))
	resp, err := http.Get(utopia.Config.String("iaasServer") + "/rest/chain/hosts?chainId=" + chainID)
	if err != nil {
		log.Error("resp error", "err", err)
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Error("read resp body error", "err", err)
		return
	}
	log.Info("get node from iaas resp", "status", resp.Status)

	type RspBody struct {
		Status int               `json:""`
		Meta   map[string]string `json:"meta"`
		Data   []string          `json:"data"`
	}
	var result RspBody
	err = json.Unmarshal(body, &result)
	if err != nil {
		log.Error("body unmarshal error", "err", err)
		return
	}

	l := len(result.Data)
	if l == 0 {
		return
	}
	r := rand.Intn(l)
	return result.Data[r]

}
