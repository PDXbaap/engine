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
package tcUpdate

import (
	"encoding/json"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"pdx-chain/core/types"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"pdx-chain/pdxcc/protos"
	"sync"
)

type TrustChainUpdTxModel struct {
	ChainId        string   `json:"chainId"`
	ChainOwner     string   `json:"chainOwner"`
	Timestamp      int64    `json:"timestamp"`
	Random         string   `json:"random"`
	TcAdmCrt       string   `json:"tcadmCrt"`
	Signature      string   `json:"signature"`
	EnodeList      []string `json:"enodeList"`
	HostList       []string `json:"hostList"`
	PrevChainID    string   `json:"prevChainID"`
	PrevChainOwner string   `json:"prevChainOwner"`
	SelfHost       string   `json:"selfHost"`
}

//缓存Lssa传来的信任链信息
var TrustHosts TrustChainHosts

type TrustChainHosts struct {
	HostList []string
	CurChainID string // current trust chain ID
	PreChainID string // previous trust chain ID
	Lock     sync.RWMutex `json:"-"`
}

func (t *TrustChainHosts) ReadHosts() []string {
	t.Lock.RLock()
	defer t.Lock.RUnlock()

	//copy
	hosts := make([]string, 0, len(t.HostList))
	hosts = append(hosts, t.HostList...)
	return hosts
}

func (t *TrustChainHosts) ReadChainID() (string, string) {
	t.Lock.RLock()
	defer t.Lock.RUnlock()

	return t.CurChainID, t.PreChainID

}

func (t *TrustChainHosts) WriteHosts(chainID string, hosts []string) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	t.HostList = hosts

}

func (t *TrustChainHosts) WriteTrustChain(preChainID, chainID string, hosts []string) {
	t.Lock.Lock()

	t.PreChainID = preChainID
	t.CurChainID = chainID
	t.HostList = hosts
	log.Info("write trust chain", "chainID", chainID, "hosts", hosts, "preChainID", t.PreChainID, "currentChainID", t.CurChainID)

	t.Lock.Unlock()

	//save db
	db := *ethdb.ChainDb
	trustChainHosts, err := json.Marshal(t)
	if err != nil {
		log.Error("marshal trustChainHosts err")
	}
	err = db.Put([]byte("TxChain"), trustChainHosts)
	if err != nil {
		log.Error("db put txChain error", "err", err)
		return
	}

}

//序列化传来的信任链信息
func TrustChainUpdTxModelDecode(input []byte) (updModel *TrustChainUpdTxModel, err error) {
	if input == nil {
		return nil, errors.New("input is nil")
	}

	ftx := &protos.Transaction{}
	err = proto.Unmarshal(input, ftx)
	if err != nil {
		log.Error("unmarshal finalTx error", "err", err)
		return
	}
	if ftx.Type != types.Transaction_invoke {
		log.Error("transaction type error", "type", ftx.Type)
		return nil, errors.New("transaction type error")
	}
	inv := &protos.Invocation{}
	err = proto.Unmarshal(ftx.Payload, inv)
	if err != nil {
		log.Error("proto unmarshal invocation error", "err", err)
		return
	}

	if len(inv.Args) != 1 {
		log.Error("params is empty")
		return
	}
	err = json.Unmarshal(inv.Args[0], &updModel)
	if err != nil {
		log.Error("unmarshal payload error", "err", err)
		return
	}

	return
}

func GetTrustHosts() (hosts []string) {
	db := *ethdb.ChainDb
	if db == nil {
		log.Info("db为空")
		return
	}

	hosts = TrustHosts.ReadHosts()
	if len(hosts) == 0 {
		data, err := db.Get([]byte("TxChain"))
		if err != nil {
			return
		}
		var trustChain TrustChainHosts
		err = json.Unmarshal(data, &trustChain)
		if err != nil {
			log.Error("unmarshal TrustChainHosts error", "err", err)
			return
		}

		//copy
		hosts = make([]string, 0, len(trustChain.HostList))
		hosts = append(hosts, trustChain.HostList...)
		//set cache
		TrustHosts.Lock.Lock()
		TrustHosts.CurChainID = trustChain.CurChainID
		TrustHosts.PreChainID = trustChain.PreChainID
		TrustHosts.HostList = trustChain.HostList
		TrustHosts.Lock.Unlock()

	}

	return hosts
}
