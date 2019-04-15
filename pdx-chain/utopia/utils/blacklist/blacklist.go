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
package blacklist

import (
	"encoding/json"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"pdx-chain/common"
	"pdx-chain/core/publicBC"
	"pdx-chain/core/types"
	"pdx-chain/crypto"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"pdx-chain/utopia"
	"sync"
)

var BlackListKey = []byte("BlackListKey")

type BlackListTx struct {
	Blacklist map[uint64][]*common.Address
	Lock      sync.RWMutex
}

var BlackListTxTask *BlackListTx

func NewBlackListTx() *BlackListTx {
	return &BlackListTx{Blacklist: make(map[uint64][]*common.Address)}
}

func (b *BlackListTx) SetBlackList(db ethdb.Database, blacklistMap []byte) {
	b.Lock.Lock()
	defer b.Lock.Unlock()

	err := db.Put(BlackListKey, blacklistMap) //数据库存
	if err != nil {
		return
	}

}

func (b *BlackListTx) GetBlackList(db ethdb.Database) []byte {
	b.Lock.Lock()
	defer b.Lock.Unlock()

	blackList, err := db.Get(BlackListKey) //数据库取
	if err != nil && err.Error() != leveldb.ErrNotFound.Error() {
		log.Error("blackList db get error", "err", err)
		return nil
	}
	return blackList
}

func (b *BlackListTx) UpdateBlackList(commitNum uint64, db ethdb.Database) {
	b.Lock.Lock()
	defer b.Lock.Unlock()

	delete(b.Blacklist, commitNum-utopia.BlackListExpire) //缓存删除10个commit以前的黑名单

	blackList, err := b.Encode()
	if err != nil {
		return
	}
	db.Put(BlackListKey, blackList) //覆盖数据库黑名单
}

func (b *BlackListTx) Encode() ([]byte, error) {
	return json.Marshal(b.Blacklist)
}

func (b *BlackListTx) Decode(data []byte) error {
	if len(data) == 0 {
		b.Blacklist = make(map[uint64][]*common.Address)
		return nil
	}
	return json.Unmarshal(data, &b.Blacklist)
}

func (b *BlackListTx) VerifyBlackListTx(tx *types.Transaction) error {
	pub := tx.PublicKey()
	key, _ := crypto.DecompressPubkey(pub)
	form := crypto.PubkeyToAddress(*key)
	log.Info("from is", ":", form.String())
	currentNum := public.BC.CurrentCommit().Number().Uint64()
	for cnum, multiAddress := range b.Blacklist {
		if currentNum > cnum && currentNum-cnum > utopia.BlackListExpire {
			log.Info("expired blacklist", "currentNum", currentNum, "cnum", cnum)
			continue
		}
		for _, address := range multiAddress {
			if form == *address {
				log.Info("in blacklist", "address", address, "currentNum", currentNum, "cnum", cnum)
				return errors.New("账户再黑名单中,终止交易")
			}
		}
	}
	return nil
}
