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
package whitelist

import (
	"encoding/json"
	"errors"
	"github.com/syndtr/goleveldb/leveldb"
	"pdx-chain/common"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"sync"
)

type WhiteList struct {
	List map[string][]byte
	Lock sync.RWMutex
}

var TrustTxWhiteList WhiteList

func InitTrustTxWhiteList() {
	v, err := (*ethdb.ChainDb).Get([]byte("TrustTxWhiteList"))
	if err != nil && leveldb.ErrNotFound.Error() != err.Error() {
		log.Error("db get error", "err", err)
		return
	}
	TrustTxWhiteList.Lock.Lock()
	defer TrustTxWhiteList.Lock.Unlock()

	TrustTxWhiteList.List = make(map[string][]byte)
	if v == nil {
		return
	}
	err = json.Unmarshal(v, &TrustTxWhiteList.List)
	if err != nil {
		log.Error("json unmarshal error", "err", err)
		return
	}

}

func Verify(from common.Address) error {
	TrustTxWhiteList.Lock.RLock()
	defer TrustTxWhiteList.Lock.RUnlock()

	log.Info("verify whitelist", "from", from.String(), "white", TrustTxWhiteList.List)
	if _, find := TrustTxWhiteList.List[from.String()]; find {
		return nil
	}
	log.Error("not in white list")
	return errors.New("not in white list")
}

//note lock nest
func AddWhite(nodes []string) error {
	TrustTxWhiteList.Lock.Lock()
	defer TrustTxWhiteList.Lock.Unlock()

	for _, v := range nodes {
		TrustTxWhiteList.List[v] = nil
	}
	//db save
	err := saveWhite()

	return err
}

func DelWhite(nodes []string) error {
	TrustTxWhiteList.Lock.Lock()
	defer TrustTxWhiteList.Lock.Unlock()

	for _, v := range nodes {
		delete(TrustTxWhiteList.List, v)
	}
	//db save
	err := saveWhite()

	return err
}

func saveWhite() error {
	v, err := json.Marshal(TrustTxWhiteList.List)
	if err != nil {
		log.Error("json marshal error", "err", err)
		return err
	}
	err = (*ethdb.ChainDb).Put([]byte("TrustTxWhiteList"), v)
	return err
}
