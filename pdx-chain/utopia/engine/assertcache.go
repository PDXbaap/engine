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
package engine

import (
	"pdx-chain/common"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"pdx-chain/utopia/types"
	"pdx-chain/utopia/utils"
	"strconv"
	"sync"
)

var SynAssertionLock sync.Mutex
var AssertCacheObject = &AssertCache{Height2AssertCache: make(map[uint64]*utils.SafeSet)}

type AssertInfo struct {
	Address     common.Address
	BlockExtra  *types.BlockExtra
	AssertExtra *types.AssertExtra
}

type AssertCache struct {
	lock              sync.RWMutex
	commitBlockHeight uint64 // commitBlockHeight of next commit block
	// safeset is composed of common.Addresss.Hex() -> AssertInfo
	Height2AssertCache map[uint64]*utils.SafeSet
}

func (q *AssertCache) Set(height uint64, set *utils.SafeSet, db ethdb.Database) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.Height2AssertCache[height] = set

	if height > q.commitBlockHeight {
		q.commitBlockHeight = height
	}

	if db == nil {
		return
	}

	data, err := set.Encode()
	if err == nil {
		db.Put([]byte("commit-asserts:"+strconv.Itoa(int(height))), data)
	}
}

func (q *AssertCache) Get(height uint64, db ethdb.Database) (*utils.SafeSet, bool) {
	q.lock.RLock()
	defer q.lock.RUnlock()
	set, ok := q.Height2AssertCache[height]
	if ok {
		return set, ok
	}

	if db == nil {
		log.Error("db is nil")
		return nil, false
	}

	data, err := db.Get([]byte("commit-asserts:" + strconv.Itoa(int(height))))

	if err != nil {
		log.Error("db get commit-asserts", "err", err)
		return nil, false
	}

	var set2 utils.SafeSet

	err = set2.Decode(data)

	if err != nil {
		log.Error("set2 decode", "err", err)
		return nil, false
	}

	if height > q.commitBlockHeight {
		q.commitBlockHeight = height
	}

	return &set2, ok
}

func (q *AssertCache) Del(height uint64, db ethdb.Database) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if height == q.commitBlockHeight {
		q.commitBlockHeight--
	}

	delete(q.Height2AssertCache, height)
	if db != nil {
		db.Delete([]byte("commit-asserts:" + strconv.Itoa(int(height))))
	}
}
