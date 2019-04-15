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
package util

import (
	"container/list"
	"sync"
)

type LRUMap interface {
	PutIfAbsent(interface{}) interface{}
	Len() int
}

type lruMapImpl struct {
	lock *sync.Mutex
	ks   *list.List
	kvs  map[interface{}]interface{}
	max  int
}

func NewLRUMap(max int) *lruMapImpl {
	return &lruMapImpl{
		lock: &sync.Mutex{},
		ks:   list.New(),
		kvs:  make(map[interface{}]interface{}),
		max:  max,
	}
}

var value = []byte{0}

func (lrm *lruMapImpl) PutIfAbsent(key interface{}) interface{} {
	lrm.lock.Lock()
	defer lrm.lock.Unlock()
	if lrm.kvs[key] == nil {
		// 如果超过最大值，删除
		if lrm.ks.Len() == lrm.max {
			delete(lrm.kvs, lrm.ks.Front().Value)
			lrm.ks.Remove(lrm.ks.Front())
		}

		lrm.ks.PushBack(key)
		lrm.kvs[key] = value

		return nil
	} else {

		for e := lrm.ks.Front(); e != nil; e = e.Next() {
			if key == e.Value {
				lrm.ks.MoveToBack(e)
				break
			}
		}
		return key
	}
}

func (lrm *lruMapImpl) Len() int {
	return lrm.ks.Len()
}
