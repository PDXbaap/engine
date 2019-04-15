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
package cacheBlock

import (
	"pdx-chain/common"
	"pdx-chain/core/types"
	"pdx-chain/log"
	"sync"
)

type cacheBlockChain struct {

	CacheBlock map[common.Hash]*types.Block
	Lock sync.RWMutex
}

//临时存储Normal和Commit

var CacheBlocks =cacheBlockChain{CacheBlock:make(map[common.Hash]*types.Block)}

func (c *cacheBlockChain)AddBlock(block *types.Block)  {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	log.Info("存的Block","高度",block.NumberU64(),"hash",block.Hash())
	c.CacheBlock[block.Hash()]=block
}

func (c *cacheBlockChain)GetBlock(key common.Hash)  (block *types.Block){
	c.Lock.RLock()
	defer c.Lock.RUnlock()
	var ok bool
	log.Info("需要找的block","hash",key)
	if block,ok=c.CacheBlock[key];ok{
		log.Info("找到Block","高度",c.CacheBlock[key].NumberU64())
	}
	return block
}

func (c *cacheBlockChain)DelBlock(block *types.Block)  {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	delete(c.CacheBlock,block.Hash())
}