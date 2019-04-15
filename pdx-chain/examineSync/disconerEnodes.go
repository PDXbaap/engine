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
package examineSync

import (
	"pdx-chain/log"
	"pdx-chain/p2p/discover"
	"sync"
)

//eNode和discover.Node
type discoverEnodes struct {
	DiscoverNode map[string]*discover.Node
	Lock         sync.RWMutex
}

var DiscoverEnode = discoverEnodes{DiscoverNode: make(map[string]*discover.Node)}

func (d *discoverEnodes) AddDiscoverEnode(id string, node *discover.Node) {
	d.Lock.Lock()
	defer d.Lock.Unlock()
	d.DiscoverNode[id] = node

}

//根据NodeID取出key
func (d *discoverEnodes) GetDiscoverEnodeKey(nodeID discover.NodeID) string {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	for id, node := range d.DiscoverNode {
		if node.ID.String() == nodeID.String() {
			return id
		}
	}
	return ""
}
func (d *discoverEnodes) DelDiscoverEnodeKey(key string)  {
	log.Info("开始加锁DelDiscoverEnodeKey")
	d.Lock.Lock()
	defer d.Lock.Unlock()
	log.Info("加锁完成DelDiscoverEnodeKey")
	delete(d.DiscoverNode,key)
}

func (d *discoverEnodes) GetDiscoverEnode(key string)(node *discover.Node)	{
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	var ok bool
	if node,ok =d.DiscoverNode[key];!ok{
		return nil
	}
	return node
}