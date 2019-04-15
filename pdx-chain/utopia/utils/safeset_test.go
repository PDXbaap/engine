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
package utils

import (
	"fmt"
	"net"
	"pdx-chain/common"
	"pdx-chain/p2p/discover"
	"testing"
	"time"
)

type BlockchainNode struct {
	IP       net.IP          // len 4 for IPv4 or 16 for IPv6
	UDP, TCP uint16          // port numbers
	ID       discover.NodeID // the node's public key

	// Time when the node was added to the table.
	addedAt time.Time

	Address common.Address
	//number of BlockAssertions since last reset
	CorrectAssertions   uint64
	IncorrectAssertions uint64
}

func TestSafeset(t *testing.T) {

	safeset := SafeSet{
		hmap: make(map[string]interface{}),
	}
	safeset.Add("a", &BlockchainNode{TCP: 102})
	bytes, e := safeset.Encode()
	if e != nil {
		fmt.Println(e)
	} else {
		fmt.Println("asd", len(bytes))
	}
	s2 := SafeSet{
		hmap: make(map[string]interface{}),
	}

	s2.Decode(bytes)
	fmt.Println("")
}
