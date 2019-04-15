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
package types

import (
	"math/big"
	"net"
	core_types "pdx-chain/core/types"
	"pdx-chain/p2p/discover"
	"pdx-chain/rlp"
)

const (
	NORNAML_BLOCK = 1
)

type BlockMiningReq struct {
	NodeID discover.NodeID

	IP *net.IP

	Port uint16

	Kind int
	// rank in master list
	Rank uint32
	// block number to mine
	Number uint64

	IsLandID string

	Empty bool

	CNumber *big.Int
}

// Set in types.Block.header.Extra
type BlockExtra struct {
	NodeID discover.NodeID
	// Miner's rank for this block
	Rank uint32
	// commit block number
	CNumber *big.Int
	// Signature of this block
	//V, R, S *big.Int
	Signature []byte
	// Use the data signed, the signature to get public key (NodeID)
	// together with IP and UDP/TCP to form discover.Node object.
	IP   net.IP
	Port uint16
	IsLandID string
	// For extension only
	Empty bool
	Extra []byte
}

func (a *BlockExtra) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(a)
}

func (a *BlockExtra) Decode(data []byte) error {
	return rlp.DecodeBytes(data, a)
}

func BlockExtraDecode(normalBlock *core_types.Block) BlockExtra {
	var blockExtra BlockExtra
	blockExtra.Decode(normalBlock.Extra())
	return blockExtra
}
