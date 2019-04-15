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
	"pdx-chain/common"
	core_types "pdx-chain/core/types"
	"pdx-chain/p2p/discover"
	"pdx-chain/rlp"
)

// NewAssertBlockEvent is posted when a block has been imported.
type NewAssertBlockEvent struct {
	Block *core_types.Block
	Nodes []*discover.Node
}

// Set in types.Block.header.Extra.Extra
type AssertExtra struct {

	// The block number and hash of last "commit" block
	LatestCommitBlockNumber *big.Int
	LatestCommitBlockHash   common.Hash

	// Asserted block path, since last committed normal block
	BlockPath []common.Hash

	// Signature of block path
	Signature []byte

	// For extension only
	Extra []byte
}

func (a *AssertExtra) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(a)
}

func (a *AssertExtra) Decode(data []byte) error {
	return rlp.DecodeBytes(data, a)
}

func AssertExtraDecode(assertBlock *core_types.Block) (BlockExtra,AssertExtra) {
	var blockExtra BlockExtra
	var assertExtra AssertExtra
	blockExtra.Decode(assertBlock.Extra())
	assertExtra.Decode(blockExtra.Extra)

	return blockExtra,assertExtra
}
