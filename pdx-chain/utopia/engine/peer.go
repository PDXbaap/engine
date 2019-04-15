/*************************************************************************
 * Copyright (C) 2016-2019 PDX Technologies, Inc. All Rights Reserved.
 *
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
	"fmt"
	"math/big"
	"sync"
	"time"

	"pdx-chain/common"
	"pdx-chain/core/types"
	"pdx-chain/p2p"
)

type peer struct {
	id string

	*p2p.Peer
	rw p2p.MsgReadWriter

	version  int
	forkDrop *time.Timer

	head common.Hash
	td   *big.Int
	lock sync.RWMutex

	queuedProps chan *types.Block
	term        chan struct{}
}

func newPeer(version int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	return &peer{
		Peer:        p,
		rw:          rw,
		version:     version,
		id:          fmt.Sprintf("%x", p.ID().Bytes()[:8]),
		queuedProps: make(chan *types.Block, 4),
		term:        make(chan struct{}),
	}
}

func (p *peer) broadcast() {
	for {
		select {
		case block := <-p.queuedProps:
			if err := p.SendNewBlock(block); err != nil {
				return
			}
		case <-p.term:
			return
		}
	}
}

// SendNewBlock propagates an entire block to a remote peer.
func (p *peer) SendNewBlock(block *types.Block) error {
	return p2p.Send(p.rw, 0x07, []interface{}{block})
}

// close signals the broadcast goroutine to terminate.
func (p *peer) close() {
	close(p.term)
}

// String implements fmt.Stringer.
func (p *peer) String() string {
	return fmt.Sprintf("Peer %s [%s]", p.id,
		fmt.Sprintf("eth/%2d", p.version),
	)
}
