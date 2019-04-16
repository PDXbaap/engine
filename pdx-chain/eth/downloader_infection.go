package eth

import (
	"github.com/pborman/uuid"
	"pdx-chain/common"
	"pdx-chain/core"
	"pdx-chain/core/types"
	"pdx-chain/log"
	"pdx-chain/p2p"
	"sync/atomic"
)

type infection struct {
	initiative   int32
	pm           *ProtocolManager
	initiativeCh chan core.FetchingBlocksEvent
	responseCh   chan cachedBlocksResponse
	requestMap   map[string]*demand
}

type demand struct {
	originalHashes []*common.Hash
	missHashes     []*common.Hash
	peer           *peer
}

func newInfection(pm *ProtocolManager) *infection {
	return &infection{
		pm:           pm,
		initiativeCh: make(chan core.FetchingBlocksEvent, 1),
		responseCh:   make(chan cachedBlocksResponse, 1),
		requestMap:   make(map[string]*demand),
	}
}

func (di *infection) fetchFromRemote(blocks []*common.Hash) {
	if swapped := atomic.CompareAndSwapInt32(&di.initiative, 0, 1); swapped {
		requestId := uuid.New()
		request := CachedBlocksRequest{Hashes: blocks, RequestId: requestId}
		err := p2p.Send(di.pm.peers.BestPeer().rw, GetBlocksByHashMsg, request)
		if err != nil {
			log.Error("rlp err", "err", err.Error())
		}
		log.Info("send request to fetch from remote", "requestId", requestId)
	}
}

// B transfer request from A to another remote C
func (di *infection) fetchByTransfer(request *CachedBlocksRequest, missHashes []*common.Hash, peer *peer) {
	demand := &demand{originalHashes: request.Hashes, missHashes: missHashes, peer: peer}
	di.requestMap[request.RequestId] = demand
	if atomic.LoadInt32(&di.initiative) == 0 {
		requestTransfer := CachedBlocksRequest{Hashes: missHashes, RequestId: request.RequestId}
		err := p2p.Send(di.pm.peers.BestPeer().rw, GetBlocksByHashMsg, requestTransfer)
		log.Info("send relay request to fetch from remote", "requestId", request.RequestId)
		if err != nil {
			log.Error("rlp err", "err", err.Error())
		}
	}
}

func (di *infection) start() {
	di.pm.blockchain.SubscribeFetchingEvent(di.initiativeCh)
	for {
		select {
		case event := <-di.initiativeCh:
			di.fetchFromRemote(event.Hashes)
		case response := <-di.responseCh:
			demand := di.requestMap[response.RequestId]
			//response is from initiative,change initiative status,notify block chain,iterate requestMap and when demand is completed send msg back to original
			if demand == nil {
				if swapped := atomic.CompareAndSwapInt32(&di.initiative, 1, 0); !swapped {
					continue
				}
				if len(di.pm.blockchain.HandleMissCh) != 0 {
					<-di.pm.blockchain.HandleMissCh
				}
				di.pm.blockchain.HandleMissCh <- struct{}{}
			maploop:
				for k, v := range di.requestMap {
					var blocks []*types.Block
					for _, hash := range v.originalHashes {
						block := di.pm.blockchain.GetCacheBlock(*hash)
						if block != nil {
							blocks = append(blocks, block)
						} else {
							continue maploop
						}
					}
					data := cachedBlocksResponse{Blocks: blocks, RequestId: k}
					err := v.peer.SendCachedBlocksByHash(data)
					if err != nil {
						log.Error("rlp err", "err", err.Error())
					} else {
						delete(di.requestMap, k)
					}
				}

			} else { //response is from transfer,just send back to original
				var blocks []*types.Block
				for _, hash := range demand.originalHashes {
					blocks = append(blocks, di.pm.blockchain.GetCacheBlock(*hash))
				}
				data := cachedBlocksResponse{Blocks: blocks, RequestId: response.RequestId}
				err := demand.peer.SendCachedBlocksByHash(data)
				if err != nil {
					log.Error("rlp err", "err", err.Error())
				}
				delete(di.requestMap, response.RequestId)
			}

		}
	}
}

func (di *infection) stop() {
	close(di.responseCh)
	//close(di.initiativeCh)
	log.Info("stop infection")
}
