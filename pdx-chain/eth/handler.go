// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"pdx-chain/cacheBlock"
	"pdx-chain/crypto"
	"pdx-chain/examineSync"
	"pdx-chain/quorum"
	"sync"
	"sync/atomic"
	"time"

	"pdx-chain/common"
	"pdx-chain/consensus"
	"pdx-chain/consensus/misc"
	"pdx-chain/core"
	"pdx-chain/core/types"
	"pdx-chain/eth/downloader"
	"pdx-chain/eth/fetcher"
	"pdx-chain/ethdb"
	"pdx-chain/event"
	"pdx-chain/log"
	"pdx-chain/p2p"
	"pdx-chain/p2p/discover"
	"pdx-chain/params"
	"pdx-chain/rlp"
	"pdx-chain/utopia/engine"
	utopia_types "pdx-chain/utopia/types"
)

const (
	softResponseLimit = 2 * 1024 * 1024 // Target maximum size of returned blocks, headers or node data.
	estHeaderRlpSize  = 500             // Approximate size of an RLP encoded block header

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096
)

var (
	daoChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the DAO handshake challenge
)

//var commitMapCache sync.Map
// errIncompatibleConfig is returned if the requested protocols and configs are
// not compatible (low protocol version restrictions and high requirements).
var errIncompatibleConfig = errors.New("incompatible configuration")

func errResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

type ProtocolManager struct {
	networkID uint64

	fastSync  uint32 // Flag whether fast sync is enabled (gets disabled if we already have blocks)
	acceptTxs uint32 // Flag whether we're considered synchronised (enables transaction processing)

	txpool      txPool
	blockchain  *core.BlockChain
	chainconfig *params.ChainConfig
	maxPeers    int

	downloader    *downloader.Downloader
	fetcher       *fetcher.Fetcher
	commitFetcher *fetcher.CbFetcher
	peers         *peerSet

	SubProtocols []p2p.Protocol

	eventMux       *event.TypeMux
	txsCh          chan core.NewTxsEvent
	txsSub         event.Subscription
	minedBlockSub  *event.TypeMuxSubscription
	commitBlockSub *event.TypeMuxSubscription
	assertBlockSub *event.TypeMuxSubscription

	BlockChainNodeCh chan *quorum.BlockchainNode
	consensusEngine consensus.Engine
	// channels for fetcher, syncer, txsyncLoop
	newPeerCh   chan *peer
	txsyncCh    chan *txsync
	quitSync    chan struct{}
	noMorePeers chan struct{}

	// wait group is used for graceful shutdowns during downloading
	// and processing
	wg        sync.WaitGroup
	infection *infection     //PDX
	etherbase common.Address //PDX
	chaindb   ethdb.Database //PDX
	missHash  []*common.Hash //PDX

}

// NewProtocolManager returns a new Ethereum sub protocol manager. The Ethereum sub protocol manages peers capable
// with the Ethereum network.
func NewProtocolManager(config *params.ChainConfig, mode downloader.SyncMode, networkID uint64, mux *event.TypeMux, txpool txPool, engineImpl consensus.Engine, blockchain *core.BlockChain, chaindb ethdb.Database, etherbase common.Address) (*ProtocolManager, error) {
	// Create the protocol manager with the base fields
	manager := &ProtocolManager{
		networkID:       networkID,
		eventMux:        mux,
		txpool:          txpool,
		blockchain:      blockchain,
		chainconfig:     config,
		peers:           newPeerSet(),
		newPeerCh:       make(chan *peer),
		noMorePeers:     make(chan struct{}),
		txsyncCh:        make(chan *txsync),
		quitSync:        make(chan struct{}),
		consensusEngine: engineImpl,
		etherbase:       etherbase,
		chaindb:         chaindb,
		missHash:        make([]*common.Hash, 0),
		BlockChainNodeCh: make(chan *quorum.BlockchainNode,1),
	}
	// Figure out whether to allow fast sync or not
	if mode == downloader.FastSync && blockchain.CurrentBlock().NumberU64() > 0 {
		log.Warn("Blockchain not empty, fast sync disabled")
		mode = downloader.FullSync
	}
	if mode == downloader.FastSync {
		manager.fastSync = uint32(1)
		examineSync.ExamineBlockWork =1
	}
	// Initiate a sub-protocol for every implemented version we can handle
	manager.SubProtocols = make([]p2p.Protocol, 0, len(ProtocolVersions))
	for i, version := range ProtocolVersions {
		// Skip protocol version if incompatible with the mode of operation
		if mode == downloader.FastSync && version < eth63 {
			continue
		}
		// Compatible; initialise the sub-protocol
		version := version // Closure for the run
		manager.SubProtocols = append(manager.SubProtocols, p2p.Protocol{
			Name:    ProtocolName,
			Version: version,
			Length:  ProtocolLengths[i],
			Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
				peer := manager.newPeer(int(version), p, rw)
				select {
				case manager.newPeerCh <- peer:
					manager.wg.Add(1)
					defer manager.wg.Done()
					return manager.handle(peer)
				case <-manager.quitSync:
					return p2p.DiscQuitting
				}
			},
			NodeInfo: func() interface{} {
				return manager.NodeInfo()
			},
			PeerInfo: func(id discover.NodeID) interface{} {
				if p := manager.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
					return p.Info()
				}
				return nil
			},
		})
	}
	if len(manager.SubProtocols) == 0 {
		return nil, errIncompatibleConfig
	}
	utopia, _ := engineImpl.(*engine.Utopia)
	// Construct the different synchronisation mechanisms
	manager.downloader = downloader.New(mode, chaindb, manager.eventMux, blockchain, nil, manager.removePeer)
	manager.downloader.Worker = utopia.Worker()
	manager.downloader.Commitchain = blockchain.CommitChain
	validator := func(header *types.Header) error {
		return engineImpl.VerifyHeader(blockchain, header, true)
	}
	heighter := func() uint64 {
		return blockchain.CurrentBlock().NumberU64()
	}
	commitHeighter := func() uint64 {
		return blockchain.CommitChain.CurrentBlock().NumberU64()
	}
	normalNewHeight := func() uint64 {
		return blockchain.CurrentBlock().NumberU64()
	}

	inserter := func(blocks types.Blocks) (int, error) {
		//If fast sync is running, deny importing weird blocks
		if atomic.LoadUint32(&manager.fastSync) == 1 {
			if len(manager.peers.peers)==0||blocks[0].Number().Uint64()<=1{
				atomic.StoreUint32(&manager.fastSync,0)
				atomic.StoreUint32(&examineSync.ExamineBlockWork,0)
			}else {
				log.Warn("Discarded bad propagated block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
				return 0, nil
			}

		}
		atomic.StoreUint32(&manager.acceptTxs, 1) // Mark initial sync done on any fetcher import

		if utopia, ok := manager.consensusEngine.(*engine.Utopia); ok {
			utopia.OnNormalBlock(blocks[0])
			return 0, nil
		} else {
			return manager.blockchain.InsertChain(blocks)
		}
	}
	commitInserter := func(commitBlocks types.Blocks) (int, error) {
		if utopia, ok := engineImpl.(*engine.Utopia); ok {
			for index, block := range commitBlocks {
				err := utopia.OnCommitBlock(block)
				if err != nil {
					log.Error("process commit block", "error", err)
					return index, err
				}
			}
		}
		return 0, nil
	}
	manager.fetcher = fetcher.New(blockchain.GetBlockByHash, validator, manager.BroadcastBlock, heighter, inserter, manager.removePeer)
	manager.commitFetcher = fetcher.NewCBFetcher(blockchain.CommitChain.GetBlockByHash, validator, commitHeighter, commitInserter, manager.removePeer, normalNewHeight)

	manager.infection = newInfection(manager)
	manager.downloader.CbFetcher = manager.commitFetcher
	return manager, nil
}

func (pm *ProtocolManager) removePeer(id string) {
	// Short circuit if the peer was already removed
	peer := pm.peers.Peer(id)
	if peer == nil {
		return
	}
	log.Debug("Removing PDX peer", "peer", id)

	// Unregister the peer from the downloader and Ethereum peer set
	pm.downloader.UnregisterPeer(id)
	if err := pm.peers.Unregister(id); err != nil {
		log.Error("Peer removal failed", "peer", id, "err", err)
	}
	log.Debug("Removing PDX peer----1", "peer", id)

	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer)
	}
	log.Debug("Removing PDX peer----2", "peer", id)

}

func (pm *ProtocolManager) Start(maxPeers int) {
	pm.maxPeers = maxPeers

	// broadcast transactions
	pm.txsCh = make(chan core.NewTxsEvent, txChanSize)
	pm.txsSub = pm.txpool.SubscribeNewTxsEvent(pm.txsCh)
	go pm.txBroadcastLoop()

	// broadcast mined blocks
	pm.minedBlockSub = pm.eventMux.Subscribe(core.NewMinedBlockEvent{})
	pm.commitBlockSub = pm.eventMux.Subscribe(utopia_types.NewCommitBlockEvent{})
	pm.assertBlockSub = pm.eventMux.Subscribe(utopia_types.NewAssertBlockEvent{})
	pm.consensusEngine.(*engine.Utopia).BlockChainNodeFeed.Subscribe(pm.BlockChainNodeCh) //订阅
	go pm.minedBroadcastLoop()
	go pm.commitBroadcastLoop()
	go pm.assertMulticastLoop()
	go pm.peerLoop()
	go pm.blockChainNodeLoop()


	// start sync handlers
	go pm.infection.start()
	go pm.syncer()
	go pm.txsyncLoop()
}

func (pm *ProtocolManager) Stop() {
	log.Info("Stopping Ethereum protocol")

	pm.txsSub.Unsubscribe()        // quits txBroadcastLoop
	pm.minedBlockSub.Unsubscribe() // quits blockBroadcastLoop

	// Quit the sync loop.
	// After this send has completed, no new peers will be accepted.
	pm.noMorePeers <- struct{}{}

	// Quit fetcher, txsyncLoop.
	close(pm.quitSync)

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the peer set.
	// sessions which are already established but not added to pm.peers yet
	// will exit when they try to register.
	pm.peers.Close()

	// Wait for all peer handler goroutines and the loops to come down.
	pm.wg.Wait()
	pm.infection.stop()

	log.Info("Ethereum protocol stopped")
}

func (pm *ProtocolManager) newPeer(pv int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	return newPeer(pv, p, newMeteredMsgWriter(rw))
}

//PDX
func (pm *ProtocolManager) SyncIsland(cNum int, blockExtra utopia_types.BlockExtra, commitExtraIsland bool) {

	log.Info("SyncIsland开始删除commit")
	if atomic.LoadInt32(&pm.downloader.AssociatedSynchronising) == 1 {
		log.Warn("SyncIsland或者SyncNormalDeleteCommit半生commit正在同步")
		return
	}

	currentCommitNum := pm.blockchain.CommitChain.CurrentBlock().NumberU64()

	//如果分叉前的高度比当前高度大,就不删了
	if uint64(cNum)-1 > currentCommitNum {
		return
	}

	for i := uint64(cNum) - 1; i <= currentCommitNum; i++ {

		block := pm.blockchain.CommitChain.GetBlockByNum(i)
		log.Info("要删除的高度", "分叉前的高度", cNum, "当前要删的高度", i, "要删除到哪里", currentCommitNum)
		//先删除 在同步
		if block == nil {
			continue
		}
		//先删除 在同步
		engine.CommitDeRepetition.Del(block.NumberU64())           //commit删除缓存
		engine.AssociatedCommitDeRepetition.Del(block.NumberU64()) //commit删除缓存
		core.DeleteCommitBlock(pm.chaindb, block.Hash(), block.NumberU64(), *pm.blockchain.CommitChain)
		engine.CommitBlockQueued.DelToProcessCommitBlockQueueMap(i) //删除toProcessCommit缓存
		quorum.CommitHeightToConsensusQuorum.Del(block.NumberU64(), pm.chaindb)
	}
	//如果当前的commit高度已经比裂脑前的高度小,就用当前高度

	localCnum := uint64(cNum) - 2
	if localCnum > currentCommitNum {
		localCnum = currentCommitNum
	}
	if localCnum == 0 {
		localCnum = 1
	}
	log.Info("commit回滚高度", "cNum", cNum, "selfCnum", localCnum, "当前commit高度", currentCommitNum)
	localCommit := pm.blockchain.CommitChain.GetBlockByNum(localCnum)
	core.SetCurrentCommitBlock(pm.blockchain.CommitChain, localCommit)

	log.Info("selfCommit", "selfCommit高度", localCommit.NumberU64())
	_,commitExtra := utopia_types.CommitExtraDecode(localCommit)
	rollbackHeight := commitExtra.NewBlockHeight.Uint64()
	currentBlockNumber := pm.blockchain.CommitChain.CurrentBlock().NumberU64()
	log.Info("currentCommit高度", "commit高度", currentBlockNumber)
	//处理Normal
	currentBlockNormalNum := pm.blockchain.CurrentBlock().NumberU64()
	pm.blockchain.SetHead(rollbackHeight)
	log.Info("normal回滚高度", "normal高度", rollbackHeight)
	for i := rollbackHeight + 1; i <= currentBlockNormalNum; i++ {
		pm.blockchain.CommitChain.DeleteHeightMap(i)
		engine.NormalBlockQueued.DelToProcessNormalBlockQueueMap(i) //删除toProcessNormal
	}

	//同步完成后存对方的岛屿ID,改变自己岛屿状态
	utopiaEng := pm.consensusEngine.(*engine.Utopia)
	utopiaEng.SetIslandIDState(blockExtra.IsLandID, utopiaEng.DB())
	utopiaEng.SetIslandState(commitExtraIsland, utopiaEng.DB())

	atomic.StoreInt32(&downloader.IslandDel, 1) //normal同步就不要删除啦

	//pm.downloader.AssociatedCH <- true
}

//检查节点是否在委员会中,如果在可以清除节点,如果不在,保留节点,有可能是新加入的节点
func (pm *ProtocolManager)ExamineConsensusQuorum(peer string) bool{
	commitHeight:=pm.blockchain.CommitChain.CurrentBlock().NumberU64()-1
	consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(uint64(commitHeight)-1, pm.chaindb)
	if !ok{
		log.Warn("委员会获取失败不能删节点")
		return true  //委员会获取失败 不能删
	}
	address:=examineSync.IDAndBlockAddress.GetAddIDAndAddress(peer)
	 BlockNode, b := quorum.BlockChainNodeSet.Get(address, pm.chaindb)
	 if !b{
	 	log.Warn("BlockChainNode获取失败不能删节点")
		return true //BlockChainNode获取失败 不能删
	}
	for _,address:=range consensusQuorum.Hmap{
		if BlockNode.Address==address{
			log.Info("节点在委员会中可以删除","peer",BlockNode.Address.Hash(),"委员会",address.Hash())
			return false  //可以删
		}

	}
	return true //不在委员会中
}

//删除超出列表的peer
func (pm *ProtocolManager)spillRemovePeer(num int) {
	delNum:=0
	for nodeId:=range pm.peers.peers{
		if examineSync.PeerExamineSync.ExaminePeer(nodeId){ //是否是正同步中的区块
			continue
		}
		if pm.ExamineConsensusQuorum(nodeId){ //检查要删除的节点是否在委员会中,防止删除新加入的节点
			continue
		}
		pm.removePeer(nodeId)
		delNum++
		if delNum==num{
				break
			}

		}
	}

func(pm *ProtocolManager)  peerLoop()  {
	for{
		rand.Seed(time.Now().UnixNano())
		time.Sleep(time.Duration(int(engine.BlockDelay)*(10+rand.Intn(10)))*time.Millisecond)
		if pm.peers.Len() >= pm.maxPeers+1 {
			//return p2p.DiscTooManyPeers
			log.Info("peer超出范围需要删除")
			pm.spillRemovePeer(pm.peers.Len()-pm.maxPeers)
		}
		if pm.peers.Len()==0 {
			log.Info("peer数量为0,开始链接StaticNodes")
			utopia:=pm.consensusEngine.(*engine.Utopia)
			staticNode := utopia.Server().StaticNodes
			for _,node:=range staticNode{
				utopia.Server().AddPeer(node)
			}
		}

	}

}


// handle is the callback invoked to manage the life cycle of an eth peer. When
// this function terminates, the peer is disconnected.
func (pm *ProtocolManager) handle(p *peer) error {
	// Ignore maxPeers if this is a trusted peer
	//if pm.peers.Len() >= pm.maxPeers && !p.Peer.Info().Network.Trusted {
	//	//return p2p.DiscTooManyPeers
	//	log.Info("peer超出范围需要删除")
	//	pm.spillRemovePeer()
	//}

	p.Log().Debug("PDX peer connected", "name", p.Name())

	// Execute the Ethereum handshake
	var (
		genesis = pm.blockchain.Genesis()
		head    = pm.blockchain.CurrentHeader()
		hash    = head.Hash()
		number  = head.Number.Uint64()
		td      = pm.blockchain.GetTd(hash, number)
	)
	if err := p.Handshake(pm.networkID, td, hash, genesis.Hash(), pm.blockchain.CommitChain.CurrentBlock().NumberU64()); err != nil {
		p.Log().Debug("PDX handshake failed", "err", err)
		return err
	}
	if rw, ok := p.rw.(*meteredMsgReadWriter); ok {
		rw.Init(p.version)
	}
	// Register the peer locally
	if err := pm.peers.Register(p); err != nil {
		p.Log().Error("PDX peer registration failed", "err", err)
		return err
	}
	defer pm.removePeer(p.id)

	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	if err := pm.downloader.RegisterPeer(p.id, p.version, p,p.Peer); err != nil {
		return err
	}
	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.
	pm.syncTransactions(p)

	// If we're DAO hard-fork aware, validate any remote peer with regard to the hard-fork
	if daoBlock := pm.chainconfig.DAOForkBlock; daoBlock != nil {
		// Request the peer's DAO fork header for extra-data validation
		if err := p.RequestHeadersByNumber(daoBlock.Uint64(), 1, 0, false); err != nil {
			return err
		}
		// Start a timer to disconnect if the peer doesn't reply in time
		p.forkDrop = time.AfterFunc(daoChallengeTimeout, func() {
			p.Log().Debug("Timed out DAO fork-check, dropping")
			pm.removePeer(p.id)
		})
		// Make sure it's cleaned up if the peer dies off
		defer func() {
			if p.forkDrop != nil {
				p.forkDrop.Stop()
				p.forkDrop = nil
			}
		}()
	}
	// main loop. handle incoming messages.
	for {
		if err := pm.handleMsg(p); err != nil {
			p.Log().Debug("PDX message handling failed", "err", err)
			return err
		}
	}
}

var commitIslandSync int32

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func (pm *ProtocolManager) handleMsg(p *peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	msg, err := p.rw.ReadMsg()
	if err != nil {
		log.Error("读取消息失败", "err", err)
		return err
	}
	if msg.Size > ProtocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch {
	case msg.Code == StatusMsg:
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")

		// Block header query, collect the requested headers and reply
	case msg.Code == GetBlockHeadersMsg:
		log.Info("request GetBlockHeadersMsg ")
		// Decode the complex header query
		var query getBlockHeadersData
		if err := msg.Decode(&query); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		hashMode := query.Origin.Hash != (common.Hash{})
		maxNonCanonical := uint64(100)

		// Gather headers until the fetch or network limits is reached
		var (
			bytes        common.StorageSize
			headers      []*types.Header
			commitblocks []*types.Block
			unknown      bool
		)
		for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit && len(headers) < downloader.MaxHeaderFetch {
			// Retrieve the next header satisfying the query
			var origin *types.Header
			if hashMode {
				normalBlcok :=cacheBlock.CacheBlocks.GetBlock(query.Origin.Hash)
				if normalBlcok==nil{
					origin = pm.blockchain.GetHeaderByHash(query.Origin.Hash)
					if origin != nil {
						query.Origin.Number = origin.Number.Uint64()

					} else {
						origin = pm.blockchain.GetHeader(query.Origin.Hash, query.Origin.Number)
					}
				}else {
					origin=normalBlcok.Header()
				}

			} else {
				origin = pm.blockchain.GetHeaderByNumber(query.Origin.Number)
			}
			if origin == nil {
				log.Error("没有找到")
				break
			}
			log.Info("origin的高度","origin高度",origin.Number.Uint64())
			//PDX collect commit blocks and send to remote peer
			if query.Skip == 0 && !hashMode {
				commitNums := pm.blockchain.CommitChain.GetCommitNumByNormalHeight(origin.Number.Uint64())
				if len(commitNums) > 0 {
					for _, commitNum := range commitNums {
						if commitNum == 0 && origin.Number.Uint64() == 0 {
							continue
						}
						if commitNum != 1 && len(commitblocks) == 0 {
							commitBlock := pm.blockchain.CommitChain.GetBlockByNum(commitNum - 1)
							if commitBlock == nil {
								continue
							}
							commitblocks = append(commitblocks, commitBlock)

						}
						commitBlock := pm.blockchain.CommitChain.GetBlockByNum(commitNum)
						if commitBlock == nil {
							continue
						}
						commitblocks = append(commitblocks, commitBlock)
					}
				}
			}
			headers = append(headers, origin)
			bytes += estHeaderRlpSize

			// Advance to the next header of the query
			switch {
			case hashMode && query.Reverse:
				// Hash based traversal towards the genesis block
				ancestor := query.Skip + 1
				if ancestor == 0 {
					unknown = true
				} else {
					query.Origin.Hash, query.Origin.Number = pm.blockchain.GetAncestor(query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
					unknown = query.Origin.Hash == common.Hash{}
				}
			case hashMode && !query.Reverse:
				// Hash based traversal towards the leaf block
				var (
					current = origin.Number.Uint64()
					next    = current + query.Skip + 1
				)
				if next <= current {
					infos, _ := json.MarshalIndent(p.Peer.Info(), "", "  ")
					p.Log().Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
					unknown = true
				} else {
					if header := pm.blockchain.GetHeaderByNumber(next); header != nil {
						nextHash := header.Hash()
						expOldHash, _ := pm.blockchain.GetAncestor(nextHash, next, query.Skip+1, &maxNonCanonical)
						if expOldHash == query.Origin.Hash {
							query.Origin.Hash, query.Origin.Number = nextHash, next
						} else {
							unknown = true
						}
					} else {
						unknown = true
					}
				}
			case query.Reverse:
				// Number based traversal towards the genesis block
				if query.Origin.Number >= query.Skip+1 {
					query.Origin.Number -= query.Skip + 1
				} else {
					unknown = true
				}

			case !query.Reverse:
				// Number based traversal towards the leaf block
				query.Origin.Number += query.Skip + 1
			}
		}
		if len(commitblocks) > 0 {
			for _,block:=range commitblocks{

				log.Info("commit发送","commitBlock",block.NumberU64())
			}
			err := p.SendAssociatedCommitBlocks(commitblocks)
			if err != nil {
				log.Error("err", err.Error())
			}
		}
		log.Info("发送的headers是","peer-id",p.id,"headers长度",len(headers))
		return p.SendBlockHeaders(headers)

	case msg.Code == GetCommitBlocksMsg:
		// Decode the complex header query
		var query getBlockHeadersData
		if err := msg.Decode(&query); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}

		// Gather headers until the fetch or network limits is reached
		var (
			bytes   common.StorageSize
			headers []*types.Block
		)
		for len(headers) < int(query.Amount) && bytes < softResponseLimit && len(headers) < downloader.MaxHeaderFetch {
			// Retrieve the next header satisfying the query
			var origin *types.Block
			origin =cacheBlock.CacheBlocks.GetBlock(query.Origin.Hash)
			if origin == nil {
				origin = pm.blockchain.CommitChain.GetBlockByHash(query.Origin.Hash)
				if origin == nil {
					break
				}
			}

			headers = append(headers, origin)
			bytes += estHeaderRlpSize

			// Advance to the next header of the query
			switch {
			case !query.Reverse:
				// Number based traversal towards the leaf block
				query.Origin.Number += query.Skip + 1
			}
		}

		return p.SendCommitBlocks(headers)
	case msg.Code == GetLatestCommitBlockMsg:
		block := pm.blockchain.CommitChain.CurrentBlock()
		var blocks []*types.Block
		blocks = append(blocks, block)
		p.SendCommitBlocks(blocks)

	case msg.Code == BlockHeadersMsg:
		// A batch of headers arrived to one of our previous requests
		var headers []*types.Header
		if err := msg.Decode(&headers); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// If no headers were received, but we're expending a DAO fork check, maybe it's that
		if len(headers) == 0 && p.forkDrop != nil {
			// Possibly an empty reply to the fork header checks, sanity check TDs
			verifyDAO := true

			// If we already have a DAO header, we can check the peer's TD against it. If
			// the peer's ahead of this, it too must have a reply to the DAO check
			if daoHeader := pm.blockchain.GetHeaderByNumber(pm.chainconfig.DAOForkBlock.Uint64()); daoHeader != nil {
				if _, td := p.Head(); td.Cmp(pm.blockchain.GetTd(daoHeader.Hash(), daoHeader.Number.Uint64())) >= 0 {
					verifyDAO = false
				}
			}
			// If we're seemingly on the same chain, disable the drop timer
			if verifyDAO {
				p.Log().Debug("Seems to be on the same side of the DAO fork")
				p.forkDrop.Stop()
				p.forkDrop = nil
				return nil
			}
		}
		// Filter out any explicitly requested headers, deliver the rest to the downloader
		filter := len(headers) == 1
		if filter {
			// If it's a potential DAO fork check, validate against the rules
			if p.forkDrop != nil && pm.chainconfig.DAOForkBlock.Cmp(headers[0].Number) == 0 {
				// Disable the fork drop timer
				p.forkDrop.Stop()
				p.forkDrop = nil

				// Validate the header and either drop the peer or continue
				if err := misc.VerifyDAOHeaderExtraData(pm.chainconfig, headers[0]); err != nil {
					p.Log().Debug("Verified to be on the other side of the DAO fork, dropping")
					return err
				}
				p.Log().Debug("Verified to be on the same side of the DAO fork")
				return nil
			}
			// Irrelevant of the fork checks, send the header to the fetcher just in case
			headers = pm.fetcher.FilterHeaders(p.id, headers, time.Now())
		}
		if len(headers) > 0 || !filter {
			err := pm.downloader.DeliverHeaders(p.id, headers)
			if err != nil {
				log.Debug("Failed to deliver headers", "err", err)
			}
		}

	case msg.Code == GetBlockBodiesMsg:
		// Decode the retrieval message
		log.Info("GetBlockBodiesMsg")
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather blocks until the fetch or network limits is reached
		var (
			hash   common.Hash
			bytes  int
			bodies []rlp.RawValue
		)
		for bytes < softResponseLimit && len(bodies) < downloader.MaxBlockFetch {
			// Retrieve the hash of the next block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested block body, stopping if enough was found
			block := cacheBlock.CacheBlocks.GetBlock(hash)
			if block==nil{
				if data := pm.blockchain.GetBodyRLP(hash); len(data) != 0 {
					bodies = append(bodies, data)
					bytes += len(data)
				}
			}else {
				//pdx 获取rlpBodie
				data, err := rlp.EncodeToBytes(block.Body())
				if err!=nil{
					return nil
				}
				bodies = append(bodies, data)
				bytes += len(data)
			}

		}
		return p.SendBlockBodiesRLP(bodies)

	case msg.Code == BlockBodiesMsg:
		// A batch of block bodies arrived to one of our previous requests
		var request blockBodiesData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver them all to the downloader for queuing
		transactions := make([][]*types.Transaction, len(request))
		uncles := make([][]*types.Header, len(request))

		for i, body := range request {
			transactions[i] = body.Transactions
			uncles[i] = body.Uncles
		}
		// Filter out any explicitly requested bodies, deliver the rest to the downloader
		filter := len(transactions) > 0 || len(uncles) > 0
		if filter {
			transactions, uncles = pm.fetcher.FilterBodies(p.id, transactions, uncles, time.Now())
		}
		if len(transactions) > 0 || len(uncles) > 0 || !filter {
			err := pm.downloader.DeliverBodies(p.id, transactions, uncles)
			if err != nil {
				log.Debug("Failed to deliver bodies", "err", err)
			}
		}

	case p.version >= eth63 && msg.Code == GetNodeDataMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash  common.Hash
			bytes int
			data  [][]byte
		)
		for bytes < softResponseLimit && len(data) < downloader.MaxStateFetch {
			// Retrieve the hash of the next state entry
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested state entry, stopping if enough was found
			if entry, err := pm.blockchain.TrieNode(hash); err == nil {
				data = append(data, entry)
				bytes += len(entry)
			}
		}
		return p.SendNodeData(data)

	case p.version >= eth63 && msg.Code == NodeDataMsg:
		// A batch of node state data arrived to one of our previous requests
		var data [][]byte
		if err := msg.Decode(&data); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the downloader
		if err := pm.downloader.DeliverNodeData(p.id, data); err != nil {
			log.Debug("Failed to deliver node state data", "err", err)
		}

	case p.version >= eth63 && msg.Code == GetReceiptsMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash     common.Hash
			bytes    int
			receipts []rlp.RawValue
		)
		for bytes < softResponseLimit && len(receipts) < downloader.MaxReceiptFetch {
			// Retrieve the hash of the next block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested block's receipts, skipping if unknown to us
			results := pm.blockchain.GetReceiptsByHash(hash)
			if results == nil {
				if header := pm.blockchain.GetHeaderByHash(hash); header == nil || header.ReceiptHash != types.EmptyRootHash {
					continue
				}
			}
			// If known, encode and queue for response packet
			if encoded, err := rlp.EncodeToBytes(results); err != nil {
				log.Error("Failed to encode receipt", "err", err)
			} else {
				receipts = append(receipts, encoded)
				bytes += len(encoded)
			}
		}
		return p.SendReceiptsRLP(receipts)

	case p.version >= eth63 && msg.Code == ReceiptsMsg:
		// A batch of receipts arrived to one of our previous requests
		var receipts [][]*types.Receipt
		if err := msg.Decode(&receipts); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the downloader
		if err := pm.downloader.DeliverReceipts(p.id, receipts); err != nil {
			log.Debug("Failed to deliver receipts", "err", err)
		}

	case msg.Code == NewBlockHashesMsg:
		var announces newBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Mark the hashes as present at the remote node
		for _, block := range announces {
			log.Info("收到Blockhash","hash",block.Hash)
			p.MarkBlock(block.Hash)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make(newBlockHashesData, 0, len(announces))
		for _, block := range announces {
			if !pm.blockchain.HasBlock(block.Hash, block.Number) {
				unknown = append(unknown, block)
			}
		}

		for _, block := range unknown {
			if cacheBlock.CacheBlocks.GetBlock(block.Hash)==nil{
				pm.fetcher.Notify(p.id, block.Hash, block.Number, time.Now(), p.RequestOneHeader, p.RequestBodies)
			}
		}

	case msg.Code == NewBlockMsg:
		// Retrieve and decode the propagated block
		var request newBlockData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		//if !pm.verifyQualification(request.Block)&&pm.fastSync==0{
		//	return nil
		//}
		//去重1 查询是否已经保存
		localBlockData := pm.blockchain.SearchingBlock(request.Block.Hash(), request.Block.NumberU64())
		if localBlockData != nil {
			return nil
		}
		//去重2 查询是否已经接收过
		log.Info("开始去重","高度",request.Block.NumberU64(),"hash",request.Block.Hash())
		if !engine.NormalDeRepetition.Add(request.Block.NumberU64(), request.Block.Hash()) {
			return nil
		}
		//去重3 查询GetBlock
		if cacheBlock.CacheBlocks.GetBlock(request.Block.Hash())!=nil {
			return nil
		}

		request.Block.ReceivedAt = msg.ReceivedAt
		request.Block.ReceivedFrom = p

		blockExtra := utopia_types.BlockExtraDecode(request.Block)

		utopiaEng := pm.consensusEngine.(*engine.Utopia)
		localIsLandID, _, _, _ := engine.IslandLoad(utopiaEng)
		var oneOfMasters bool
		//判断岛屿Id是否一样,如果不一样 直接丢掉
		currentCommitNum := pm.blockchain.CommitChain.CurrentBlock().NumberU64()
		if blockExtra.IsLandID != "" && blockExtra.IsLandID != localIsLandID &&currentCommitNum>=1{
			consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(currentCommitNum, pm.chaindb)
			if !ok{
				return nil
			}
			for _, address := range consensusQuorum.Keys() {
				if request.Block.Coinbase().String() == address {
					oneOfMasters = true
					break
				}
				quorumLen := consensusQuorum.Len()
				if !oneOfMasters || quorumLen != 1 {
					//本地大陆,收到岛屿,判断父hash是否和本地currenthash相同,如果相同,直接接收
					if request.Block.ParentHash() != pm.blockchain.CurrentBlock().Hash() {
						log.Error("收到无效的岛屿块,直接丢掉", "高度", request.Block.NumberU64(), "父hash", request.Block.ParentHash())
						return nil
					}
				}
			}
		}
		// Mark the peer as owning the block and schedule it for import
		log.Info("receive Normal block", "Num", request.Block.NumberU64(), "Rank", blockExtra.Rank, "Hash", request.Block.Hash())
		p.MarkBlock(request.Block.Hash())
		pm.fetcher.Enqueue(p.id, request.Block)
		//cache block for
		pm.blockchain.CacheBlock(request.Block)

		// Assuming the block is importable by the peer, but possibly not yet done so,
		// calculate the head hash and TD that the peer truly must have.
		var (
			trueHead = request.Block.ParentHash()
			trueTD   = new(big.Int).Sub(request.TD, request.Block.Difficulty())
			trueNum  = request.CommitNum
		)
		// Update the peers total difficulty if better than the previous
		if _, td := p.Head(); trueTD.Cmp(td) > 0 {
			p.SetHead(trueHead, trueTD, trueNum)
			// Schedule a sync if above ours. Note, this will not fire a sync for a gap of
			// a singe block (as the true TD is below the propagated block), however this
			// scenario should easily be covered by the fetcher.
			currentBlock := pm.blockchain.CurrentBlock()
			commitBlockNum := pm.blockchain.CommitChain.CurrentBlock().NumberU64()
			getTd := pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64())
			if getTd==nil{
				getTd=big.NewInt(0)
			}
			if trueTD.Cmp(getTd) > 0 ||
				(blockExtra.CNumber.Uint64() > commitBlockNum+1 && blockExtra.IsLandID != localIsLandID) {
				log.Debug("半路发现有差距,进行同步", "当前链上的Normal高度", currentBlock.NumberU64(), "收到的Normal高度", request.Block.NumberU64(),
					"链上的commit高度", commitBlockNum, "收到的commit高度", blockExtra.CNumber.Uint64())
				go pm.synchronise(p, request.Block.NumberU64())
			}
		}

	case msg.Code == TxMsg:
		// Transactions arrived, make sure we have a valid and fresh chain to handle them
		if atomic.LoadUint32(&pm.acceptTxs) == 0 {
			break
		}
		// Transactions can be processed, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		for i, tx := range txs {
			//log.Info("receive tx Msg", "tx hash", tx.Hash())
			// Validate and mark the remote transaction
			if tx == nil {
				return errResp(ErrDecode, "transaction %d is nil", i)
			}
			p.MarkTransaction(tx.Hash())
		}
		go pm.txpool.AddRemotes(txs)

		//PDX: call pdxc consensus engine
	case msg.Code == BlockAssertMsg:

		var assertBlock types.Block

		if err := msg.Decode(&assertBlock); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		if utopia, ok := pm.consensusEngine.(*engine.Utopia); ok {
			assertBlock.ReceivedAt = msg.ReceivedAt
			assertBlock.ReceivedFrom = p
			utopia.OnAssertBlock(&assertBlock)
		}

		//PDX 接收commitHash
	case msg.Code == NewCommitBlockHashesMsg:
		var announces newCommitBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make(newCommitBlockHashesData, 0, len(announces))
		for _, block := range announces {
			if !pm.blockchain.CommitChain.HasBlock(block.Hash, block.Number) {
				unknown = append(unknown, block)
			}
		}

		for _, block := range unknown {
			if cacheBlock.CacheBlocks.GetBlock(block.Hash)==nil{
				pm.commitFetcher.Notify(p.id, block.Hash, block.Number, time.Now(), p.RequestCommitsByHash)
			}
		}


	case msg.Code == BlockCommitMsg:

		if pm.blockchain.CurrentBlock().NumberU64() < 1 {
			return nil
		}

		var commitBlock types.Block
		if err := msg.Decode(&commitBlock); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		//if !pm.verifyQualification(&commitBlock)&&pm.fastSync==0{
		//	return nil
		//}
		if !pm.verifyRepeatCommitBlock(commitBlock){
			log.Info("BlockCommitMsg重复接收")
			return nil
		}
			blockExtra := utopia_types.BlockExtraDecode(&commitBlock)
		log.Debug("receive broadcast commit block ", "num:", commitBlock.NumberU64(), "ip", blockExtra.IP, "hash", commitBlock.Hash().Hex(), "rank", blockExtra.Rank, "收到的islandID", blockExtra.IsLandID)
		commitBlock.ReceivedAt = msg.ReceivedAt
		commitBlock.ReceivedFrom = p
			err := pm.verifyCommitBlockIsland(commitBlock, p)
			if err != nil {
				return err

		}


	case msg.Code == CommitBlocksMsg:
		var commitBlocks []*types.Block
		if err := msg.Decode(&commitBlocks); err != nil {
			log.Error("decode err", "err", err.Error())
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		if len(commitBlocks) > 0 {
			log.Info("receive sync commitblocks msg", "firstNum", commitBlocks[0].NumberU64(), "lastNum", commitBlocks[len(commitBlocks)-1].NumberU64())
			for _, block := range commitBlocks {
				block.ReceivedAt = msg.ReceivedAt
				block.ReceivedFrom = p
				if !pm.verifyRepeatCommitBlock(*block){
					log.Info("CommitBlocksMsg重复接收")
					return nil
				}
				//增加去重
				//if !pm.verifyQualification(block)&&pm.fastSync==0{
				//	return nil
				//}
					err := pm.verifyCommitBlockIsland(*block, p)
					if err != nil {
						return err
					}
				}

		}

	case msg.Code == AssociatedCommitBlocksMsg:
		var commitBlocks []*types.Block
		if err := msg.Decode(&commitBlocks); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		if len(commitBlocks) > 0 {
			for _, block := range commitBlocks {
				log.Info("commitBlocks", "commit高度", block.NumberU64(), "hash", block.Hash())
				block.ReceivedAt = msg.ReceivedAt
				block.ReceivedFrom = p
			}
			pm.downloader.ProcessCommitChAssociated <- commitBlocks
			log.Info("半生传输完毕", "commitBlocks长度", len(commitBlocks))
		}

	case msg.Code == GetBlocksByHashMsg:
		var request CachedBlocksRequest
		var response cachedBlocksResponse
		var missHashes []*common.Hash
		lack := false
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		log.Info("receive cached blocks from remote request", "requestId", request.RequestId)
		hashes := request.Hashes
		for _, hash := range hashes {
			block := pm.blockchain.GetCacheBlock(*hash)
			if block != nil {
				log.Info("找到的block进行回传", "block", block.Hash().Hex(), "高度", block.NumberU64())

				response.Blocks = append(response.Blocks, block)
			} else {
				log.Info("没找到的blockHash", "hash", hash)
				//避免循环找块
				for _, missHash := range pm.missHash {
					if *missHash == *hash {
						log.Info("本地没有这个hash", "hash", hash)
						return nil
					}
				}
				lack = true
				pm.missHash = append(pm.missHash, hash)
				missHashes = append(missHashes, hash)
			}
		}
		if lack {
			pm.infection.fetchByTransfer(&request, missHashes, p)
		} else {
			response.RequestId = request.RequestId
			err := p.SendCachedBlocksByHash(response)
			if err != nil {
				log.Error("rlp err", "err", err.Error())
			}
		}
	case msg.Code == BlocksByHashMsg:
		var response cachedBlocksResponse

		if err := msg.Decode(&response); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		log.Info("receive cached blocks from remote response", "requestId", response.RequestId)
		//cache blocks
		log.Info("BlocksByHashMsg传来的normal块", "response", len(response.Blocks))
		for _, block := range response.Blocks {
			log.Info("BlocksByHashMsg传来的normal块是", "高度", block.NumberU64(), "hash", block.Hash().Hex())
		}
		pm.blockchain.CacheBlocks(response.Blocks)
		//notify infection
		pm.infection.responseCh <- response


	case msg.Code == BlockChainNodeSet:
		var node quorum.BlockchainNode
		if err := msg.Decode(&node); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		//收到了就不进行广播了
		if _,ok:=quorum.BlockChainNodeSet.Get(node.Address.String(),pm.chaindb);ok{
			return nil
		}
		log.Info("收到node信息","node",node)
		quorum.BlockChainNodeSet.Set(node.Address.String(), &quorum.BlockchainNode{ID: node.ID, IP: node.IP, TCP: node.TCP,Address:node.Address}, pm.chaindb)
		for _,peer:=range pm.peers.Peers(){
			go peer.SendBlockChainNode(node)
		}



	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

func (pm *ProtocolManager) verifyRepeatCommitBlock(block types.Block) bool {
	self := pm.blockchain.CommitChain.SearchingBlock(block.Hash(), block.NumberU64())
	if self != nil {
		return false
	}
	//去重2 查询是否已经接收过
	if !engine.CommitDeRepetition.Add(block.NumberU64(), block.Hash()) {
		return false
	}
	//去重3 查询ca
	if cacheBlock.CacheBlocks.GetBlock(block.Hash())!=nil {
		return false
	}
	return true
}

func (pm *ProtocolManager) verifyCommitBlockIsland(commitBlock types.Block, p *peer) error {
	var localIslandID string
	blockExtra,commitExtra := utopia_types.CommitExtraDecode(&commitBlock) //收到的commitExtra
	commitExtraIsland := commitExtra.Island //接收的commit是否是岛屿

	utopiaEng := pm.consensusEngine.(*engine.Utopia)
	localIslandID, localIslandState, localIslandcNum, localIslandQuorum := engine.IslandLoad(utopiaEng)

	islandsync := func() error {
		defer atomic.StoreInt32(&commitIslandSync, 0)
		//如果是自己说岛屿,和收到的commit对比委员会成员
		log.Info("分叉高度和委员会数量", "自己的高度", localIslandcNum)
		switch {
		case len(localIslandQuorum) > len(commitExtra.Quorum):
			log.Error("收到的commit块是岛屿块,委员会数量比自己少,直接丢掉", "收到的commit委员会数量", commitExtra.Quorum, "自己的委员会数量", localIslandQuorum)
			return nil
		case len(localIslandQuorum) < len(commitExtra.Quorum):
			//小于走commit同步逻辑
			log.Info("本地分叉前的委员会成员比收到的commit块少", "本地委员会数量", len(localIslandQuorum), "commit块委员会数量", len(commitExtra.Quorum))
			pm.SyncIsland(int(localIslandcNum), blockExtra, commitExtraIsland)

			go pm.synchronise(p, commitExtra.NewBlockHeight.Uint64())
			return nil

		case len(localIslandQuorum) == len(commitExtra.Quorum):
			//相等比较当前地址的大小
			localHash := crypto.Keccak256Hash([]byte(localIslandID))
			localInt := common.BytesToUint32(localHash[:])

			commitHash := crypto.Keccak256Hash([]byte(blockExtra.IsLandID))
			commitInt := common.BytesToUint32(commitHash[:])

			if localInt < commitInt {
				//小于走commit同步逻辑
				log.Info("本地岛屿ID的hash后比收到的commit块的岛屿ID小", "自己hash后的值", localInt, "收到的hash后的值", commitInt)
				pm.SyncIsland(localIslandcNum, blockExtra, commitExtraIsland)

				go pm.synchronise(p, commitExtra.NewBlockHeight.Uint64())
				return nil
			} else {
				var isMaster bool
				currentNum := pm.blockchain.CommitChain.CurrentBlock().NumberU64()
				consensusQuorum, _ := quorum.CommitHeightToConsensusQuorum.Get(currentNum, pm.chaindb)
				for _, address := range consensusQuorum.Keys() {
					if commitBlock.Coinbase().String() == address {
						isMaster = true
						break
					}
				}
				quorumLen := consensusQuorum.Len()

				if isMaster && quorumLen == 1 {
					pm.SyncIsland(localIslandcNum, blockExtra, commitExtraIsland)
					log.Info("收到的commit块的岛屿ID是唯一的", "岛屿ID", commitBlock.Coinbase().String())
					go pm.synchronise(p, commitExtra.NewBlockHeight.Uint64())
					return nil
				}

				//自己节点的hash后比收到的大,直接return
				log.Error("收到的commit块是岛屿块,自己节点的hash后比收到commitHash的大,直接丢掉", "自己hash后的值", localInt, "收到的hash后的值", commitInt, "收到的岛屿ID", blockExtra.IsLandID)
				return nil
			}

		}
		return nil
	}
	log.Info("开始验证commitBlock","高度",commitBlock.NumberU64())
	//判断本地是大陆还是岛屿
	switch localIslandState {

	case true:
		//本地岛屿 不能重复接收commit块
		if !atomic.CompareAndSwapInt32(&commitIslandSync, 0, 1) {
			log.Error("正在给其他节点同步,不能接收commit块")
			return nil
		}
		if commitExtraIsland {
			//收到的是岛屿块跟自己不是同岛进行合并
			log.Info("收到的岛屿ID", "收到岛屿ID", blockExtra.IsLandID, "本地岛屿ID", localIslandID)
			if localIslandID != blockExtra.IsLandID {
				return islandsync()
			} else {
				//如果是同一岛屿,正常往下进行
				log.Info("收到的岛屿ID与自己相同", "岛屿ID", blockExtra.IsLandID)
				atomic.StoreInt32(&commitIslandSync, 0)
			}

		} else {
			//收到的是大陆块是 合并
			log.Info("本地是岛屿,收到了大陆块,需要去同步大陆块", "收到的commit高度", commitBlock.NumberU64(), "父hash", commitBlock.ParentHash())
			prentCommitBlock := pm.blockchain.CommitChain.SearchingBlock(commitBlock.ParentHash(), commitBlock.NumberU64()-1)
			//如果找到父块,并且等于当前hash,证明没有裂脑正常往下进行
			if prentCommitBlock != nil && prentCommitBlock.Hash() == pm.blockchain.CommitChain.CurrentBlock().Hash() {
				log.Info("找到commit父块,继续进行,不回滚")
				atomic.StoreInt32(&commitIslandSync, 0)
			} else {
				//先删除原来的commit 在同步
				pm.SyncIsland(localIslandcNum, blockExtra, commitExtraIsland)
				go pm.synchronise(p, commitExtra.NewBlockHeight.Uint64())
				atomic.StoreInt32(&commitIslandSync, 0)
				return nil
			}
		}

	case false:
		//本地大陆
		if commitExtraIsland {
			var isMaster bool
			currentCommitNum := pm.blockchain.CommitChain.CurrentBlock().NumberU64()
			consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(currentCommitNum, pm.chaindb)
			if ok {
				for _, address := range consensusQuorum.Keys() {
					if commitBlock.Coinbase().String() == address {
						isMaster = true
						break
					}
				}
				quorumLen := consensusQuorum.Len()
				self := pm.blockchain.CommitChain.SearchingBlock(commitBlock.ParentHash(), commitBlock.NumberU64()-1)
				log.Info("Normal区块数据", "高度", commitBlock.NumberU64(), "是不是本地委员会成员", isMaster, "本地委员会数量", quorumLen)
				if self != nil || (isMaster && quorumLen == 1) {
					log.Info("本地大陆收到了岛屿commit块,但是父块相同或者是唯一的master可以接受", "commit高度", commitBlock.NumberU64())
				} else {
					//收到了岛屿块不要这个块
					log.Info("commit本地是大陆,收到了岛屿块,直接丢掉", "收到的commit高度", commitBlock.NumberU64())
					//结束合并
					return nil
				}
			}
		}
		//收到的是大陆块 正常保存
	}

	pm.commitFetcher.Enqueue(p.id, []*types.Block{&commitBlock})

	currentBlock := pm.blockchain.CommitChain.CurrentBlock()
	_,commitExtraDecode := utopia_types.CommitExtraDecode(currentBlock)
	log.Info("当前岛屿状态", "存储的是否是岛屿", localIslandState, "当前高度", currentBlock.NumberU64(), "当前的commit状态", commitExtraDecode.Island)
	return nil
}

// BroadcastBlock will either propagate a block to a subset of it's peers, or
// will only announce it's availability (depending what's requested).
func (pm *ProtocolManager) BroadcastBlock(block *types.Block, propagate bool) {
	hash := block.Hash()
	peers := pm.peers.PeersWithoutBlock(hash)
	log.Info("广播peer数量","数量",len(peers))
	// If propagation is requested, send to a subset of the peer
	if propagate {
		// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
		var td *big.Int
		if parent := pm.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
			if pm.blockchain.GetTd(block.ParentHash(), block.NumberU64()-1) == nil {
				return
			}
			td = new(big.Int).Add(block.Difficulty(), pm.blockchain.GetTd(block.ParentHash(), block.NumberU64()-1))
		} else {
			log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
			return
		}
		// Send the block to a subset of our peers
		var transfer []*peer
		if len(peers) >= 4 {
			transfer = peers[:int(math.Sqrt(float64(len(peers))))]
			for _, peer := range transfer {
				log.Info("发送的节点是", "节点", peer.id)
			}

		} else {
			// PDX 发送所有节点
			transfer = peers
		}

		for _, peer := range transfer {
			peer.AsyncSendNewBlock(block, td, pm.blockchain.CommitChain.CurrentBlock().NumberU64())

		}
		log.Info("Propagated block", "发给谁", transfer, "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
		return
	}
	// Otherwise if the block is indeed in out own chain, announce it
	//if pm.blockchain.HasBlock(hash, block.NumberU64()) {
		for _, peer := range peers {
			peer.AsyncSendNewBlockHash(block)
		//}
		log.Info("Announced block", "hash", hash, "recipients", len(peers), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
	}
}

// BroadcastCommit will propagate a "commit" block to all of it's peers
func (pm *ProtocolManager) BroadcastCommit(commitBlock *types.Block) {

	peers := pm.peers.Peers()
	//log.Info("peers列表Commit", "peers长度", len(peers), "peers列表", peers)
	//for _, peer := range peers {
	//	go p2p.Send(peer.rw, BlockCommitMsg, commitBlock)
	//}
	//发送commit块
	var transfer []*peer
	if len(peers) >= 4 {
		transfer = peers[:int(math.Sqrt(float64(len(peers))))]
		for _, peer := range transfer {
			log.Info("发送的commit节点是", "节点", peer.id)
		}
	} else {
		// PDX 发送所有节点
		transfer = peers
	}

	for _, peer := range transfer {
		go p2p.Send(peer.rw, BlockCommitMsg, commitBlock)
	}
	//发送commitHash
	for _, peer := range peers {
		go peer.SendNewCommitBlockHashes([]common.Hash{commitBlock.Hash()}, []uint64{commitBlock.NumberU64()})
	}

	return
}

// MulticastAssert will multicast a block assertion to a list of peers
func (pm *ProtocolManager) MulticastAssert(assertBlock *types.Block, nodes []*discover.Node) error {
	log.Info("multicast assert start","node长度",len(nodes))
	defer log.Info("multicast assert end")
	// for each node, get its Peer object; if available send the block assert nonblock
	// otherwise connect,setup, add it to the p2p peerlist, then send it nonblock
	total:=len(nodes)
	peers := pm.peers.Peers()
	over:
	for{
	run:
		for i, node := range nodes {
			for _, peer := range peers {
				if node.ID == peer.Peer.ID() {
					total--
					log.Warn("要发送的assertion","peer",peer.Peer.ID())
					go p2p.Send(peer.rw, BlockAssertMsg, assertBlock)
					nodes=append(nodes[:i],nodes[i+1:]...)
					break run
				}
			}
			if i+1==len(nodes){
				break over
			}
		}
		if total<=0{
			return nil
		}
	}


	timer:=time.NewTimer(time.Duration(engine.BlockDelay)*time.Second)
	for {
		select {

		case <-pm.downloader.AssertionNewPeer:
			peers := pm.peers.Peers()
			loop:
			for i, node := range nodes {
				for _, peer := range peers {
					if node.ID == peer.Peer.ID() {
						total--
						log.Warn("要发送的assertion", "peer", peer.Peer.ID())
						go p2p.Send(peer.rw, BlockAssertMsg, assertBlock)
						nodes=append(nodes[:i],nodes[i+1:]...)
						break loop
					}
				}
			}
			if total<=0{
				return nil
			}
		case <-timer.C:
			 log.Error("assertion发送超时")
			return nil
		}


	}



	return nil
}

// BroadcastTxs will propagate a batch of transactions to all peers which are not known to
// already have the given transaction.
func (pm *ProtocolManager) BroadcastTxs(txs types.Transactions) {
	var txset = make(map[*peer]types.Transactions)

	// Broadcast transactions to a batch of peers not knowing about it
	for _, tx := range txs {
		peers := pm.peers.PeersWithoutTx(tx.Hash())
		peers = peers[:int(math.Sqrt(float64(len(peers))))]
		for _, peer := range peers {
			txset[peer] = append(txset[peer], tx)
		}
		//log.Info("Broadcast transaction", "hash", tx.Hash(), "recipients", len(peers))
	}
	// FIXME include this again: peers = peers[:int(math.Sqrt(float64(len(peers))))]
	for peer, txs := range txset {
		peer.AsyncSendTransactions(txs)
	}
}

// Mined broadcast loop
func (pm *ProtocolManager) minedBroadcastLoop() {
	// automatically stops if unsubscribe
	for obj := range pm.minedBlockSub.Chan() {
		if ev, ok := obj.Data.(core.NewMinedBlockEvent); ok {
			pm.BroadcastBlock(ev.Block, true)  // First propagate block to peers
			pm.BroadcastBlock(ev.Block, false) // Only then announce to the rest
		}
	}
}

// Commit broadcast loop
func (pm *ProtocolManager) commitBroadcastLoop() {
	// automatically stops if unsubscribe
	for obj := range pm.commitBlockSub.Chan() {
		if ev, ok := obj.Data.(utopia_types.NewCommitBlockEvent); ok {
			pm.BroadcastCommit(ev.Block)
		}
	}
}

// Assert multicast loop
func (pm *ProtocolManager) assertMulticastLoop() {
	// automatically stops if unsubscribe
	for obj := range pm.assertBlockSub.Chan() {
		if ev, ok := obj.Data.(utopia_types.NewAssertBlockEvent); ok {
			go pm.MulticastAssert(ev.Block, ev.Nodes)
		}
	}
}

func (pm *ProtocolManager) txBroadcastLoop() {
	for {
		select {
		case event := <-pm.txsCh:
			//log.Debug("broadcast txs", "len of ch", len(pm.txsCh))
			pm.BroadcastTxs(event.Txs)

			// Err() channel will be closed when unsubscribing.
		case <-pm.txsSub.Err():
			return
		}
	}
}
func (pm *ProtocolManager) blockChainNodeLoop() {
	for{
		select {
		case node:=<-pm.BlockChainNodeCh:
			log.Info("开始广播BlockChainNodeCh")
			for _,peer:=range pm.peers.Peers(){
				go peer.SendBlockChainNode(*node)
			}
		}
	}
}




// NodeInfo represents a short summary of the Ethereum sub-protocol metadata
// known about the host peer.
type NodeInfo struct {
	Network    uint64              `json:"network"`    // Ethereum network ID (1=Frontier, 2=Morden, Ropsten=3, Rinkeby=4)
	Difficulty *big.Int            `json:"difficulty"` // Total difficulty of the host's blockchain
	Genesis    common.Hash         `json:"genesis"`    // SHA3 hash of the host's genesis block
	Config     *params.ChainConfig `json:"config"`     // Chain configuration for the fork rules
	Head       common.Hash         `json:"head"`       // SHA3 hash of the host's best owned block
}

// NodeInfo retrieves some protocol metadata about the running host node.
func (pm *ProtocolManager) NodeInfo() *NodeInfo {
	currentBlock := pm.blockchain.CurrentBlock()
	return &NodeInfo{
		Network:    pm.networkID,
		Difficulty: pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64()),
		Genesis:    pm.blockchain.Genesis().Hash(),
		Config:     pm.blockchain.Config(),
		Head:       currentBlock.Hash(),
	}
}

//func (pm *ProtocolManager)verifyQualification(block *types.Block)bool  {
//	//先判断收到块的作者在不在委员会中
//	utopiaEngine:=pm.consensusEngine.(*engine.Utopia)
//	commitHeight:=pm.blockchain.CommitChain.CurrentBlock().NumberU64()
//	consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(commitHeight, utopiaEngine.DB())
//	if !ok{
//		log.Warn("验证区块,获取委员会失败")
//		return true
//	}
//	if _,ok:=consensusQuorum.Hmap[block.Coinbase().String()];ok{
//		return true
//	}
//	if _, state, _, quora := engine.IslandLoad(utopiaEngine);state{
//		for _,add:=range quora{
//			if add==block.Coinbase().String(){
//				return true
//			}
//
//		}
//	}
//	log.Warn("区块验证失败,收到的区块无效")
//	return false
//
//}