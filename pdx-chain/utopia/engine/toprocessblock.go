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
	"encoding/json"
	"errors"
	"math"
	"pdx-chain/core/types"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"pdx-chain/quorum"
	utopia_types "pdx-chain/utopia/types"
	"strconv"
	"sync"
	"time"
)

var CACHESIZE uint64 = 1

type normalBlockQueue struct {
	NormalBlockQueueMap     map[uint64]*ToProcessBlock
	normalBlockQueueMapLock sync.RWMutex
}

func newNormalBlockQueue() *normalBlockQueue {
	return &normalBlockQueue{NormalBlockQueueMap: make(map[uint64]*ToProcessBlock)}
}

func (n *normalBlockQueue) read(block *types.Block, processFunction func(block *types.Block, sync bool) error, broadcastFunction func(block *types.Block)) *ToProcessBlock {
	n.normalBlockQueueMapLock.Lock()
	defer n.normalBlockQueueMapLock.Unlock()
	toProcessBlocks, ok := n.NormalBlockQueueMap[block.NumberU64()]
	if !ok {
		blockExtra := utopia_types.BlockExtraDecode(block)
		toProcessBlocks = newToProcessBlock(processFunction, broadcastFunction, blockExtra.Rank)
		toProcessBlocks.num = block.NumberU64()
		n.NormalBlockQueueMap[block.NumberU64()] = toProcessBlocks
	}
	return toProcessBlocks
}

func (n *normalBlockQueue) DelToProcessNormalBlockQueueMap(Height uint64) {
	n.normalBlockQueueMapLock.Lock()
	defer n.normalBlockQueueMapLock.Unlock()
	delete(n.NormalBlockQueueMap, Height)
}

func (n *normalBlockQueue) write(block *types.Block, toProcessBlocks *ToProcessBlock) {
	n.normalBlockQueueMapLock.Lock()
	defer n.normalBlockQueueMapLock.Unlock()
	n.NormalBlockQueueMap[block.NumberU64()] = toProcessBlocks
}

func (n *normalBlockQueue) del(height uint64) {
	n.normalBlockQueueMapLock.Lock()
	defer n.normalBlockQueueMapLock.Unlock()
	delete(n.NormalBlockQueueMap, height)
}

var NormalBlockQueued *normalBlockQueue

type CommitBlockQueue struct {
	CommitBlockQueueMap map[uint64]*ToProcessBlock

	commitBlockQueueMapLock sync.RWMutex
}

var CommitBlockQueued *CommitBlockQueue

func (c *CommitBlockQueue) DelToProcessCommitBlockQueueMap(Height uint64) {
	c.commitBlockQueueMapLock.Lock()
	defer c.commitBlockQueueMapLock.Unlock()
	delete(c.CommitBlockQueueMap, Height)
}

func newCommitBlockQueueMap() *CommitBlockQueue {
	return &CommitBlockQueue{CommitBlockQueueMap: make(map[uint64]*ToProcessBlock)}
}

func (c *CommitBlockQueue) read(block *types.Block, processFunction func(block *types.Block, sync bool) error, broadcastFunction func(block *types.Block)) *ToProcessBlock {
	c.commitBlockQueueMapLock.Lock()
	defer c.commitBlockQueueMapLock.Unlock()
	toProcessBlocks, ok := c.CommitBlockQueueMap[block.NumberU64()]
	if !ok {
		blockExtra := utopia_types.BlockExtraDecode(block)
		toProcessBlocks = newToProcessBlock(processFunction, broadcastFunction, blockExtra.Rank)
		toProcessBlocks.num = block.NumberU64()
		c.CommitBlockQueueMap[block.NumberU64()] = toProcessBlocks
	}

	return toProcessBlocks
}

func (c *CommitBlockQueue) del(height uint64) {
	c.commitBlockQueueMapLock.Lock()
	defer c.commitBlockQueueMapLock.Unlock()
	delete(c.CommitBlockQueueMap, height)
}

func init() {
	CommitBlockQueued = newCommitBlockQueueMap()
	NormalBlockQueued = newNormalBlockQueue()
}

func newToProcessBlock(processFunction func(block *types.Block, sync bool) error, broadcastFunction func(block *types.Block), blockRank uint32) *ToProcessBlock {
	return &ToProcessBlock{
		blockQueue:        make([]*types.Block, numMasters+int32(blockRank)), //先收到的可能是r=0 在收自己无效的是4(numMasters)+1(委员会)
		processFunction:   processFunction,
		BroadcastFunction: broadcastFunction,
		waitCh:            make(chan *types.Block, 10),
	}
}

func ProcessCommitBlock(block *types.Block, c *Utopia, processFunction func(block *types.Block, sync bool) error, broadcastFunction func(block *types.Block), isCommit bool) {

	blockExtra, commitExtra := utopia_types.CommitExtraDecode(block)
	log.Info("开启了commitBlock处理流程", "处理的是高度为", block.NumberU64(), "rank", blockExtra.Rank)
	currentNum := c.blockchain.CommitChain.CurrentBlock().NumberU64()
	if block.NumberU64() != currentNum+1 {
		CommitDeRepetition.Del(block.NumberU64()) //commit删除缓存
		log.Debug("toProcessCommit高度无效,什么都不做", "num", block.NumberU64())
		return
	}

	toProcessBlocks := CommitBlockQueued.read(block, processFunction, broadcastFunction)
	//去重
	if len(toProcessBlocks.blockQueue) > int(blockExtra.Rank) && toProcessBlocks.blockQueue[blockExtra.Rank] != nil && block.Coinbase().String() != c.signer.String() {
		log.Debug("当前Rank的commit块已经接收过", "高度", block.NumberU64(), "rank", blockExtra.Rank)
		return
	}

	if block.NumberU64() > 1 {
		currentQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(block.NumberU64()-1, c.db)
		if !ok {
			log.Error("no quorum existed on commit block height", "height", block.NumberU64()-1)
			return
		}
		//判断rank是否有效 无效的直接跳过
		if blockExtra.Rank < uint32(int(numMasters)+currentQuorum.Len()-1) {
			mixMaster := math.Max(float64(numMasters), float64(currentQuorum.Len()))
			_, state, _, _ := IslandLoad(c)
			neededNodes := int(math.Ceil(float64(currentQuorum.Len()) / 3 * 2))
			//验证commit中assertion的数量是否大于共识节点的多数 岛屿状态是大陆
			log.Info("commit信息", "Evidences数量", len(commitExtra.Evidences), "neededNodes数量", neededNodes, "blockExtra.IsLandID", blockExtra.IsLandID, "mixMaster", mixMaster)
			if len(commitExtra.Evidences) < neededNodes && !state && blockExtra.IsLandID == c.signer.String() {
				log.Info("分叉commit进入冬眠", "commit高度", block.NumberU64())
				broadcastFunction(block)
				if !toProcessBlocks.getExecuting() {
					go toProcessBlocks.executeBlock(c, isCommit)
					toProcessBlocks.setExecuting(true)
				}
				time.Sleep(time.Duration(BlockDelay*numMasters) * time.Millisecond) //等待所有的rank时间
				//判断是否有其他的区块存上
				if block.NumberU64() <= c.blockchain.CommitChain.CurrentBlock().NumberU64() {
					return
				}
			}
		}
	}

	if !blockExtra.Empty {
		broadcastFunction(block)
		if uint32(toProcessBlocks.index) >= blockExtra.Rank {
			toProcessBlocks.waitCh <- block
			toProcessBlocks.add(int(blockExtra.Rank), block)
		} else {
			toProcessBlocks.add(int(blockExtra.Rank), block)
		}
	}

	//是否开启了for
	toProcessBlocks.executeBlockLock.Lock()
	if !toProcessBlocks.getExecuting() {
		go toProcessBlocks.executeBlock(c, isCommit)
		toProcessBlocks.setExecuting(true)
	}
	toProcessBlocks.executeBlockLock.Unlock()

}

func ProcessNormalBlock(block *types.Block, c *Utopia, processFunction func(block *types.Block, sync bool) error, broadcastFunction func(block *types.Block), isCommit bool) {
	//normal块走这里
	toProcessBlocks := NormalBlockQueued.read(block, processFunction, broadcastFunction)

	currentNum := c.blockchain.CurrentBlock().NumberU64()
	if block.NumberU64() != currentNum+1 {
		log.Debug("this Normal height is invalid,so do nothing!", "num", block.NumberU64())
		return
	}

	blockExtra := utopia_types.BlockExtraDecode(block)

	//去重
	if len(toProcessBlocks.blockQueue) > int(blockExtra.Rank) && toProcessBlocks.blockQueue[blockExtra.Rank] != nil {
		log.Debug("当前Rank的normal块已经接收过")
		return
	}

	if !blockExtra.Empty {
		broadcastFunction(block)
		if uint32(toProcessBlocks.index) >= blockExtra.Rank {
			toProcessBlocks.waitCh <- block
			toProcessBlocks.add(int(blockExtra.Rank), block)
		} else {
			toProcessBlocks.add(int(blockExtra.Rank), block)
		}
	}

	//是否开启了for
	toProcessBlocks.executeBlockLock.Lock()
	if !toProcessBlocks.getExecuting() {
		go toProcessBlocks.executeBlock(c, isCommit)
		toProcessBlocks.setExecuting(true)
	}
	toProcessBlocks.executeBlockLock.Unlock()
}

type ToProcessBlock struct {
	blockQueue        []*types.Block //收集的block
	executing         bool           //是否开启for循环
	queueLock         sync.RWMutex
	executingLock     sync.Mutex
	executeBlockLock  sync.Mutex
	index             int //选出的循环下标
	processFunction   func(block *types.Block, sync bool) error
	BroadcastFunction func(block *types.Block)
	num               uint64
	waitCh            chan *types.Block
}

func (t *ToProcessBlock) getExecuting() bool {
	t.executingLock.Lock()
	defer t.executingLock.Unlock()
	return t.executing
}

func (t *ToProcessBlock) setExecuting(v bool) {
	t.executingLock.Lock()
	defer t.executingLock.Unlock()
	t.executing = v
}

//循环找从rank0开始找block
func (t *ToProcessBlock) executeBlock(c *Utopia, isCommit bool) {
	//是否要停止循环 外面收到了比现在rank高的区块
	queueSize := len(t.blockQueue)
	log.Info("开启循环等待Rank","高度",t.num)
	for i := 0; i < queueSize-1; i++ {
		t.index = i
		switch {
		case len(t.waitCh) > 0:
			b := <-t.waitCh
			log.Info("加塞路线")
			//t.BroadcastFunction(b)
			if err := t.processFunction(b, false); err != nil {
				t.del(i)
				log.Error("加塞路线processed block err,continue next", "err", err.Error())
			} else {
				t.overState(isCommit, b.NumberU64())
				return
			}
		case t.get(i) == nil:
			delay := time.NewTimer(time.Duration(BlockDelay) * time.Millisecond)
			start := time.Now()
			select {
			case b := <-t.waitCh:
				//t.BroadcastFunction(b)
				log.Info("加塞路线1")
				err := t.processFunction(b, false)
				if err != nil {
					log.Error("加塞路线1processed block err,continue next", "err", err.Error())
					t.del(i)
					time.Sleep(time.Duration(BlockDelay)*time.Millisecond - time.Now().Sub(start))
					continue
				}else {
					t.overState(isCommit, b.NumberU64())
					return
				}
			case <-delay.C:
				continue
			}
		case t.get(i) != nil:
			log.Info("正常路线")
			//t.BroadcastFunction(t.get(i))
			err := t.processFunction(t.get(i), false)
			if err != nil {
				log.Error("正常路线processed block err,continue next", "err", err.Error())
				t.del(i)
				time.Sleep(time.Duration(BlockDelay) * time.Millisecond)
				continue
			}else {
				t.overState(isCommit, t.get(i).NumberU64())
				return
			}

		}

		//
		//t.index = i
		//if len(t.waitCh) > 0 {
		//	b := <-t.waitCh
		//	err := t.processFunction(b, false)
		//	if err != nil {
		//		t.del(i)
		//		log.Error("加塞路线processed block err,continue next", "err", err.Error())
		//	} else {
		//		if c.blockchain.CurrentBlock().NumberU64() == b.NumberU64() || c.blockchain.CommitChain.CurrentBlock().NumberU64() == b.NumberU64() {
		//			t.BroadcastFunction(b)
		//			t.overState(isCommit, b.NumberU64())
		//			return
		//		} else {
		//			log.Debug("forked block", "num", b.NumberU64())
		//		}
		//	}
		//}
		//if t.get(i) == nil {
		//	delay := time.NewTimer(time.Duration(BlockDelay) * time.Millisecond)
		//	start := time.Now()
		//	select {
		//	case b := <-t.waitCh:
		//		log.Info("加塞路线1")
		//		err := t.processFunction(b, false)
		//		if err != nil {
		//			log.Error("加塞路线1processed block err,continue next", "err", err.Error())
		//			t.del(i)
		//			time.Sleep(time.Duration(BlockDelay)*time.Millisecond - time.Now().Sub(start))
		//			continue
		//		}
		//		if c.blockchain.CurrentBlock().NumberU64() == b.NumberU64() || c.blockchain.CommitChain.CurrentBlock().NumberU64() == b.NumberU64() {
		//			t.BroadcastFunction(b)
		//			t.overState(isCommit, b.NumberU64())
		//			return
		//		}
		//	case <-delay.C:
		//		continue
		//	}
		//}
		//log.Info("正常路线")
		//err := t.processFunction(t.get(i), false)
		//if err != nil {
		//	log.Error("正常路线processed block err,continue next", "err", err.Error())
		//	t.del(i)
		//	time.Sleep(time.Duration(BlockDelay) * time.Millisecond)
		//	continue
		//}
		////判断是不是自己打的块
		//if c.blockchain.CurrentBlock().NumberU64() == t.get(i).NumberU64() || c.blockchain.CommitChain.CurrentBlock().NumberU64() == t.get(i).NumberU64() {
		//	t.BroadcastFunction(t.get(i))
		//	t.overState(isCommit, t.get(i).NumberU64())
		//	return
		//}

	}
	log.Warn("没有发现有效的区块!", "高度", t.get(queueSize-1))

	t.overState(isCommit, 0)
}

func (t *ToProcessBlock) overState(isCommit bool, height uint64) {
	t.setExecuting(false)
	if isCommit {
		CommitBlockQueued.del(height)
	} else {
		NormalBlockQueued.del(height)
	}
}

func (t *ToProcessBlock) get(index int) *types.Block {
	t.queueLock.RLock()
	t.queueLock.RUnlock()
	return t.blockQueue[index]
}
func (t *ToProcessBlock) del(index int) {
	t.queueLock.RLock()
	t.queueLock.RUnlock()
	t.blockQueue[index] = nil
}

func (t *ToProcessBlock) add(index int, block *types.Block) {
	t.queueLock.Lock()
	defer t.queueLock.Unlock()
	if index >= len(t.blockQueue) {
		for i := len(t.blockQueue); i <= index; i++ {
			t.blockQueue = append(t.blockQueue, nil)
		}
	}
	t.blockQueue[index] = block
}

var FinishToProcessNextMap = NewFinishToProcessNext()

// 保存处理完的结果集
type finishToProcessNext struct {
	finishedProcessMap map[uint64]bool
	lock               sync.RWMutex
}

func NewFinishToProcessNext() *finishToProcessNext {
	return &finishToProcessNext{
		finishedProcessMap: make(map[uint64]bool),
	}
}

func (f *finishToProcessNext) Encode() ([]byte, error) {
	return json.Marshal(f.finishedProcessMap)
}

func (f *finishToProcessNext) Decode(data []byte) error {
	return json.Unmarshal(data, &f.finishedProcessMap)
}

func (f *finishToProcessNext) Set(height uint64, isFinished bool, db ethdb.Database) error {

	f.lock.Lock()
	defer f.lock.Unlock()

	_, ok := f.finishedProcessMap[height]
	if ok {
		return errors.New("this height already seted")
	}

	f.finishedProcessMap[height] = isFinished

	if db == nil {
		return nil
	}

	data, err := f.Encode()
	if err == nil {
		db.Put([]byte("commit-finished:"+strconv.Itoa(int(height))), data)
	}
	return err
}

func (f *finishToProcessNext) Get(height uint64, db ethdb.Database) bool {
	f.lock.Lock()
	defer f.lock.Unlock()
	set, ok := f.finishedProcessMap[height]
	if ok {
		return set
	}

	if db == nil {
		return false
	}

	data, err := db.Get([]byte("commit-finished:" + strconv.Itoa(int(height))))
	if err != nil {
		//log.Error("NOT found commit-finished date")
		return false
	}

	set2 := NewFinishToProcessNext()

	err = set2.Decode(data)
	if err != nil {
		log.Error("decode error")
		return false
	}

	// update cache
	f.finishedProcessMap[height] = set2.finishedProcessMap[height]

	return set2.finishedProcessMap[height]
}
