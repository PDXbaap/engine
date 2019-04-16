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
package engine

import (
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"pdx-chain/ethdb"
	"pdx-chain/examineSync"
	"pdx-chain/pdxcc/conf"
	"pdx-chain/quorum"
	"pdx-chain/utopia/iaasconn"
	"pdx-chain/utopia/utils/blacklist"
	"pdx-chain/utopia/utils/tcUpdate"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"net"

	"math"
	"pdx-chain/accounts"
	"pdx-chain/common"
	"pdx-chain/common/hexutil"
	"pdx-chain/core"
	core_types "pdx-chain/core/types"
	"pdx-chain/crypto"
	"pdx-chain/log"
	"pdx-chain/p2p/discover"
	"pdx-chain/p2p/simulations/adapters"
	"pdx-chain/rlp"
	"pdx-chain/utopia"
	"pdx-chain/utopia/types"
	"pdx-chain/utopia/utils"
)

const (
	observer_only = ^uint32(0)
)
const (
	Simple=0    //简单共识
	Twothirds=1 //三分之二共识
)


// number of masters for each block
var numMasters int32

var BlockDelay int32 // in millisecond

//block confirmation window
var Cnfw *big.Int = new(big.Int)

var BecameQuorumLimt int32

var ConsensusQuorumLimt int32

var Majority int

// Record for an unconfirmed block

type blockTask struct {
	Number *big.Int
	masters []common.Address
	// miner and its rank for this block
	miner common.Address
	rank  uint32
	// block itself
	block *core_types.Block
	empty bool
}

type examineBlock struct {
	block *core_types.Block
	masterBatch map[common.Hash]int //[当前Normal高度]第几批master
}

var ExamineBlock *examineBlock
var IslandIDState atomic.Value //岛屿Id  空是大陆
var IslandState atomic.Value   //岛屿状态
var IslandCNum atomic.Value          //分叉前CNum
var IslandQuorum atomic.Value

//atomic.Value  //分叉前委员会数量

func IslandStore(eng *Utopia, id string, state bool, cNum int, quorum []string, db ethdb.Database) {
	eng.SetIslandState(state, db)
	eng.SetIslandIDState(id, db)
	eng.SetIslandCNum(cNum, db)
	eng.SetIslandQuorum(quorum, db)

}

func IslandLoad(eng *Utopia) (id string, state bool, cNum int, quorum []string) {
	return eng.GetIslandIDState(), eng.GetIslandState(), eng.GetIslandCNum(), eng.GetIslandQuorum()
}

func init() {
	IslandState.Store(false)                         //默认大陆
	IslandIDState.Store("")                          //默认岛屿ID是空
	IslandCNum.Store(0)                                   //默认CNum
	IslandQuorum.Store([]string{})
	NormalDeRepetition = NewDeRepetition()           //normalBlock的去重
	CommitDeRepetition = NewDeRepetition()           //commitBlock的去重
	AssociatedCommitDeRepetition = NewDeRepetition() //AssociatedCommit的去重
	ExamineBlock = &examineBlock{masterBatch: make(map[common.Hash]int)} //第几批master
	examineSync.PeerExamineSync= examineSync.NewExamineSync() //peer删除的时候别删同步的
	blacklist.BlackListTxTask = blacklist.NewBlackListTx()  //黑名单
	examineSync.IDAndBlockAddress=examineSync.NewIDAndAddress() //储存ID对应打块地址
}

type utopiaWorker struct {
	commitCh      chan *core_types.Block //act on successfully saved a normal block
	syncJoinCH    chan core_types.Block
	syncCh        chan struct{}
	isWaitSync    bool
	utopia     *Utopia
	blockchain *core.BlockChain
	timer *time.Timer
	rw    sync.RWMutex
	trustNodeList []*discover.Node
	examineBlockCh chan *examineBlock
}

func newWorker(utopia *Utopia) *utopiaWorker {
	w := &utopiaWorker{
		commitCh:       utopia.blockchain.CommitCh,
		syncJoinCH:     make(chan core_types.Block, 1),
		utopia:         utopia,
		timer:          &time.Timer{},
		blockchain:     utopia.blockchain,
		syncCh:         make(chan struct{}, 1),
		trustNodeList:  make([]*discover.Node, 0),
		examineBlockCh: make(chan *examineBlock),
	}
	return w
}

type TrustTxData struct {
	ChainID             string      `json:"chainID"` //as registered in Registry
	CommitBlockNo       uint64      `json:"commitBlockNo"`
	CommitBlockHash     common.Hash `json:"commitBlockHash"`
	PrevCommitBlockHash common.Hash `json:"prevCommitBlockHash"`
	NodeAddress         string      `json:"nodeAddress"`
}

func (w *utopiaWorker) SyncJoin(block core_types.Block) {
	w.syncJoinCH <- block
}

func (w *utopiaWorker) waitSync() {
	w.isWaitSync = true
}

func (w *utopiaWorker) StopWaitSync() {
	if w.isWaitSync {
		w.syncCh <- struct{}{}
		w.isWaitSync = false
	}
}

func (w *utopiaWorker) realIP() *net.IP {
	var realIP *net.IP

	if w.utopia.stack.Server().NAT != nil {
		if ext, err := w.utopia.stack.Server().NAT.ExternalIP(); err == nil {
			realIP = &ext
		}
	}

	if realIP == nil {
		ext := adapters.ExternalIP()
		realIP = &ext
	}

	if realIP == nil {
		ext := net.ParseIP("0.0.0.0")
		realIP = &ext
	}

	return realIP
}

func (w *utopiaWorker) Start() {

	//time.Sleep(3 * time.Second) //wait to make sure miner fully starts
	go w.commitLoop()
	go w.examineBlockLoop()

	//读取trustedNode配置文件 如果不需要trustedNode列表就将TrustedNodes改成StaticNodes
	w.startWithLocalTrustedNodeConfig()
}

func (w *utopiaWorker) startWithLocalTrustedNodeConfig()  {
	//读取trustedNode配置文件 如果不需要trustedNode列表就将TrustedNodes改成StaticNodes
	trustedNodes := w.utopia.stack.Server().TrustedNodes
	if trustedNodes == nil {
		w.trustNodeList = append(w.trustNodeList, w.utopia.stack.Server().Self())
	} else {
		for _, node := range trustedNodes {
			w.trustNodeList = append(w.trustNodeList, node)
		}
	}

	currentCommitBlockNum := w.blockchain.CommitChain.CurrentBlock().NumberU64()
	currentNormalBlockNum := w.blockchain.CurrentBlock().NumberU64()
	//判断有没有trustedNodes
	switch  {
	case trustedNodes!=nil: //后加入节点
		if currentCommitBlockNum == 0 && currentNormalBlockNum == 0 {
			var blockMiningReq= &types.BlockMiningReq{Number: 0}
			w.utopia.MinerCh <- blockMiningReq //打一个空块
		}else {
			w.continueBlock() //先同步然后打块
		}
	case trustedNodes==nil://第一个启动的节点
		if currentCommitBlockNum == 0 && currentNormalBlockNum == 0 {
			w.createNormalBlockAndCommitBlock()
		}else {
			w.continueBlock()
		}
	}
}


func (w *utopiaWorker) createNormalBlockAndCommitBlock()  {
	commitBlock, _, _ := w.creatCommitBlockWithOutTrustNode()
	w.utopia.BroadcastCommitBlock(commitBlock)

	err := w.ProcessCommitBlock(commitBlock, false)
	if err != nil {
		log.Error("1st commit block NOT saved, panic-ing now", "error", err)
		panic(err)
	}

	//打一个normal块
	w.createAndBoradcastNormalBlockWithTask(1,0,nil,false)
}

func (w *utopiaWorker) createAndBoradcastNormalBlockWithTask(number int64,rank uint32,block *core_types.Block,empty bool)  {
	task := &blockTask{Number: big.NewInt(number), rank: rank,block: block,empty: empty}
	w.createAndBroadcastNormalBlock(task)
}

func (w *utopiaWorker)examineQuorums() (flag bool){
	currentCommitBlock := w.blockchain.CommitChain.CurrentBlock()
	examineQuorums,ok := quorum.CommitHeightToConsensusQuorum.Get(currentCommitBlock.NumberU64()-1,w.utopia.db)
	if !ok{
		return flag
	}
	//验证
	for _,add:=range examineQuorums.Hmap{
		if add==w.utopia.signer{
			flag=true
			break
		}

	}
	return flag
}

func (w *utopiaWorker) examineBlockLoop() {
	var blockHash common.Hash
	for {
		timer := time.NewTimer(time.Duration((numMasters*3)*BlockDelay) * time.Millisecond)
		select {
		case <-timer.C:
			if !w.examineQuorums(){
				log.Info("还没有加入委员会,不需要进行区块验证")
				continue
			}
			currentNormalBlock := w.blockchain.CurrentBlock()
			currentCommitBlock := w.blockchain.CommitChain.CurrentBlock()
			id, state, _, _ := IslandLoad(w.utopia)

			quorums,_ := quorum.CommitHeightToConsensusQuorum.Get(currentCommitBlock.NumberU64()-1,w.utopia.db)
			//验证commit是否正常
			if examineSync.ExamineBlockWork == 0 && !w.relativeBlock(currentNormalBlock, currentCommitBlock) {
				if _, ok := ExamineBlock.masterBatch[currentCommitBlock.Hash()]; !ok {
					//取一下共识委员会，看看我在不在里面
					ExamineBlock.masterBatch[currentCommitBlock.Hash()] = 1
				} else {
					quorumLen := int32(quorums.Len())
					masterBatchNum := quorumLen / numMasters
					if int32(masterBatchNum) < masterBatchNum && masterBatchNum > 1{
						ExamineBlock.masterBatch[currentCommitBlock.Hash()]++
					}
				}
				batch := ExamineBlock.masterBatch[currentCommitBlock.Hash()]
				log.Error("Commit打块异常", "commit高度", currentCommitBlock.NumberU64(),"master批次", ExamineBlock.masterBatch[currentCommitBlock.Hash()], "commitHash", currentCommitBlock.Hash(), "batch",batch)
				//
				//删除两个commit
				for i := currentCommitBlock.NumberU64()-2; i <= currentCommitBlock.NumberU64(); i++ {
					block := w.utopia.blockchain.CommitChain.GetBlockByNum(i)
					CommitDeRepetition.Del(i)           //commit删除缓存
					AssociatedCommitDeRepetition.Del(i) //commit删除缓存
					core.DeleteCommitBlock(w.utopia.db, block.Hash(), i, *w.blockchain.CommitChain)
					CommitBlockQueued.DelToProcessCommitBlockQueueMap(i) //删除toProcessCommit缓存
					quorum.CommitHeightToConsensusQuorum.Del(i,w.utopia.db)
				}
				//删除对应的normal
				_,commitExtra := types.CommitExtraDecode(currentCommitBlock)
				normalBlockNum := commitExtra.NewBlockHeight.Uint64()-(Cnfw.Uint64()*2)
				w.blockchain.SetHead(normalBlockNum)
				log.Info("Commit打块异常,normal回滚高度", "Normal高度", w.blockchain.CurrentBlock().NumberU64())
				for i := normalBlockNum + 1; i <= currentNormalBlock.NumberU64(); i++ {
					w.blockchain.CommitChain.DeleteHeightMap(i)
					NormalBlockQueued.DelToProcessNormalBlockQueueMap(i) //删除toProcessNormal
				}
				//w.utopia.SetIslandIDState(w.utopia.signer.String(), w.utopia.DB())
				//w.utopia.SetIslandState(true, w.utopia.DB())
				normalBlockNum =w.utopia.blockchain.CurrentBlock().NumberU64()
				//从新开始打commit块
				log.Info("commit异常回滚后的noraml高度","高度",normalBlockNum)
				normalBlock := w.blockchain.GetBlockByNumber(normalBlockNum)
				w.ProcessCommitLogic(normalBlock, batch, true)
			}


			//验证normal是否正常
			if examineSync.ExamineBlockWork == 1 || blockHash != currentNormalBlock.Hash() || currentNormalBlock.NumberU64() <= 1 {
				delete(ExamineBlock.masterBatch, blockHash) //清除批次
				log.Info("Normal打快正常", "原始Hash", blockHash.String(), "最新hash", currentNormalBlock.Hash().String())
				blockHash = currentNormalBlock.Hash()
				break
			}

			ExamineBlock.block = currentNormalBlock
			if num, ok := ExamineBlock.masterBatch[currentNormalBlock.Hash()]; !ok {
			   ExamineBlock.masterBatch[currentNormalBlock.Hash()] = 1
			} else {
				quorumLen := int32(quorums.Len())
				masterBatchNum := quorumLen / numMasters
				if int32(num) < masterBatchNum && masterBatchNum > 1 {
					ExamineBlock.masterBatch[currentNormalBlock.Hash()]++
				}
			}
			log.Error("Normal打快异常", "原始Hash", blockHash.String(), "最新hash", currentNormalBlock.Hash().String(), "master批次", ExamineBlock.masterBatch[currentNormalBlock.Hash()])
			if !state {
				IslandStore(w.utopia, w.utopia.signer.String(), true, 0, nil, w.utopia.db)
			}
			if !state || id == w.utopia.signer.String() {
				w.examineBlockCh <- ExamineBlock
			}
		}
	}
}

//判断当前commit是否正常
func (w *utopiaWorker) relativeBlock(currentNormalBlock *core_types.Block, commitBlock *core_types.Block) bool {
	//先判断commit是否正常,如果正常走正常流程,不正常先去发第二批的assertion和打第二批的commit
	_,commitExtra := types.CommitExtraDecode(commitBlock)
	relativeNum := commitExtra.NewBlockHeight.Uint64() //当前commit对应的Normal
	log.Debug("验证commit", "当前commit对应的Normal高度+cfd+5", relativeNum+Cnfw.Uint64()+5, "当前的Normal高度", currentNormalBlock.NumberU64())
	if currentNormalBlock.NumberU64() > relativeNum+Cnfw.Uint64()+5 && examineSync.ExamineBlockWork==0{
		return false //commit不正常
	} else {
		return true
	}
}

func (w *utopiaWorker) continueBlock() {
	// restart as the ONLY node of the chain
	timeout := time.NewTimer(time.Duration(4*BlockDelay) * time.Millisecond) //超过时间后自己打快
	w.waitSync()
	for {
		select {
		case <-w.syncCh:
			return
		case <-timeout.C:
			w.selfWork()
		}
	}
}

func (w *utopiaWorker) selfWork() {

		currentBlock := w.blockchain.CurrentBlock()
		currentCommitBlock := w.blockchain.CommitChain.CurrentBlock()
		committedNormalBlockNum := w.blockchain.CommitChain.CurrentCommitExtra().NewBlockHeight.Uint64()
		currentCommitBlockNum := currentCommitBlock.NumberU64()
		consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(currentCommitBlockNum, w.utopia.db)
		if !ok{
			log.Error("ConsensusQuorum获取失败","commitHeight",currentCommitBlockNum)
			return
		}
		IslandStore(w.utopia, w.utopia.signer.String(), true, int(currentCommitBlockNum), consensusQuorum.Keys(), w.utopia.db) //带数据启动变岛屿状态

		//计算normalRank
		_,rank, _, err := w.masterRank(currentBlock, w.utopia.signer, -1, 0)
		if err != nil || rank == observer_only {
			log.Error("重新启动后没有打块资格,继续等待")
			return
		}

		if currentCommitBlockNum >= committedNormalBlockNum+Cnfw.Uint64() {
			//计算commit的Rank
			masters,rank, _, err := w.masterRank(currentBlock, w.utopia.signer, int64(currentCommitBlock.NumberU64()), 0)
			if err != nil {
				time.Sleep(time.Duration(10*BlockDelay) * time.Millisecond)
			}

			// task.Number is the current commit block height
			number := currentCommitBlock.Number()
			task := &blockTask{number, masters, w.utopia.signer, rank, currentBlock, false}

			w.createAndMulticastBlockAssertion(task)

			w.createAndBroadcastCommitBlock(task)
		} else {
			//打一个normal块
			w.createAndBoradcastNormalBlockWithTask(currentBlock.Number().Int64() + 1,rank,nil,false)
			return
		}

		w.createAndBoradcastNormalBlockWithTask(currentBlock.Number().Int64() + 1,rank,nil,false)
	}


func (w *utopiaWorker) creatCommitBlockWithOutTrustNode() (*core_types.Block, *types.BlockExtra, *types.CommitExtra) {
	address := w.utopia.signer
	nodeInfo := w.utopia.stack.Server().NodeInfo()
	nodeIP := w.realIP()

	//需要rank和区块高度
	commitExtra := types.CommitExtra{NewBlockHeight: big.NewInt(0), MinerAdditions: []common.Address{address}}
	commitExtraByte, err := rlp.EncodeToBytes(commitExtra)
	if err != nil {
		log.Error("err rlp encode error", "error", err)
	}

	blockExtra := &types.BlockExtra{Rank: 0, CNumber: big.NewInt(1), IP: *nodeIP, Port: uint16(nodeInfo.Ports.Listener), Extra: commitExtraByte}
	blockExtra.Signature = nil
	data, err := rlp.EncodeToBytes(blockExtra)
	if err != nil {
		return nil, nil, nil
	}

	hash := crypto.Keccak256Hash(data)
	sig, err := w.utopia.signFn(accounts.Account{Address: address}, hash.Bytes())
	blockExtra.Signature = sig
	currentBlock := w.blockchain.CommitChain.CurrentBlock()

	genesisBlockHash := currentBlock.Hash()
	commitBlock := types.NewCommitblock(blockExtra, genesisBlockHash, address)

	return commitBlock, blockExtra, &commitExtra
}

//find next block of masters
func (w *utopiaWorker) getMasterOfNextBlock(isCommit int64, block *core_types.Block, batch int) ([]common.Address, int, error) {
	var commitHeight int64
	var masters int32

	if isCommit != -1 {
		commitHeight = isCommit - 1
	} else {
		blockExtra := types.BlockExtraDecode(block)
		commitHeight = blockExtra.CNumber.Int64() - 1
	}

	if commitHeight <= 0 {
		commitHeight = 1
	}
	consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(uint64(commitHeight), w.utopia.db)
	if !ok {
		log.Error("cannot get consensus quorum for commit height:", "commitHeight", commitHeight)
		return nil, 0, errors.New("cannot get consensus quorum for commit height")
	}
	log.Info("取出委员会的高度和数量", "取委员会的高度", commitHeight, "取出委员会的数量", consensusQuorum.Len(),"batch",batch)

	consensusNodesOrdered := consensusQuorum.KeysOrdered()

	consensusNodesOrderedLen := len(consensusNodesOrdered)

	if consensusNodesOrderedLen == 1 {
		log.Info("共识委员会成员是","address",consensusNodesOrdered[0],"本机地址是",w.utopia.signer.String())
	}

	masters = int32(len(consensusNodesOrdered))
	//增加master限制
	if masters >= numMasters {
		masters = numMasters
	}
	var nextBlockMasters = make([]common.Address, int(masters)+batch*int(masters))

	log.Info("计算本次的", "要计算的Commit高度", isCommit, "Commit高度是", commitHeight)
	//获取一段距离的所有blockhash和打块地址哈希
	tempHash,err := w.get2cfdBlockHashAndMasterAddressHash(isCommit,block)
	if err != nil {
		log.Error("取哈希和master有错")
		return nil, consensusQuorum.Len(), err
	}

	var alreadyUsedNum = make([]uint32, 0)
	log.Info("计算master的终点值是多少","num",int(masters)+batch*int(masters),"batch",batch)
	for r := 0; r < int(masters)+batch*int(masters); r++ {
		if r >= consensusNodesOrderedLen { //防止越界
			break
		}
		var data []byte
		if isCommit != -1 {
			prevCommitBlock := w.blockchain.CommitChain.GetBlockByNum(uint64(commitHeight))
			if prevCommitBlock == nil {
				return nil, consensusQuorum.Len(), errors.New("prevCommitBlock is nil")
			}
			prevCommitBlockHash := prevCommitBlock.Hash()
			prevHash := prevCommitBlockHash[:]
			data = bytes.Join([][]byte{tempHash[:], common.IntToHex(int64(r)), prevHash}, []byte{})
		} else {
			data = bytes.Join([][]byte{tempHash[:], common.IntToHex(int64(r)), common.IntToHex(block.Number().Int64())}, []byte{})
		}

		tempTotalHash := crypto.Keccak256Hash(data)
		tempInt := common.BytesToUint32(tempTotalHash[:])
	cLoop:
		i := tempInt % uint32(consensusNodesOrderedLen)
		for _, value := range alreadyUsedNum {
			if i == value {
				tempInt += 1
				goto cLoop
			}
		}
		alreadyUsedNum = append(alreadyUsedNum, i)
		address := common.HexToAddress(consensusNodesOrdered[i])
		nextBlockMasters[r] = address
		log.Info("计算rank", "Rank", r, "地址", address, "委员会consensusNodesOrderedLen", consensusNodesOrderedLen)
	}
	if len(nextBlockMasters) < batch*int(masters) {
		delete(ExamineBlock.masterBatch, block.Hash())
		return nil, consensusQuorum.Len(), err
	}
	return nextBlockMasters, consensusQuorum.Len(), nil
}

func (w *utopiaWorker) get2cfdBlockHashAndMasterAddressHash(isCommit int64, block *core_types.Block) (common.Hash,error) {
	log.Info("传进来的值是","isCommit",isCommit)
	blockHash, err := w.get2cfdBlockHash(isCommit, block)
	if err != nil {
		return common.Hash{}, err
	}

	addressHash, err := w.getcfdBlockMasterAddr(isCommit, block)
	if err != nil {
		return common.Hash{}, err
	}

	totalAddressHash := getCfdBlockMasterAddressHash(addressHash)
	hash := bytes.Join([][]byte{blockHash, totalAddressHash}, []byte{})
	tempHash := crypto.Keccak256Hash(hash)

	return tempHash,nil
}

//根据masters们获得判断自己的rank
func (w *utopiaWorker) masterRank(block *core_types.Block, addr common.Address, isCommit int64, batch int) ([]common.Address,uint32, int, error) {
	log.Info("计算masterRank", "高度", block.NumberU64()+1, "isCommit", isCommit,"batch",batch)
	//获得下一个区块的masters
	masters, consensusQuorumLen, err := w.getMasterOfNextBlock(isCommit, block, batch)
	if err != nil {
		//如果 没有取到委员会,就取自己当前的委员会数量
		if consensusQuorumLen == 0 {
			currentCommitNum := w.blockchain.CommitChain.CurrentBlock().NumberU64()
			consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(uint64(currentCommitNum), w.utopia.db)
			if !ok {
				consensusQuorumLen = int(numMasters)
			} else {
				consensusQuorumLen = consensusQuorum.Len()
			}
		}
		log.Error("cant get masters", "err", err)
		return []common.Address{},observer_only, consensusQuorumLen, err
	}

	for r, v := range masters {
		if v == addr {
			bth := r / int(numMasters)
			if bth == 0 {
				num := int(numMasters)

				if int(numMasters) > len(masters){
					num = len(masters)
				}
				return masters[:num],uint32(r),consensusQuorumLen, nil
			}else {
				return masters[batch*int(numMasters):],uint32(r),consensusQuorumLen ,nil
			}
		}
	}

	return masters,observer_only, consensusQuorumLen, nil
}

//get 2*dis*cfd blockhash
func (w *utopiaWorker) get2cfdBlockHash(isCommitMaster int64, block *core_types.Block) ([]byte, error) {
	var blockHashCollect []byte //返回值
	var currentNum int64        //目前的位置
	var finishNum int64         //开始的num

	if isCommitMaster != -1 {
		currentNum = isCommitMaster
		if currentNum-Cnfw.Int64() >= 0 {
			finishNum = currentNum - Cnfw.Int64()
		} else {
			finishNum = 0
		}
	}else {
		currentNum = block.Number().Int64() - 1
		cnfNum := 2 * Cnfw.Int64()
		if currentNum - cnfNum >= 0 {
			finishNum = currentNum - cnfNum
		} else {
			finishNum = 0
		}
	}


	for ; currentNum > finishNum; currentNum-- {
		var block *core_types.Block
		if isCommitMaster != -1{
			block = w.blockchain.CommitChain.GetBlockByNum(uint64(currentNum))
		}else {
			block = w.blockchain.GetBlockByNumber(uint64(currentNum))
		}

		//block := w.blockchain.CommitChain.GetBlockByNum(uint64(currentNum))
		if block == nil {
			log.Warn("get2cfdBlockHash没取到CommitBlock,出问题了")
			return nil, errors.New("get2cfdBlockHash没取到CommitBlock,出问题了")
		}
		header := block.Header()
		if header == nil {
			log.Info("block header is empty")
			return nil,errors.New("block header is nill")
		}
		headHash := header.Hash()
		if bytes.Compare(headHash.Bytes(), []byte{0}) == 0 {
			log.Info("header hash is empty")
		}
		blockHashCollect = bytes.Join([][]byte{blockHashCollect, block.Header().Hash().Bytes()}, []byte{})
	}

	return blockHashCollect, nil
}

//get one cfd blocks of masters address
func (w *utopiaWorker) getcfdBlockMasterAddr(isCommitMaster int64, block *core_types.Block) ([][]byte, error) {

	var addresses [][]byte //返回的地址
	var currentNum int64   //当前的高度
	var finishNum int64    //开始的高度
	if isCommitMaster != -1{
		currentNum = isCommitMaster
	}else {
		currentNum = block.Number().Int64() - 1
	}

	if currentNum >= Cnfw.Int64() {
		finishNum = currentNum - Cnfw.Int64()
	} else {
		finishNum = 0
	}

	for ; currentNum > finishNum; currentNum-- {
		var block *core_types.Block
		if isCommitMaster != -1 {
			block = w.blockchain.CommitChain.GetBlockByNum(uint64(currentNum))
		}else {
			block = w.blockchain.GetBlockByNumber(uint64(currentNum))
		}
		if block == nil {
			log.Error("getcfdBlockMasterAddr取block出问题")
			return nil, errors.New("getcfdBlockMasterAddr取block出问题")
		}
		addresses = append(addresses, []byte(block.Header().Coinbase.String()))
	}
	return addresses, nil
}

//get sum of addresshash
func getCfdBlockMasterAddressHash(addrs [][]byte) []byte {
	var addrHash []byte
	for i := 0; i < len(addrs)-1; i += 2 {
		addrHash = bytes.Join([][]byte{
			addrs[i],
			addrs[i+1],
		}, []byte{})
	}
	return addrHash
}


//创建blockExtra
func (w *utopiaWorker) createBlockExtra(blockExtra types.BlockExtra,cNum int64,rank uint32,extraData []byte) (*types.BlockExtra,error) {
	blockExtra.CNumber = big.NewInt(cNum)
	blockExtra.NodeID = discover.PubkeyID(&w.utopia.stack.Server().PrivateKey.PublicKey)
	blockExtra.IP = *w.realIP()
	blockExtra.Port = uint16(w.utopia.stack.Server().NodeInfo().Ports.Listener)
	//当前节点rank
	blockExtra.Rank = rank
	blockExtra.Extra = extraData
	signFn, signer := w.utopia.signFn, w.utopia.signer
	//blockExtra进行签名
	data, err := rlp.EncodeToBytes(blockExtra)
	if err != nil {
		log.Error("blockExtra EncodeToBytes error")
		return nil,errors.New("blockExtra EncodeToBytes error")
	}

	hash := crypto.Keccak256Hash(data)
	sig, _ := signFn(accounts.Account{Address: signer}, hash.Bytes())
	blockExtra.Signature = sig

	return &blockExtra,nil
}


func (w *utopiaWorker) createAndMulticastBlockAssertion(task *blockTask) {
	// 1) Create AssertExtra
	log.Info("计算要发的assertion的地址")
	blockPath := make([]common.Hash, 0)
	assertExtra := types.AssertExtra{}
	cfdStart := w.blockchain.CommitChain.CurrentCommitExtra().NewBlockHeight.Uint64() + 1
	cfdEnd := cfdStart + Cnfw.Uint64()-1
	log.Info("创建assertion需要发送的normal区块","要打的assertion",task.Number.Uint64()+1,"从",cfdStart,"到",cfdEnd)
	//循环取出所有区间cfd个区块并且转成hash储存到blockPath
	for i := cfdStart; i <= cfdEnd; i++ {
		block := w.blockchain.GetBlockByNumber(i)
		if block == nil {
			log.Error("---------------block is nil num :", "normal高度", i)
			return
		}
		blockPath = append(blockPath, block.Hash())
	}

	assertExtra.BlockPath = blockPath
	commitBlockNumber := big.NewInt(task.Number.Int64())
	assertExtra.LatestCommitBlockNumber = commitBlockNumber

	//签名Assert 对整个 AssertExtra 进行签名
	signer, signFn := w.utopia.signer, w.utopia.signFn
	data, err := rlp.EncodeToBytes(blockPath)
	if err != nil {
		log.Info("blockPath EncodeToBytes error")
		return
	}
	hash := crypto.Keccak256Hash(data)
	sig, _ := signFn(accounts.Account{Address: signer}, hash.Bytes())
	assertExtra.Signature = sig

	// 2) Create BlockExtra
	var blockExtra types.BlockExtra
	data, _ = assertExtra.Encode()
	extra,err := w.createBlockExtra(blockExtra,w.blockchain.CommitChain.CurrentBlock().Number().Int64() + 1,0,data)
	if err != nil {
		return
	}
	data, _ = extra.Encode()

	// 3) Create block header
	head := &core_types.Header{
		Number:     new(big.Int).SetUint64(commitBlockNumber.Uint64() + 1), // next commit height
		Nonce:      core_types.EncodeNonce(rand.Uint64()),
		Time:       new(big.Int).SetUint64(uint64(time.Now().Unix())),
		ParentHash: hash,
		Extra:      data,
		GasLimit:   5000000,
		Difficulty: big.NewInt(1024),
		MixDigest:  common.BytesToHash(hexutil.MustDecode("0x0000000000000000000000000000000000000000000000000000000000000000")),
		Root:       crypto.Keccak256Hash(nil),
		Coinbase:   w.utopia.signer,
	}
	nodeId := discover.PubkeyID(&w.utopia.stack.Server().PrivateKey.PublicKey)

	// 4) Create block assert
	block := core_types.NewBlock(head, nil, nil, nil)
	//多播给masters,让下一个区块的master进行commitBlock确认
	var nodes []*discover.Node
	peers := w.utopia.stack.Server().Peers()

	for _, address := range task.masters {
		if address.Hash() == w.utopia.signer.Hash(){
			continue
		}
		flag := false
		node, ok := quorum.BlockChainNodeSet.Get(address.String(), w.utopia.db)
		if !ok {
			log.Info("没有取到node取下一个master得node")
			continue
		}
		dnode := discover.Node{ID: node.ID, IP: node.IP, TCP: node.TCP}
		//保证assertion发送给所有master,如果没有master链接,就跟master建立链接,master是自己就不用建立
		for _,peer := range peers {
			if peer.ID() == node.ID || nodeId.String() == node.ID.String() {
				flag = true
				break
			}
		}

		if !flag && len(peers) != 0 {
			log.Info("需要跟master建立链接", "masterId", node.ID.String()[:16], "eNode", nodeId.String()[:16])
			w.utopia.stack.Server().AddPeer(&dnode)
		}
		nodes = append(nodes, &dnode)
	}

	//多播asserblock给commitblock区块的master
	w.utopia.MulticastAssertBlock(block, nodes) //进行master多播

	w.processBlockAssert(block)
}

func (w *utopiaWorker) createAndBroadcastCommitBlock(task *blockTask) {

	timeout := time.NewTimer(time.Millisecond * time.Duration(BlockDelay))

	//外面从新存的commitNumber
	nextCommitHeight := task.Number.Uint64() + 1
	log.Info("获取assertion的commit高度","高度",nextCommitHeight)
	var allNewAssertions *utils.SafeSet

	var ok bool
waiting:
	for {
		select {

		case <-timeout.C:
			allNewAssertions, ok = AssertCacheObject.Get(nextCommitHeight, w.utopia.db)
			if !ok {
				log.Error("no block assertions received, ignore creat and broadcast commit block")
				return
			}
			break waiting
		}
	}

	log.Info("done waiting for assertion collection")

	//孤儿
	if allNewAssertions.Len() <= 0 {
		log.Error("received no assertions before timeout")
		return
	}

	////取上一个commit高度获取共识节点
	consensusQuorum,err := w.getQuorumForHeight()
	if err != nil {
		return
	}

	//create blockExtra & commitExtra
	blockExtra,commitExtra,err := w.createCommitExtra(consensusQuorum,allNewAssertions,task,nextCommitHeight)
	if err != nil {
		return
	}

	commit, _ := commitExtra.Encode()
	//set commitExtra to blockExtra
	extra,err := w.createBlockExtra(blockExtra,task.Number.Int64() + 1,task.rank,commit)
	if err != nil{
		return
	}

	currentBlockHash := w.blockchain.CommitChain.CurrentBlock().Hash()
	//创建commitBlcok
	commitBlock := types.NewCommitblock(extra, currentBlockHash, w.utopia.signer)

	log.Info("commit打包完成", "commit信息 number", commitBlock.NumberU64(), "hash", commitBlock.Hash().String(), "rank", extra.Rank)

	blockextra,_:= types.CommitExtraDecode(commitBlock)
	log.Info("rank","rank",blockextra.Rank)

	w.utopia.CommitFetcher.Enqueue(fmt.Sprintf("%x", blockExtra.NodeID.Bytes()[:8]), []*core_types.Block{commitBlock})

	return
}

func (w *utopiaWorker) createCommitExtra(consensusQuorum *quorum.NodeAddress,allNewAssertions *utils.SafeSet,task *blockTask,nextCommitHeight uint64) (types.BlockExtra,types.CommitExtra,error) {
	//如果是utopia共识就用委员会总数,否则就是收到assertion的数量
	var total int
	switch Majority {
	case Twothirds:
		log.Info("Twothirds共识")
		total=consensusQuorum.Len()
	case Simple:
		log.Info("Simple共识")
		total=allNewAssertions.Len()
	default:
		log.Info("Simple共识")
		total=allNewAssertions.Len()
	}

	//获取所有共识节点的2/3节点的数量
	needNode := int(math.Ceil(float64(total) / 3 * 2))
	log.Debug("开始needNode", "needNode", needNode)

	currentCommitBlock := w.blockchain.CommitChain.CurrentBlock()
	//取上一个commitExtra
	_,lastCommitExtra := types.CommitExtraDecode(currentCommitBlock)
	log.Debug("lastCommitExtra", "commit高度", task.Number, "长度", lastCommitExtra.Quorum)

	if task.Number.Uint64()+1 != currentCommitBlock.Number().Uint64()+1 {
		return types.BlockExtra{},types.CommitExtra{},errors.New("区块高度有误")
	}

	if needNode == 0 {
		needNode = 1
	}
	log.Debug("最终needNode", "needNode", needNode)

	validNewAssertions := allNewAssertions.Copy()

	//用收到的assertion计算共识BlocksPath
	commitExtra,commitBlocksPath := w.setUpCommitBlockPath(validNewAssertions,needNode)

	//判断岛屿并设置岛屿标识
	blockExtra,commitExtra := w.setUpIslandInfo(needNode,validNewAssertions,task,commitBlocksPath,consensusQuorum,commitExtra,lastCommitExtra)

	//setUp evidence
	commitExtra = w.setUpEvidence(allNewAssertions,commitBlocksPath,commitExtra)

	log.Info("save condensedEvidence")
	//根据commitBlocksPath的最后一区块hash获取最新区块高度

	var height *big.Int
	if len(commitBlocksPath) > 0 {
		height = big.NewInt(int64(len(commitBlocksPath)) + w.blockchain.CommitChain.CurrentCommitExtra().NewBlockHeight.Int64())
	} else {
		//path是0 取上一个commitBlock存的高度
		height = w.blockchain.CommitChain.CurrentCommitExtra().NewBlockHeight
	}
	log.Info("commit的newHeight", "commit高度", task.Number.Uint64()+1, "newHeight", height.Uint64())
	commitExtra.NewBlockHeight = height

	//更新活跃的
	w.updateStaticsAndQualification(nextCommitHeight, &commitExtra, w.utopia.signer)

	//计算节点增减
	commitExtra.MinerAdditions, commitExtra.MinerDeletions,
		commitExtra.NodeAdditions, commitExtra.NodeDeletions = w.calculateMembershipUpdates(nextCommitHeight)

	log.Info("委员会状态", "新增委员会成员", len(commitExtra.MinerAdditions), "收到的assertion数量", allNewAssertions.Len())
	//自己会给自己发送一条assertion
	if commitExtra.Island == true && allNewAssertions.Len() == 1 {
		//如果分叉 删除所有节点
		commitExtra = w.removeOtherNode(consensusQuorum,commitExtra,nextCommitHeight)
	}



	return blockExtra,commitExtra,nil
}

func (w *utopiaWorker) removeOtherNode(consensusQuorum *quorum.NodeAddress,commitExtra types.CommitExtra,nextCommitHeight uint64) types.CommitExtra {
	log.Info("删除所有节点")
	commitExtra.NodeAdditions = []common.Address{}
	commitExtra.NodeDeletions = []common.Address{}
	commitExtra.MinerDeletions = []common.Address{}
	commitExtra.MinerAdditions = []common.Address{}
	for _, address := range consensusQuorum.Hmap {
		if address != w.utopia.signer {
			log.Info("清除活跃的数据","需要清除的地址是",address.String(),"开始清除的高度",nextCommitHeight)
			commitHeight2NodeDetailSetCache.DelData(nextCommitHeight-1,address,w.utopia.db)
			commitExtra.MinerDeletions = append(commitExtra.MinerDeletions, address)
		}
	}
	commitExtra.MinerAdditions = append(commitExtra.MinerAdditions, w.utopia.signer)
	return commitExtra
}

//set up blockpath
func (w *utopiaWorker) setUpCommitBlockPath(validNewAssertions *utils.SafeSet,needNode int) (types.CommitExtra,[]common.Hash) {
	var commitExtra types.CommitExtra
	commitBlocksPath := blockPath(validNewAssertions, needNode)
	log.Info("commitBlocksPath信息", "commitBlocksPath长度", len(commitBlocksPath), "validNewAssertions长度", validNewAssertions.Len(), "收到的Assertions信息", validNewAssertions.Keys())
	if len(commitBlocksPath) == 0 {
		//准备回滚
		commitExtra.Reset = true
		commitExtra.AcceptedBlocks = []common.Hash{}
	} else {
		commitExtra.Reset = false
		commitExtra.AcceptedBlocks = commitBlocksPath //标准区块
	}
	//lastQuorum := w.utopia.GetIslandQuorumMap()
	//需要把非原来共识委员会的assertions剔除
	//if lastQuorum != nil && w.utopia.GetIslandState(){
	//	for _ ,address := range validNewAssertions.Keys() {
	//		_,ok := lastQuorum[address]
	//		if ok {
	//			continue
	//		}else {
	//			validNewAssertions.Del(address)
	//		}
	//	}
	//	log.Error("进来了","validNewAssertions num",len(validNewAssertions.Keys()))
	//}
	return commitExtra,commitBlocksPath
}

//set up island info
func (w *utopiaWorker) setUpIslandInfo(needNode int,validNewAssertions *utils.SafeSet,task *blockTask,commitBlocksPath []common.Hash,consensusQuorum *quorum.NodeAddress,commitExtra types.CommitExtra,lastCommitExtra types.CommitExtra) (types.BlockExtra,types.CommitExtra) {
	var blockExtra types.BlockExtra
	unbifQuorum := w.utopia.GetIslandQuorum()
	changeToLandNum := int(math.Ceil(float64(len(unbifQuorum)) / 3 * 2))
	log.Info("打commit的一些信息","高度",task.Number.Uint64()+1,"未分叉共识委员会的数量",len(unbifQuorum),"三分之二数",changeToLandNum,"共识委员会数量",int(consensusQuorum.Len()))
	//1.收到的assertion < needNode  2.分叉前委员会 >= 委员会  3.path==0
	if validNewAssertions.Len() < needNode || changeToLandNum > int(consensusQuorum.Len()) || len(commitBlocksPath) == 0 {
		log.Info("分叉了", "高度", task.Number.Uint64()+1, "needNode", needNode, "当前Quorum", consensusQuorum.Len())
		_, _, cNum, _ := IslandLoad(w.utopia)
		if (!lastCommitExtra.Island && !IslandState.Load().(bool)) || (IslandState.Load().(bool) && cNum == 0) {
			//第一次
			log.Debug("打commit第一次分叉", "cnum", task.Number.Uint64()+1, "querum", consensusQuorum.Keys())
			commitExtra.Island = true
			blockExtra.IsLandID = w.utopia.signer.String()
			commitExtra.CNum = task.Number.Uint64() + 1
			commitExtra.Quorum = consensusQuorum.Keys()
		} else {
			//以后
			cnum:=0
			blockExtra.IsLandID, commitExtra.Island, cnum, commitExtra.Quorum = IslandLoad(w.utopia)
			commitExtra.CNum=uint64(cnum)
			log.Debug("打commit以后", "岛屿id", blockExtra.IsLandID, "岛屿状态", commitExtra.Island,
				"cNum", commitExtra.CNum)
		}
		//如果分叉 就用自己的path
		commitBlocksPath := w.selfPath(validNewAssertions)
		commitExtra.AcceptedBlocks = commitBlocksPath
		commitExtra.Reset = false
	}
	return blockExtra,commitExtra
}


//set up evidence
func (w *utopiaWorker) setUpEvidence(allNewAssertions *utils.SafeSet,commitBlocksPath []common.Hash,commitExtra types.CommitExtra) types.CommitExtra {
	//保存condensedEvidence信息
	for _, key := range allNewAssertions.Keys() {
		val := allNewAssertions.Get(key)
		assertInfo := val.(*AssertInfo)
		condensedEvidence := types.CondensedEvidence{}
		condensedEvidence.Address = assertInfo.Address
		condensedEvidence.Signature = assertInfo.AssertExtra.Signature
		condensedEvidence.IP = assertInfo.BlockExtra.IP
		condensedEvidence.TCP = assertInfo.BlockExtra.Port

		//var flag bool
		//判断长度
		if len(assertInfo.AssertExtra.BlockPath) >= len(commitBlocksPath) {
			condensedEvidence.ExtraKind = types.EVIDENCE_ADD_EXTRA
			for i, blockHash := range assertInfo.AssertExtra.BlockPath {
				//sBlockHahs 是选出来3/2标准的
				if i < len(commitBlocksPath)  {
					sBlockHash := commitBlocksPath[i]
					if sBlockHash != blockHash {
						//得到跟标准blockPath不同的区块hash下标,并且把从这个区块开始后面所有的区块都保存起来
						//把每个节点BlockPath比共识的BlockPath多余的部分保存在condensedEvidence.ExtraBlocks中
						copy(condensedEvidence.ExtraBlocks, assertInfo.AssertExtra.BlockPath[i:])
						break
					}
				}else {
					copy(condensedEvidence.ExtraBlocks, assertInfo.AssertExtra.BlockPath[i:])
					break
				}
			}
			//condensedEvidence保存在commitExtra.Evidences中
			commitExtra.Evidences = append(commitExtra.Evidences, condensedEvidence)
		} else {
			//比path短的路线
			condensedEvidence.ExtraKind = types.EVIDENCE_DEL_EXTRA
			for i, sBlockHash := range commitBlocksPath  {
				if i > len(assertInfo.AssertExtra.BlockPath) {
					blockHash := assertInfo.AssertExtra.BlockPath[i]
					if blockHash != sBlockHash {
						copy(condensedEvidence.ExtraBlocks, assertInfo.AssertExtra.BlockPath[i:])
						break
					}
				}else {
					copy(condensedEvidence.ExtraBlocks, assertInfo.AssertExtra.BlockPath[i:])
					break
				}
			}
		}
	}
	return commitExtra
}

func (w *utopiaWorker) getQuorumForHeight() (*quorum.NodeAddress,error) {
	//取上一个commit高度
	commitBlockHeight := w.blockchain.CommitChain.CurrentBlock().NumberU64() - 1
	if commitBlockHeight == 0 {
		commitBlockHeight = 1
	}
	//获取共识节点
	consensusQuorum, _ := quorum.CommitHeightToConsensusQuorum.Get(commitBlockHeight, w.utopia.db)
	if consensusQuorum == nil {
		log.Error("no consensus quorum, failed creating commit block")
		return nil,errors.New("no consensus quorum, failed creating commit block")
	}
	return consensusQuorum,nil
}


func (w *utopiaWorker) selfPath(allNewAssertions *utils.SafeSet) []common.Hash {
	for _, add := range allNewAssertions.KeysOrdered() {
		if add == w.utopia.signer.String() {
			value := allNewAssertions.Get(add)
			assertInfo := value.(*AssertInfo)
			return assertInfo.AssertExtra.BlockPath
		}
	}
	return nil
}

func blockPath(assertions *utils.SafeSet, nodesNeeded int) []common.Hash {
	matrix := list.New()
	for _, key := range assertions.KeysOrdered() {
		value := assertions.Get(key)
		assertInfo := value.(*AssertInfo)
		blockpath := assertInfo.AssertExtra.BlockPath

		if blockpath != nil && len(blockpath) > 0 {
			matrix.PushBack(blockpath)
		}
	}
	result := make([]common.Hash, 0)
	for i := 0; int64(i) < Cnfw.Int64(); i++ { // the block sequence in the interval
		counters := make(map[string]int, 0)

		for row := matrix.Front(); row != nil; row = row.Next() {

			blockpath := row.Value.([]common.Hash)

			for j := 0; j < len(blockpath); j++ {
				if j == i {
					counters[blockpath[j].Hex()] += 1
					break
				}
			}
		}

		if len(counters) == 0 {
			break
		}

		var acceptedBlock string
		vote := 0

		for key, val := range counters {
			if val > vote {
				vote = val          //出现次数
				acceptedBlock = key //区块hash
			}
		}

		if vote >= nodesNeeded {

			result = append(result, common.HexToHash(acceptedBlock))
			//remove failed assertion paths

		loop:
			for row := matrix.Front(); row != nil; row = row.Next() {
				blockpath := row.Value.([]common.Hash)
				if i >= len(blockpath) {
					row0 := row
					row = row.Next()
					if row == nil {
						break
					}
					matrix.Remove(row0)
					continue
				}

				for j := 0; j < len(blockpath); j++ {
					if j == i {
						if strings.Compare(blockpath[j].Hex(), acceptedBlock) != 0 {
							row0 := row
							row = row.Next()
							if row == nil {
								break loop
							}
							matrix.Remove(row0)
							continue
						}
					}
				}
			}
		} else {
			log.Warn("出现测次数不大于3/2退出", "BlockPath长度", len(result))
			break
		}
	}

	return result
}

func (w *utopiaWorker) createAndBroadcastNormalBlock(task *blockTask) {
	// call miner to create normal block
	ip := w.realIP()
	server := w.utopia.stack.Server()
	port := uint16(server.NodeInfo().Ports.Listener)
	nodeId := discover.PubkeyID(&server.PrivateKey.PublicKey)

	blockMiningReq := &types.BlockMiningReq{Kind: types.NORNAML_BLOCK, Rank: task.rank, Number: task.Number.Uint64(),
		IP: ip, Port: port,
		NodeID:   nodeId,
		CNumber:  w.blockchain.CommitChain.CurrentBlock().Number(),
		IsLandID: IslandIDState.Load().(string), //岛屿ID 空是大陆
		Empty:    task.empty,
	}
	w.utopia.MinerCh <- blockMiningReq
}

//验证签名
func verifySignNormalBlock(block *core_types.Block) bool {
	header := block.Header()
	sig := make([]byte, 65)
	copy(sig, header.Extra[len(header.Extra)-65:])

	//签名清空
	header.Extra = header.Extra[:len(header.Extra)-65]
	data, err := rlp.EncodeToBytes(header)
	if err != nil {
		log.Error("rlp error", "err", err.Error())
		return false
	}
	hash := crypto.Keccak256Hash(data)
	publicKey, _ := utopia.Ecrecover(hash.Bytes(), sig)

	//通过签名和公钥验证签名
	signature := crypto.VerifySignature(publicKey, hash.Bytes(), sig[:len(sig)-1])
	if !signature {
		return signature
	}
	return signature
}

//verify rank
func (w *utopiaWorker) verifyBlockRank(block *core_types.Block, needVerifyedAddress common.Address, needVerifyedRank uint32, isCommit int64, batch int) bool {
	var currentBlock *core_types.Block
	var currentBlockNum uint64
	if isCommit == -1 {
		//计算 normal commit不需要normal高度
		currentBlockNum = block.NumberU64() - 1 //用上一个区间的normal验证rank
		currentBlock = w.blockchain.GetBlockByNumber(currentBlockNum)
		if currentBlock == nil {
			log.Info("verifyBlockRank没有取到currentBlock", "currentBlockNum", currentBlockNum)
			return false
		}
		log.Info("取到的normal高度", "高度", currentBlock.NumberU64())
	}

	master, _, err := w.getMasterOfNextBlock(isCommit, currentBlock, batch)
	if err != nil || master == nil {
		log.Info("verifyBlockRank_getMasterOfNextBlock错误", "master", master, "err", err)
		return false
	}

	log.Info("需要验证的block信息", "高度", block.Number().Int64(), "打块地址", needVerifyedAddress.String(), "rank", needVerifyedRank)

	for index, address := range master {
		// uint32(index+batch*int(numMasters))
		if address == needVerifyedAddress && needVerifyedRank == uint32(index){
			return true
		}
		log.Info("verifyBlockRank计算", "address", address, "Rank", index)
	}
	return false
}

//需要等待第一个commit block 处理完
func (w *utopiaWorker) firstCommit() {
	firstCommitCh := make(chan int, 1)
	for {
		if w.blockchain.CommitChain.CurrentBlock().NumberU64() >= 1 {
			firstCommitCh <- 0
		}
		select {
		case <-firstCommitCh:
			return
		case <-time.After(1 * time.Second):
			log.Error("no receive event")
			break
		}
	}
}

func (w *utopiaWorker) processNormalBlock(block *core_types.Block, sync bool) error {
	timeNow:=time.Now()
	defer func() {
		NormalDeRepetition.Del(block.NumberU64()) //删除normal缓存
		log.Info("保存区块时间","时间",time.Since(timeNow).Seconds())
	}()

	w.firstCommit()

	if block.NumberU64() != w.blockchain.CurrentBlock().NumberU64()+1 {
		log.Error("无效的Normal高度", "normal高度", block.NumberU64(), "当前高度", w.blockchain.CurrentBlock().NumberU64())
		return errors.New("无效的Normal高度")
	}

	blockExtra := types.BlockExtraDecode(block)
	id, state, _, _ := IslandLoad(w.utopia)

	if state && id != block.Coinbase().String() && !sync && blockExtra.IsLandID != "" {
		log.Error("Normal只能保存与自己岛屿ID相同的区块", "岛屿ID", id, "打快地址", block.Coinbase().String())
		return errors.New("Normal只能保存与自己岛屿ID相同的区块")
	}


	log.Info("normalBlock start ", "height", block.NumberU64(), "hash", block.Hash().Hex(), "IP", blockExtra.IP, "rank", blockExtra.Rank)

	if blockExtra.Rank != 0 {
		log.Warn("Normal Rank!=0", "rank", blockExtra.Rank)
	}

	if w.blockchain.CommitChain.CurrentBlock().NumberU64() != 1 || block.NumberU64() != 1 {
		needVerifiedBlockAddress := block.Coinbase()
		needVerifiedRank := blockExtra.Rank
		batch := 0 //根据Rank计算master批次
		if needVerifiedRank >= uint32(numMasters) {
			batch = int(needVerifiedRank / uint32(numMasters))
			log.Debug("收到commitMaster批次", "批次", batch)
		}
		isVerified := w.verifyBlockRank(block, needVerifiedBlockAddress, needVerifiedRank, -1, batch)
		if !isVerified {
			return errors.New("received block not verified")
		}
	}

	// signature verification (此处签的是 block.Header,用的是seal中的sighash)
	signature := verifySignNormalBlock(block)
	if !signature {
		log.Error("verifySign error")
		return errors.New("verifySign error")
	}

	blocks := []*core_types.Block{block}         //保存区块链需要这个参数
	_, error := w.blockchain.InsertChain(blocks) //区块上链
	if error != nil {
		log.Error("processNormalBlock--InsertChain error", "err", error.Error())
		return error
	}
	log.Info("normalBlock保存完毕","num",block.NumberU64(),"rank",blockExtra.Rank,"hash",block.Hash().String())
	return nil
}

func (w *utopiaWorker) prepareNextBlock(currentBlock *core_types.Block, batch int) {
	if currentBlock.NumberU64() != w.blockchain.CurrentBlock().NumberU64() {
		log.Debug("无效的normal高度", "要处理的normal高度", currentBlock.NumberU64(), "当前高度", w.blockchain.CurrentBlock().NumberU64())
		return
	}
	//计算Normal的Rank
	_,rank, consensusQuorumLen, err := w.masterRank(currentBlock, w.utopia.signer, -1, batch)
	if err != nil {
		log.Error("get next block masterRank error","error",err)
		return
	}
	log.Info("normal的rank", "next normal height", currentBlock.NumberU64()+1, "next rank", rank)
	// create & broadcast normal block when it's my turn
	if rank != observer_only { //rank大于0 说明是master
		delete(ExamineBlock.masterBatch, currentBlock.Hash()) //清除批次
		w.createAndBoradcastNormalBlockWithTask(currentBlock.Number().Int64(),rank,currentBlock,false)
	} else {
		//是ob也要打块去触发toProcess
		w.createAndBoradcastNormalBlockWithTask(currentBlock.Number().Int64(),uint32(consensusQuorumLen + int(numMasters) - 1),currentBlock,true)
	}
}

func (w *utopiaWorker) ProcessCommitLogic(block *core_types.Block, batch int, march bool) {
	commitExtra := w.blockchain.CommitChain.CurrentCommitExtra()
	// create & multicast block assertion if on cfd boundary
	currBlockNum := commitExtra.NewBlockHeight.Uint64()
	diff := int64(block.Number().Uint64()) - int64(currBlockNum+Cnfw.Uint64())
	log.Info("diff", "normalHeight", block.Number().Uint64(), "boundary", currBlockNum+Cnfw.Uint64(), "diff", diff)
	//
	if diff == 0 || (diff >= Cnfw.Int64() && diff%Cnfw.Int64() == 0) || march { // Time for block assertion
		// task.Number is the current commit block height
		number := w.blockchain.CommitChain.CurrentBlock().Number()
		log.Info("当前commit高度是","number",number,"batch",batch)
		// create block assertion and multicast it to all m masters of the next commit block
		masters,rank, consensusQuorumLen, err := w.masterRank(block, w.utopia.signer, number.Int64(), batch)
		if err != nil {
			log.Error("masterRank is error", "err", err)
		}
		task := &blockTask{Number: number, masters: masters, rank: rank, block: block}
		log.Info("commitRank", "commit高度", number.Uint64()+1, "rank", rank)
		// change here
		w.createAndMulticastBlockAssertion(task)
		if rank != observer_only {
			// i am a master (with rank m-1 or better) or
			// just a consensus node eager to assume mastership
			w.createAndBroadcastCommitBlock(task)
			log.Info("commit边界 ----------->>>>>>>--------------", "normalHeight", block.Number().Uint64(), "Cfd", currBlockNum+Cnfw.Uint64())
		} else {
			log.Info("i am observer, done after createAndMulticastBlockAssertion.")
			w.emptyCommitBlock(task, number.Int64(), consensusQuorumLen)
		}

	} else {
		log.Info("normal边界 ----------->>>>>>>--------------", "normalHeight", block.Number().Uint64(), "Cfd", currBlockNum+Cnfw.Uint64())
	}
}

func (w *utopiaWorker) emptyCommitBlock(task *blockTask, isCommit int64, consensusQuorumLen int) {
	//模拟asserted的时间
	time.Sleep(time.Millisecond * time.Duration(BlockDelay))

	//模拟BlockExtra
	var blockExtra types.BlockExtra
	blockExtra.Empty = true
	blockExtra.NodeID = discover.PubkeyID(&w.utopia.stack.Server().PrivateKey.PublicKey)

	blockExtra.Rank = uint32(consensusQuorumLen + int(numMasters) - 1) //取委员会的最大值,如果委员会有一个,那这个无效的就是Rank1

	blockExtra.CNumber = big.NewInt(task.Number.Int64() + 1)

	log.Debug("打无效的块的信息", "高度", blockExtra.CNumber.Uint64(), "Rank", blockExtra.Rank)

	currentBlockHash := w.blockchain.CommitChain.CurrentBlock().Hash()

	commitBlock := types.NewCommitblock(&blockExtra, currentBlockHash, w.utopia.signer)

	w.utopia.OnCommitBlock(commitBlock)
}

func (w *utopiaWorker) commitLoop() {
	for {
		select {
		case block := <-w.commitCh:
			w.ProcessCommitLogic(block, 0, false)
			w.prepareNextBlock(block, 0)
			log.Info("commitLoop结束", "normal高度", block.NumberU64())
		case block := <-w.syncJoinCH:
			//判断是否没有收到其他节点发来的块,只能自己打快
			if block.NumberU64() < w.blockchain.CurrentBlock().NumberU64() || w.blockchain.CurrentBlock().NumberU64() == 0 {
				log.Warn("无效的高度")
				break
			}
			//如果只能自己打快,修改自己的岛屿ID
			w.utopia.SetIslandIDState(w.utopia.signer.String(), w.utopia.db)
			w.selfWork() //自己打快
		case examineBlock := <-w.examineBlockCh:
			//没有收到区块,启动下一批master打快

			currentNormalNum := w.blockchain.CurrentBlock().NumberU64()
			currentCommitNum := w.blockchain.CommitChain.CurrentBlock().NumberU64()

			if currentCommitNum == 0 && currentNormalNum == 0 {
				//TODO 如果没有块也连不上主节点，怎么处理
				currentNormalBlock := w.blockchain.CurrentBlock()
				ExamineBlock.masterBatch[currentNormalBlock.Hash()] = 0
			}else {
				batch := examineBlock.masterBatch[examineBlock.block.Hash()] //批次
				w.ProcessCommitLogic(examineBlock.block, batch, false)
				w.prepareNextBlock(examineBlock.block, batch)
			}
		}
	}
}

func (w *utopiaWorker) ProcessCommitBlock(block *core_types.Block, sync bool) error {
	defer func() {
		CommitDeRepetition.Del(block.NumberU64())           //commit删除缓存
		AssociatedCommitDeRepetition.Del(block.NumberU64()) //commit删除缓存
	}()

	newCommitHeight := block.NumberU64()
	currentNum := w.blockchain.CommitChain.CurrentBlock().NumberU64()
	if block.NumberU64() != currentNum+1 {
		log.Debug("commit高度无效,什么都不做", "num", block.NumberU64(), "current", currentNum)
		return errors.New("commit高度无效,什么都不做")
	}

	blockExtra,commitExtra := types.CommitExtraDecode(block)

	id, state, CNum, _ := IslandLoad(w.utopia)
	// 1.本地是岛屿 2.收到的是岛屿块 3.收到的岛屿块跟自己是同一ID 4.高度大于自己裂脑前的高度
	if state && id != block.Coinbase().String() && block.NumberU64() >= uint64(CNum) && !sync && blockExtra.IsLandID != "" {
		log.Error("Commit只能保存与自己岛屿ID相同的区块", "id", id, "区块地址", block.Coinbase().String(), "状态", state, "高度", block.NumberU64(), "hash", block.Hash())
		return errors.New("Commit只能保存与自己岛屿ID相同的区块")
	}

	log.Info("commitBlock processing", "height", newCommitHeight, "hash", block.Hash().Hex(), "IP", blockExtra.IP, "Rank", blockExtra.Rank, "打快地址", block.Coinbase())

	// verify commit height
	needVerifiedAddress := block.Coinbase()
	needVerifiedRank := blockExtra.Rank
	if newCommitHeight != 1 {
		batch := 0 //根据Rank计算master批次
		if needVerifiedRank >= uint32(numMasters) {
			batch = int(needVerifiedRank / uint32(numMasters))
		}
		isVerified := w.verifyBlockRank(block, needVerifiedAddress, needVerifiedRank, int64(newCommitHeight-1), batch)
		if !isVerified {
			log.Info("received commit block not verified", "number is", newCommitHeight, "address", needVerifiedAddress, "rank", needVerifiedRank)
			return errors.New("received commit block not verified")
		}
	}

	currentCommitBlock := w.blockchain.CommitChain.CurrentBlock()
	// 对收到的commitBlock.blockExtra进行的验签
	sign := make([]byte, len(blockExtra.Signature))
	copy(sign, blockExtra.Signature)

	blockExtra.Signature = nil
	err := verifySignBlockExtra(sign, blockExtra)
	if err != nil {
		log.Error("verifySignCommitBlockExtra error")
		return err
	}
	//set NodeID etc
	currentCommitHeight := currentCommitBlock.Number().Uint64()

	// 验证CondensedEvidence 信息
	err = w.verifyCommitEvidence(newCommitHeight,currentCommitHeight,commitExtra)
	if err != nil {
		return err
	}

	log.Debug("BlockPath verify succeed")
	//TODO make sure all blocks in block path are in normal ledger already

	err = w.blockchain.InsertCommitBlock(block)
	if err != nil {
		log.Error("commit保存错误", "err", err.Error())
		return err
	}
	//拦截分叉后新加入的节点
	commitExtra, err = w.processCommitAdditionsAndDeletions(commitExtra)
	if err != nil {
		return err
	}
	log.Info("更新完的委员会成员", "高度", block.NumberU64(), "增加的成员数量", len(commitExtra.MinerAdditions), "减少的成员", len(commitExtra.MinerDeletions))
	//更新委员会
	err = w.upDateQuorum(block,newCommitHeight,commitExtra)
	if err != nil {
		return err
	}
	//log.Debug("BlockPath verify succeed")
	////TODO make sure all blocks in block path are in normal ledger already


	//判断是否是分叉
	w.forkCommit(blockExtra.IsLandID, commitExtra, block, currentCommitBlock)

	w.utopia.CommitFetcher.InsertCh <- struct{}{}

	//存储黑名单
	blacklist.BlackListTxTask.SetBlackList(w.utopia.db, commitExtra.Blacklist)
	blacklist.BlackListTxTask.Decode(commitExtra.Blacklist)
	log.Info("黑名单", "内容", blacklist.BlackListTxTask.Blacklist, "高度", block.NumberU64())

	log.Info("保存CommitBlock完成", "number", block.NumberU64(), "hash", block.Hash().String())

	//send commit block to iaas
	w.sendToIaas(block,blockExtra,commitExtra)

	//send trust tx after n commit block
	err = w.commitSendTx(block)
	if err != nil {
		return err
	}
	return nil
}

func (w *utopiaWorker) verifyCommitEvidence(newCommitHeight uint64,currentCommitHeight uint64,commitExtra types.CommitExtra) error {
	if newCommitHeight > 1 {
		currentQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(currentCommitHeight, w.utopia.db)
		if !ok {
			return errors.New("no quorum existed on commit block height")
		}

		//模拟删除委员会成员
		testCommitQuorum := currentQuorum.Copy()
		for _, del := range commitExtra.MinerDeletions {
			testCommitQuorum.Del(del.String())
		}
		if testCommitQuorum.Len() == 0 {
			log.Error("不能删除所有节点,无效的commit", "当时委员会数量", testCommitQuorum.Len(), "要删除的数量", len(commitExtra.MinerDeletions))
		}

		//增加一个commit块中assertion数量的判断,收到的assertion数量要大于委员会中2/3的数量
		neededNodes := int(math.Ceil(float64(currentQuorum.Len()) / 3 * 2))

		if len(commitExtra.Evidences) < neededNodes {
			neededNodes = len(commitExtra.Evidences) / 3 * 2
		}

		if neededNodes == 0 {
			neededNodes = 1
		}
		log.Info("验证BlockPath", "neededNodes", neededNodes, "commitExtra.Evidences长度", len(commitExtra.Evidences))
		assertions := utils.NewSafeSet()
		for _, condensedEvidence := range commitExtra.Evidences {
			total := make([]common.Hash, 0)
			if condensedEvidence.ExtraKind == types.EVIDENCE_ADD_EXTRA {
				//恢复每个节点的path  多余的区块Hash+标准的BlockPath
				total = append(commitExtra.AcceptedBlocks, condensedEvidence.ExtraBlocks...)
			}

			if condensedEvidence.ExtraKind == types.EVIDENCE_DEL_EXTRA {
				//恢复每个节点的path  标准的BlockPath-多的blockpath
				condensedEvidenceMap := make(map[common.Hash]int, 0)
				for _, v := range condensedEvidence.ExtraBlocks {
					condensedEvidenceMap[v] = 1
				}

				for _, v := range commitExtra.AcceptedBlocks {
					if _, find := condensedEvidenceMap[v]; find {
						continue
					}
					total = append(total, v)
				}
			}

			//验证每一个节点的签名
			err := verifySignAssertBlock(total, condensedEvidence.Signature)
			if err != nil {
				return errors.New("verify sing error")
			}

			var assertExtra types.AssertExtra
			assertExtra.BlockPath = total
			assertions.Add(condensedEvidence.Address.String(), &AssertInfo{AssertExtra: &assertExtra})
		}
		if !commitExtra.Island {
			//如果是大陆,验证path,岛屿不用验证
			blockPath := blockPath(assertions, neededNodes)
			log.Info("BlockPathLen", "len", len(blockPath), "assertions长度", assertions.Len())
			if len(blockPath) != len(commitExtra.AcceptedBlocks) {
				return errors.New("blockPath Verify error")
			}
			//对比blockPath和commitExtra.AcceptedBlocks是否相等
			for index, nblockHahs := range blockPath {
				if nblockHahs != commitExtra.AcceptedBlocks[index] {
					return errors.New("blockPath Verify error")
				}
			}
		}
	}
	return nil
}


func (w *utopiaWorker) upDateQuorum(block *core_types.Block,newCommitHeight uint64,commitExtra types.CommitExtra) error {
	var preSectionHeight uint64
	//取没存之前的commit的newheight
	if w.blockchain.CommitChain.CurrentBlock().NumberU64() != 0 {
		preSectionHeight = w.blockchain.CommitChain.CurrentCommitExtra().NewBlockHeight.Uint64()
	}
	isSaved := UpdateConsensusQuorum(w, newCommitHeight, &commitExtra, block)
	switch isSaved {
	case true:
		err := core.SetCurrentCommitBlock(w.blockchain.CommitChain, block)
		if err != nil {
			log.Error("SetCurrentCommitBlock错误", "err", err)
			return err
		}
		core.UpdateHeightMap(commitExtra.NewBlockHeight.Uint64(), block.NumberU64(), w.blockchain.CommitChain)
		//避免无限rollback
		log.Info("回滚信息", "当前链上Normal高度", w.blockchain.CurrentBlock().NumberU64(), "commitNewHeight+CFD", commitExtra.NewBlockHeight.Uint64()+Cnfw.Uint64())
		if w.blockchain.CurrentBlock().NumberU64() < commitExtra.NewBlockHeight.Uint64()+Cnfw.Uint64() || block.Coinbase().String() == w.utopia.signer.String() {
			forkingFlag, rollbackHeight, newHeight, err := w.blockchain.ReorgChain(commitExtra.AcceptedBlocks, commitExtra.Reset, preSectionHeight)
			if forkingFlag {
				for i := rollbackHeight + 1; i < newHeight; i++ {
					NormalBlockQueued.DelToProcessNormalBlockQueueMap(i) //删除toProcessNormal缓存
				}
				if err != nil {
					log.Error("commit回滚Normal失败", "commit高度", block.NumberU64())
					return err
				}
			}
		}

	case false:
		log.Error("委员会更新错误")
	}
	return nil
}

//存储黑名单
func (w *utopiaWorker) sendToIaas(block *core_types.Block,blockExtra types.BlockExtra,commitExtra types.CommitExtra)  {
	blacklist.BlackListTxTask.SetBlackList(w.utopia.db, commitExtra.Blacklist)
	blacklist.BlackListTxTask.Decode(commitExtra.Blacklist)
	//send commit block to iaas
	if block.Coinbase() == w.utopia.signer {
		go iaasconn.SendToIaas(block, blockExtra, commitExtra)
	}
}

//send trust tx after n commit block
func (w *utopiaWorker) commitSendTx (block *core_types.Block) error {
	sub := new(big.Int).Sub(block.Number(), big.NewInt(utopia.TrustTxCommitBlockLimit))
	if sub.Cmp(big.NewInt(0)) == +1 && block.Coinbase() == w.utopia.signer {
		//信任链信息
		id, status, _, _ := IslandLoad(w.utopia)
		if status && id != w.utopia.signer.String() {
			return nil
		}
		go w.sendTxData(sub)
	}
	return nil
}


func (w *utopiaWorker) forkCommit(islandID string, commitExtra types.CommitExtra, commitBlock *core_types.Block, lastCommitBlock *core_types.Block) {
	//判断保存的块是岛屿,改变自己的状态
	if commitExtra.Island == true {
		log.Error("保存的是岛屿块", "高度", commitBlock.NumberU64())
		//判断是不是第一次变岛屿
		_,lastCommitExtra := types.CommitExtraDecode(lastCommitBlock)
		_, state, cNum, _ := IslandLoad(w.utopia)
		if (!lastCommitExtra.Island && !state) || (state && cNum == 0) {
			//第一次变成岛屿,删除其他节点
			//取上一个commit高度
			log.Info("第一次分叉", "高度", commitBlock.NumberU64(), "last高度", lastCommitBlock.NumberU64())
			lastBlockNum := lastCommitBlock.NumberU64() - 1
			if lastBlockNum < 1 {
				lastBlockNum = 1
			}
			consensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(lastBlockNum, w.utopia.db)
			//第一次保存数据库
			if ok {
				IslandStore(w.utopia, types.BlockExtraDecode(commitBlock).IsLandID, true, int(commitBlock.NumberU64()), consensusQuorum.Keys(), w.utopia.db)
			}
		} else {
			//不是第一次
			log.Info("还在分叉", "高度", commitBlock.NumberU64())
		}
	} else {
		_, _, num, _ := IslandLoad(w.utopia)
		if commitBlock.NumberU64() <= uint64(num) {
			log.Info("还是岛屿不变", "高度", commitBlock.NumberU64())
		} else {
			//保存的大陆块就把岛屿数据清空
			log.Info("现在是大陆块", "高度", commitBlock.NumberU64())
			IslandStore(w.utopia, "", false, 0, []string{}, w.utopia.db)
		}
	}
}

//对commit里面的additions和Deletions进行筛选处理
func (w *utopiaWorker) processCommitAdditionsAndDeletions(commitExtra types.CommitExtra) (types.CommitExtra, error) {

	receiveCommitExtra := commitExtra
	receiveIslandLabel := receiveCommitExtra.Island

	quorumAdditions := receiveCommitExtra.MinerAdditions
	quorumDeletions := receiveCommitExtra.MinerDeletions

	_, localIslandState, _, _ := IslandLoad(w.utopia)

	switch localIslandState {

	case true:
		//本地的是岛屿
		if receiveIslandLabel == true {
			// 收到的也是岛屿块
			// 这里我需要把quorumAdditions里面的成员与分叉前的共识委员会成员进行对比，
			// 只有原共识委员会的成员可以重新加入
			//分叉前的共识委员会可以按照高度去数据库取
			result := w.getIntersections(quorumAdditions)
			if len(result) == 0 {
				//新增成员里面没有属于原共识委员会的，那么所有的增加均无效
				quorumAdditions = nil
			} else {
				quorumAdditions = result
			}

			receiveCommitExtra.MinerAdditions = quorumAdditions
			receiveCommitExtra.MinerDeletions = quorumDeletions
			return receiveCommitExtra, nil
		} else {
			//收到的是大陆块
			return receiveCommitExtra, nil
		}

	case false:
		//本地的是大陆
		if receiveIslandLabel == true {
			//收到的是岛屿块
			//PS : 这里可能有问题
			return receiveCommitExtra, nil
		} else {
			//收到的是大陆块
			return receiveCommitExtra, nil
		}
	}
	return receiveCommitExtra, nil
}

func (w *utopiaWorker) getIntersections(rangeArray []common.Address) []common.Address {
	var resultArray = make([]common.Address, 0)
	quorum := w.utopia.GetIslandQuorumMap()

	//log.Info("原先委员会的成员是", "委员会成员", quorum, "高度", cNum)
	for _, addr := range rangeArray {
		addressStr := addr.String()
		if _,ok := quorum[addressStr]; ok{
			resultArray = append(resultArray, addr)
		}
	}
	return resultArray
}

func UpdateConsensusQuorum(w *utopiaWorker, commitHeight uint64, commitExtra *types.CommitExtra, block *core_types.Block) bool {

	w.updateStaticsAndQualification(commitHeight, commitExtra, block.Coinbase())
	currentCommitHeight := w.blockchain.CommitChain.CurrentBlock().NumberU64()
	quorumAdditions := commitExtra.MinerAdditions
	if currentCommitHeight == 0 {
		// recv'd first commit block after genesis commit block
		var commitQuorum = quorum.NewNodeAddress()
		if len(quorumAdditions) > 0 {
			for _, address := range quorumAdditions {
				commitQuorum.Add(address.Hex(), address)
			}
		}
		quorum.CommitHeightToConsensusQuorum.Set(commitHeight, commitQuorum, w.utopia.db)
	} else {
		currentCommitHeight := w.blockchain.CommitChain.CurrentBlock().Number().Uint64()
		currentCommitHeight -= 1
		if currentCommitHeight == 0 {
			currentCommitHeight = 1
		}
		// retrieve previous node set
		// ignore genesisCommitBlock

		currentQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(currentCommitHeight, w.utopia.db)
		if !ok {
			// the current commit block does NOT have quorum
			//return errors.New("failed to get quorum for current commit height")
			return false
		} else {
			commitQuorum := currentQuorum.Copy()
			// combine (add/delete)
			quorumDeletions := commitExtra.MinerDeletions
			if len(quorumDeletions) > 0 {
				for _, address := range quorumDeletions {
					commitQuorum.Del(address.Hex())
				}
			}

			if len(quorumAdditions) > 0 {
				for _, address := range quorumAdditions {
					commitQuorum.Add(address.Hex(), address)
					if address==w.utopia.signer{
						log.Info("新加入委员会成员,广播node信息","address",address)
						ip := w.realIP()
						server := w.utopia.stack.Server()
						port := uint16(server.NodeInfo().Ports.Listener)
						nodeId := discover.PubkeyID(&server.PrivateKey.PublicKey)
						node:=quorum.BlockchainNode{ID:nodeId,Address:w.utopia.signer,IP:*ip,TCP:port}
						w.utopia.MulticastBlockChainNodeFeed(&node)
					}
				}
			}

			if ConsensusQuorumLimt != 100 && ConsensusQuorumLimt != 0{
				nodesDetails,_ := commitHeight2NodeDetailSetCache.Get(commitHeight,w.utopia.db)

				nodes := make(byQualificationIndex, 0)
				for address,_ := range commitQuorum.Hmap  {
					nodeDetail := nodesDetails.Get(address)
					if nodeDetail != nil {
						nodes = append(nodes,nodeDetail)
					}
				}

				sort.Sort(nodes)

				flt := float64(ConsensusQuorumLimt)/float64(100)

				index := int(math.Ceil(float64(len(nodes))*flt))

				nodes = nodes[:index]

				for address,_ := range commitQuorum.Hmap {
					nodesDetail := nodesDetails.Get(address)
					if !containts(nodes,nodesDetail) {
						commitQuorum.Del(address)
					}
				}

				log.Info("commitQuorumlen","len",commitQuorum.Len())
			}

			quorum.CommitHeightToConsensusQuorum.Set(commitHeight, commitQuorum, w.utopia.db)
		}
	}

	return true
}

func verifySignAssertBlock(BlockPath []common.Hash, sig []byte) error {

	data, err := rlp.EncodeToBytes(BlockPath)
	if err != nil {
		return err
	}

	hash := crypto.Keccak256Hash(data)
	publicKey, err := utopia.Ecrecover(hash.Bytes(), sig)
	if err != nil {
		log.Error("err","err",err)

		return errors.New("publicKey error")
	}

	//根据公钥和签名验证
	ok := crypto.VerifySignature(publicKey, hash.Bytes(), sig[:len(sig)-1])
	if !ok {
		return errors.New("assertExtra sig error")
	}
	return nil
}

func verifySignBlockExtra(sig []byte, blockExtra types.BlockExtra) error {

	data, err := rlp.EncodeToBytes(blockExtra)
	if err != nil {
		log.Error("rlp error", "err", err.Error())
		return err
	}

	hash := crypto.Keccak256Hash(data)
	publicKey, err := utopia.Ecrecover(hash.Bytes(), sig)
	if err != nil {
		log.Error("err","err",err)
		return errors.New("publicKey error")
	}

	//根据公钥和签名验证
	ok := crypto.VerifySignature(publicKey, hash.Bytes(), sig[:len(sig)-1])
	if !ok {
		return errors.New("blockExtra sig error")
	}
	return nil
}

func (w *utopiaWorker) processBlockAssert(block *core_types.Block) error {
	//收到assertBlock 取出 blockExtra 和	assertExtra
	blockExtra,assertExtra := types.AssertExtraDecode(block)

	sig := make([]byte, len(blockExtra.Signature))
	copy(sig, blockExtra.Signature)
	err := verifySignAssertBlock(assertExtra.BlockPath, sig)
	if err != nil {
		log.Error("verifySignAssertBlock error")
		return err
	}

	//验证 blockExtra 签名
	err = verifySignBlockExtra(blockExtra.Signature, blockExtra)
	if err != nil {
		log.Error("block extra in assert block failed to verify")
		return err
	}

	nextCommitNumber := block.NumberU64()
	SynAssertionLock.Lock()
	assertions, ok := AssertCacheObject.Get(nextCommitNumber, w.utopia.db)
	if !ok {
		assertions = utils.NewSafeSet()
		AssertCacheObject.Set(nextCommitNumber, assertions, w.utopia.db)
	}
	//统计收到的assertion
	assertions.Add(block.Coinbase().Hex(),
	&AssertInfo{
		block.Coinbase(), &blockExtra,
		&assertExtra})
	SynAssertionLock.Unlock()
	log.Info("assertion中的数据","高度",nextCommitNumber,"数据",assertions.Keys())
	return nil
}

func (w *utopiaWorker) sendTxData(commitNum *big.Int) {
	//send trust tx
	chainID := conf.ChainId.String()

	hosts := tcUpdate.GetTrustHosts()

	log.Info("get trust hosts", "hosts", hosts)
	if len(hosts) == 0 {
		log.Info("host list is zero, go to get hosts from iaas")
		trustHosts := utopia.GetTrustNodeFromIaas(chainID)
		if len(trustHosts) == 0 {
			log.Error("trust nodes empty from Iaas")
			return
		}
		hosts = trustHosts
	}

	if hosts[0] == "300" {
		log.Info("no under layer trust chain")
		return
	}

	log.Debug("trust nodes hosts", "list", hosts, "len", len(hosts))
	maxNum := utopia.GetMaxNumFromTrustChain(hosts, chainID)
	var commitNums []*big.Int
	if maxNum == nil {
		//commitNum < 100 then send from 1th commitNum
		if commitNum.Cmp(big.NewInt(100)) <= 0 {
			log.Info("get max num from trustChain nil, commitNum < 100", "commitNum", commitNum)
			numLimit := 0 // prevent transaction oversized data
			for i := big.NewInt(1); i.Cmp(commitNum) <= 0; i.Add(i, big.NewInt(1)) {
				if numLimit >= 10 {
					break
				}
				c := new(big.Int).Set(i)
				commitNums = append(commitNums, c)
				numLimit++
			}
		} else {
			log.Error("get max num from trustChain nil")
			commitNums = append(commitNums, commitNum)
		}
	} else {
		log.Info("get max num from trustChain", "maxNum", maxNum.Int64(), "commitNum", commitNum.Int64())
		sub := new(big.Int).Sub(commitNum, maxNum)

		if sub.Cmp(big.NewInt(0)) == +1 {
			numLimit := 0 // prevent transaction oversized data
			for i := maxNum.Add(maxNum, big.NewInt(1)); i.Cmp(commitNum) <= 0; i.Add(i, big.NewInt(1)) {
				if numLimit >= 10 {
					break
				}
				c := new(big.Int).Set(i) // copy
				commitNums = append(commitNums, c)
				numLimit++
			}
		} else {
			commitNums = append(commitNums, commitNum)
		}
	}
	w.sendTx(hosts, chainID, commitNums)
}


func (w *utopiaWorker) sendTx(hosts []string, chainID string, commitNums []*big.Int) {
	ccName := "baap-trusttree"
	version := "v1.0"
	fcn := "putState"

	var params [][]byte
	for _, commitNum := range commitNums {
		commitB := w.blockchain.CommitChain.GetBlockByNum(commitNum.Uint64())
		if commitB == nil {
			log.Error("get block by num nil")
			return
		}
		log.Info("send trust tx start", "sendCommit", commitB.NumberU64())

		type TrustTxData struct {
			PreTrustChainID     string      `json:"preTrustChainID"` // pre trust chain ID
			CurTrustChainID     string      `json:"curTrustChainID"` // cur trust chain ID
			ChainID             string      `json:"chainID"`         // service chain ID
			CommitBlockNo       uint64      `json:"commitBlockNo"`
			CommitBlockHash     common.Hash `json:"commitBlockHash"`
			PrevCommitBlockHash common.Hash `json:"prevCommitBlockHash"`
			NodeAddress         string      `json:"nodeAddress"`
		}
		blockNum := commitB.NumberU64()
		curTrustChainID, preTrustChainID := tcUpdate.TrustHosts.ReadChainID()
		txData := TrustTxData{
			PreTrustChainID:     preTrustChainID,
			CurTrustChainID:     curTrustChainID,
			ChainID:             chainID,
			CommitBlockNo:       blockNum,
			CommitBlockHash:     commitB.Hash(),
			PrevCommitBlockHash: w.blockchain.CommitChain.GetBlockByNum(blockNum - 1).Hash(),
			NodeAddress:         w.utopia.signer.String(),
		}
		txBuf, err := json.Marshal(txData)
		if err != nil {
			log.Error("marshal txData error", "err", err)
			return
		}
		params = append(params, txBuf)
	}

	meta := map[string][]byte{"baap-tx-type": []byte("exec")}

	from := w.utopia.signer
	account := accounts.Account{Address: from}
	wallet, err := w.utopia.AccountManager.Find(account)
	if err != nil {
		log.Error("account manager error", "err", err)
		return
	}

	log.Debug("utopia signer", "from", from)

	//shuffle hosts
	retryN := 0
	utopia.Shuffle(hosts)
retry:
	for i, dstHost := range hosts {
		log.Info("retry trust tx", "i", i)
		if i > 4 {
			log.Error("try five times fail", "i", i)
			break
		}
		if err := utopia.SendTrustTx(account, wallet, dstHost, ccName, version, fcn, params, meta); err != nil {
			if err.Error() == "replacement transaction underpriced" {
				time.Sleep(time.Millisecond * 500)
				continue
			}
			
			if retryN == 0 {
				log.Info("send trust tx fail, get new hosts")
				trustHosts := utopia.GetTrustNodeFromIaas(chainID)
				if len(trustHosts) == 0 {
					log.Error("trust nodes empty from Iaas")
					return
				}
				hosts = trustHosts
				retryN = 1
				goto retry
			}
			continue
		}
		break
	}
}
