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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"pdx-chain/quorum"

	"pdx-chain/common"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	utopia_types "pdx-chain/utopia/types"
	"sort"
	"strconv"
	"sync"
)

var (
	errClosed     = errors.New("object set is closed")
	errAlreadySet = errors.New("object is already set")
	errNotSet     = errors.New("object is not set")
)

const (
	//评判资格的区间距离  100
	distantOfcdf uint64 = 3
	//活跃区间距离    1000
	activedDistant uint64 = 3
)

/* disqualifiedAtResaon */
const (
	lossOfConsensus = "The right to a loss of consensus"
	emptyString     = ""
)

type NodeDetail struct {
	Address common.Address `json:"address"`

	//nil if no assertion for this commit height  - collect assertnum
	NumAssertionsAccepted uint64 `json:"num_assertions_accepted"`

	//number of assertions for the past 100 commit heights
	//need to adjust whenever commit height increases
	NumAssertionsTotal uint64 `json:"num_assertions_total"`

	//number of blocks accepted for this commit height
	NumBlocksAccepted uint64 `json:"num_blocks_accepted"`

	//number of blocks accepted for the past 100 commit heights
	//need to adjust whenever commit height increases
	NumBlocksAcceptedTotal uint64 `json:"num_blocks_accepted_total"`

	//number of blocks failed to verify for this commit height
	NumBlocksFailed uint64 `json:"num_blocks_failed"`

	//number of blocks accepted for the past 100 commit heights
	//need to adjust whenever commit height increases
	NumBlocksFailedTotal uint64 `json:"num_blocks_failed_total"`

	//same as numAssertionsTotal for now
	ActivenessIndex uint64 `json:"activeness_index"`

	//80*numBlocksAcceptedTotal + 20*numAssertionsTotal
	ContributionIndex uint64 `json:"contribution_index"`

	//probed by each node, default to 100 for now
	CapabilitiesIndex uint64 `json:"capabilities_index"`

	//60*activenessIndex + 20*contributionIndex + 20*capabilitiesIndex
	//nodes are ordered as such, then pick the ones joining consensus quorum
	QualificationIndex uint64 `json:"qualification_index"`

	//the commit height this node gets pre-qualified but deferred
	//a pre-qualified node must wait for at 1000 commit height
	//and must be within the 5% increase on each consensus quorum addition
	//if more than 5% is qualified, sort based on the Address.Hex()
	//and select the 5% of them
	PrequalifiedAt uint64 `json:"prequalified_at"`

	//the commit height this node gets qualified as consensus node
	QualifiedAt uint64 `json:"qualified_at"`

	//the commit height this node get disqualified as consensus node
	DisqualifiedAt uint64 `json:"disqualified_at"`

	//disqualified because of
	DisqualifiedReason string `json:"disqualified_reason"`
}

type SafeNodeDetailSet struct {
	lock    sync.RWMutex
	nodeMap map[string]*NodeDetail
}

func NewNodeSet() *SafeNodeDetailSet {
	return &SafeNodeDetailSet{
		nodeMap: make(map[string]*NodeDetail),
	}
}

func (n *NodeDetail) String() string {
	b, err := json.Marshal(*n)
	if err != nil {
		return fmt.Sprintf("%+v", *n)
	}
	var out bytes.Buffer
	err = json.Indent(&out, b, "", "    ")
	if err != nil {
		return fmt.Sprintf("%+v", *n)
	}
	return out.String()
}

// Add injects a new object into the working set, or returns an error if the
// object is already known.
func (n *SafeNodeDetailSet) Add(id string, p *NodeDetail) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.nodeMap[id] = p

	return nil
}

// Del removes a object from the active set.
func (n *SafeNodeDetailSet) Del(id string) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	//_, ok := n.nodeMap[id]
	//if !ok {
	//	return errNotSet
	//}
	delete(n.nodeMap, id)
	return nil
}

// Get retrieves the registered object with the given id.
func (n *SafeNodeDetailSet) Get(id string) *NodeDetail {
	n.lock.RLock()
	defer n.lock.RUnlock()

	val, ok := n.nodeMap[id]

	if ok {
		return val
	}

	return nil
}

// Len returns if the current number of objects in the set.
func (n *SafeNodeDetailSet) Len() int {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return len(n.nodeMap)
}

func (n *SafeNodeDetailSet) Keys() []string {
	n.lock.RLock()
	defer n.lock.RUnlock()
	keys := make([]string, 0)
	for k, _ := range n.nodeMap {
		keys = append(keys, k)
	}

	return keys
}

func (n *SafeNodeDetailSet) KeysOrdered() []string {
	keys := n.Keys()

	sort.Strings(keys)

	return keys
}

func (n *SafeNodeDetailSet) Copy() *SafeNodeDetailSet {

	set := NewNodeSet()

	n.lock.RLock()
	defer n.lock.RUnlock()
	for k, _ := range n.nodeMap {
		val := n.Get(k)
		if val != nil {
			set.Add(k, val)
		}
	}

	return set
}

func (n *SafeNodeDetailSet) Encode() ([]byte, error) {
	return json.Marshal(n.nodeMap)
}

func (n *SafeNodeDetailSet) Decode(data []byte) error {
	return json.Unmarshal(data, &n.nodeMap)
}

type byQualificationIndex []*NodeDetail

func (q byQualificationIndex) Len() int {
	return len(q)
}

func (q byQualificationIndex) Less(i, j int) bool {
	return q[i].QualificationIndex > q[j].QualificationIndex
}

func (q byQualificationIndex) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

var commitHeight2NodeDetailSetCache = &CommitHeight2NodeDetailSet{Height2NodeDetailSet: make(map[uint64]*SafeNodeDetailSet)}

type CommitHeight2NodeDetailSet struct {
	lock              sync.RWMutex
	commitBlockHeight uint64 // commitBlockHeight of commit block
	// safeset is composed of consensus nodes (address.hex -> NodeDetail)
	Height2NodeDetailSet map[uint64]*SafeNodeDetailSet
}

func (q *CommitHeight2NodeDetailSet) Set(height uint64, set *SafeNodeDetailSet, db ethdb.Database) {
	q.lock.Lock()
	defer q.lock.Unlock()

	q.Height2NodeDetailSet[height] = set

	if height > q.commitBlockHeight {
		q.commitBlockHeight = height
	}

	if db == nil {
		return
	}

	data, err := set.Encode()
	if err == nil {
		db.Put([]byte("commit-statistics:"+strconv.Itoa(int(height))), data)
	}
}

func (q *CommitHeight2NodeDetailSet) Get(height uint64, db ethdb.Database) (*SafeNodeDetailSet, bool) {
	q.lock.RLock()
	defer q.lock.RUnlock()
	set, ok := q.Height2NodeDetailSet[height]
	if ok {
		return set, ok
	}

	if db == nil {
		return nil, false
	}

	data, err := db.Get([]byte("commit-statistics:" + strconv.Itoa(int(height))))
	if err != nil {
		return nil, false
	}

	var set2 = NewNodeSet()
	err = set2.Decode(data)
	if err != nil {
		return nil, false
	}

	if height > q.commitBlockHeight {
		q.commitBlockHeight = height
	}
	return set2, true
}

func (q *CommitHeight2NodeDetailSet) Dup(height uint64, db ethdb.Database) (*SafeNodeDetailSet, bool) {
	set, ok := q.Get(height, db)
	if !ok {
		return nil, false
	}

	q.lock.Lock()
	defer q.lock.Unlock()

	data, err := set.Encode()
	if err != nil {
		log.Info("encode error", "err", err.Error())
		return nil, false
	}

	set2 := NewNodeSet()
	err = set2.Decode(data)

	if err != nil {
		log.Info("decode error", "err", err.Error())
		return nil, false
	}
	return set2, true
}

func (q *CommitHeight2NodeDetailSet) Del(height uint64, db ethdb.Database) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if height == q.commitBlockHeight {
		q.commitBlockHeight--
	}

	delete(q.Height2NodeDetailSet, height)
	if db != nil {
		db.Delete([]byte("commit-statistics:" + strconv.Itoa(int(height))))
	}
}

func (q *CommitHeight2NodeDetailSet) DelData(startNum uint64, address common.Address, db ethdb.Database) {
	q.lock.Lock()
	defer q.lock.Unlock()

	nodetails, ok := q.Height2NodeDetailSet[startNum]
	if ok {
		nodetail := nodetails.Get(address.String())
		nodetail = ClearnUpNodeDetailInfo(nodetail,startNum)
		nodetails.Add(address.String(),nodetail)

		data, err := nodetails.Encode()
		if err == nil {
			log.Info("保存了高度","num",startNum)
			db.Put([]byte("commit-statistics:"+strconv.Itoa(int(startNum))), data)
		}
	}
}

func (q *CommitHeight2NodeDetailSet) Keys() []uint64 {
	q.lock.RLock()
	defer q.lock.RUnlock()

	keys := make([]uint64, 0)
	for k, _ := range q.Height2NodeDetailSet {
		keys = append(keys, k)
	}

	return keys
}

// remove the counters on heightToRemove fro nodeDetails for each node as we move forward to a new commit height
func discountStaticsAndQualification(nodeDetails *SafeNodeDetailSet, commitHeightToRemove uint64, db ethdb.Database) *SafeNodeDetailSet {

	nodeDetailsOld, ok := commitHeight2NodeDetailSetCache.Get(commitHeightToRemove, db)
	if !ok {
		return nodeDetails
	}

	for _, key := range nodeDetails.Keys() {

		curval := nodeDetails.Get(key)

		oldval := nodeDetailsOld.Get(key)

		//is that means ?
		curval.NumBlocksAccepted -= oldval.NumBlocksAccepted

		curval.NumAssertionsAccepted -= oldval.NumAssertionsAccepted

		curval.NumBlocksFailed -= oldval.NumBlocksFailed

		curval.NumAssertionsTotal -= oldval.NumAssertionsAccepted

		curval.NumBlocksAcceptedTotal -= oldval.NumBlocksAccepted

		curval.NumBlocksFailedTotal -= oldval.NumBlocksFailed
	}

	return nodeDetails
}

func (w *utopiaWorker) updateStaticsAndQualification(commitHeight uint64, commitExtra *utopia_types.CommitExtra, miner common.Address) {

	//get or create node details set for this commit height
	nodeDetails, ok := commitHeight2NodeDetailSetCache.Dup(commitHeight-1, w.utopia.db)
	if !ok {
		nodeDetails = NewNodeSet()
		nodeDetails.Add(miner.Hex(), &NodeDetail{Address: miner})
	} else {
		minerDetail := nodeDetails.Get(miner.Hex())
		if minerDetail == nil {
			nodeDetails.Add(miner.Hex(), &NodeDetail{Address: miner})
		}
	}
	//log.Info("updateStaticsAndQualification更新委员会状态", "高度", commitHeight-1, "minerDetails", nodeDetails.KeysOrdered())
	localIslandState := w.utopia.GetIslandState()

	unbifQuerum := w.utopia.GetIslandQuorumMap()

	log.Info("未分叉前的共识委员会","unbifQuerum",w.utopia.GetIslandQuorum(),"高度",commitHeight)

	for address ,_ := range unbifQuerum {
		log.Info("unbifQuerumMap","address",address)
	}


	nodeDetails = w.upDataBlockAcceptedValue(commitExtra,nodeDetails,localIslandState,unbifQuerum)

	//NumAssertionsAccepted 信息统计
	nodeDetails = w.upDataAssertionsAccepted(commitExtra,nodeDetails,commitHeight,localIslandState,unbifQuerum)
	// 减少资格
	if commitHeight > distantOfcdf {
		w.clearnUpActiveNum(commitHeight,nodeDetails)
	}

	// update qualification indices
	for key, value := range nodeDetails.nodeMap {
		if localIslandState && !contants(unbifQuerum, key) {
			continue
		}
		//活跃度总量
		value.ActivenessIndex = value.NumAssertionsTotal
		value.ContributionIndex = 80*value.NumBlocksAcceptedTotal + 20*value.NumAssertionsTotal
		value.CapabilitiesIndex = 100 //TODO each node probe and commit node verify

		value.QualificationIndex = 60*value.ActivenessIndex + 20*value.ContributionIndex + 20*value.CapabilitiesIndex
	}

	// persist node statistics details 更新最新的高度
	commitHeight2NodeDetailSetCache.Set(commitHeight, nodeDetails, w.utopia.db)
}


func (w *utopiaWorker) upDataBlockAcceptedValue(commitExtra *utopia_types.CommitExtra,nodeDetails *SafeNodeDetailSet,localIslandState bool,unbifQuerum map[string]int) *SafeNodeDetailSet {
	var emptyBlockHash []*common.Hash
	// update block contributions
	blockpath := commitExtra.AcceptedBlocks
	for _, hash := range blockpath {
		block := w.utopia.blockchain.GetCacheBlock(hash)
		if block == nil {
			emptyBlockHash = append(emptyBlockHash, &hash)
		} else {
			coinbase := block.Coinbase()
			if coinbase == [20]byte{0} {
				panic("error creater coinbase is empty")
			}
			//如果是岛屿状态并且不在分叉前的委员会中
			if localIslandState && !contants(unbifQuerum, coinbase.String()) {
				continue
			}
			// nodeDetail 活跃度纪录 coinbase对应的
			value := nodeDetails.Get(coinbase.Hex())
			if value == nil {
				value = &NodeDetail{Address: block.Coinbase()}
				nodeDetails.Add(block.Coinbase().Hex(), value)
			}
			value.NumBlocksAccepted++
			value.NumBlocksAcceptedTotal++
			nodeDetails.Add(coinbase.Hex(),value)
		}
	}
	return nodeDetails
}

func (w *utopiaWorker) upDataAssertionsAccepted(commitExtra *utopia_types.CommitExtra,nodeDetails *SafeNodeDetailSet,commitHeight uint64,localIslandState bool,unbifQuerum map[string]int) *SafeNodeDetailSet {
	for _, evidence := range commitExtra.Evidences {
		//log.Info("取出的evidence数量", "evidence地址", evidence.Address)
		if localIslandState && !contants(unbifQuerum, evidence.Address.Hex()) {
			log.Info("岛屿状态并且未分叉前的共识委员会不包含","address",evidence.Address.Hex())
			continue
		}
		//10
		evidenceValue := nodeDetails.Get(evidence.Address.Hex())
		if evidenceValue == nil {
			evidenceValue = &NodeDetail{Address: evidence.Address}
			nodeDetails.Add(evidence.Address.Hex(), evidenceValue)
		}

		evidenceValue.NumAssertionsAccepted++
		evidenceValue.NumAssertionsTotal++

		//判断assert数量够不够  增加资格
		if evidenceValue.NumAssertionsTotal >= activedDistant {
			//至少证明了我活跃数量是够了 接下来判断我活跃的区间是不是过去的1000个区间
			newNodeDetails, ok := w.nodeDetailsWithDistanct(activedDistant, commitHeight-1)
			if !ok {
				log.Error("no data on this height:", "height", commitHeight-1)
				panic("no data on this height:")
			} else {
				//一段区间的活跃的差值
				newNodeDetail := newNodeDetails.Get(evidenceValue.Address.Hex())
				if newNodeDetail == nil {
					log.Error("一段区间的活跃的差值 ---没有节点记录", "address", evidenceValue.Address.Hex())
					panic("")
				}
				if newNodeDetail.NumAssertionsTotal >= activedDistant {
					//本高度的100个区间 newNodeDetail.NumAssertionsTotal >= distantOfcdf/3*2
					// && commitHeight >= newNodeDetail.DisqualifiedAt+activedDistant) -- 失去资格但是重新活跃了activedDistant个区间
					if newNodeDetail.PrequalifiedAt == 0 &&
						(newNodeDetail.DisqualifiedAt == 0 || (newNodeDetail.DisqualifiedAt != 0 &&
							commitHeight >= newNodeDetail.DisqualifiedAt+activedDistant)) {

						evidenceValue.PrequalifiedAt = commitHeight
						evidenceValue.QualifiedAt = commitHeight + 2

						evidenceValue.DisqualifiedAt = 0
						evidenceValue.DisqualifiedReason = emptyString
					}
				}
			}
		}
		//log.Info("增加资格的地址", "地址", evidence.Address)
		nodeDetails.Add(evidence.Address.Hex(), evidenceValue)
	}
	return nodeDetails
}

func (w *utopiaWorker) clearnUpActiveNum (commitHeight uint64,nodeDetails *SafeNodeDetailSet)  {
	prevConsensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(commitHeight-1, w.utopia.db)
	if !ok {
		log.Error("no prev ConsensusQuorum on height :", "height", commitHeight-2)
		panic("no prev ConsensusQuorum on height")
	} else {
		//前一高度的委员会成员
		for _, address := range prevConsensusQuorum.Keys() {
			currentHeightNodeDetails, ok := w.nodeDetailsWithDistanct(distantOfcdf, commitHeight-1)
			if ok {
				nodeDetail := currentHeightNodeDetails.Get(address)
				//log.Info("减少的资格", "地址", nodeDetail.Address.Hex(), "活跃度", nodeDetail.NumAssertionsTotal,
				//	"nodeDetail.QualifiedAt", nodeDetail.QualifiedAt, "nodeDetail.DisqualifiedAt", nodeDetail.DisqualifiedAt)
				if nodeDetail != nil {
					//活跃度小于 distantOfcdf/3*2&&在委员会中&&没有被剔除
					if nodeDetail.NumAssertionsTotal < distantOfcdf/3*2 &&
						nodeDetail.QualifiedAt != 0 && nodeDetail.DisqualifiedAt == 0 {
						nodeDetail = ClearnUpNodeDetailInfo(nodeDetail,commitHeight)
						log.Info("失去资格的地址", "地址", address)
						nodeDetails.Add(address, nodeDetail)
					}
				}
			}
		}
	}
}

//清理活跃度
func ClearnUpNodeDetailInfo(nodeDetail *NodeDetail,commitHeight uint64) *NodeDetail {
	nodeDetail.NumAssertionsTotal = 0
	nodeDetail.NumAssertionsAccepted = 0
	nodeDetail.NumBlocksAccepted = 0
	nodeDetail.NumBlocksAcceptedTotal = 0
	//nodeDetail.QualifiedAt = 0
	nodeDetail.PrequalifiedAt = 0
	nodeDetail.DisqualifiedAt = commitHeight
	nodeDetail.DisqualifiedReason = lossOfConsensus
	return nodeDetail
}

func contants(quorumList map[string]int, address string) bool {
	_,ok := quorumList[address]
	if ok {
		return true
	}
	return false
}

//获取当前高度距离指定距离的NodeDetails集合
func (w *utopiaWorker) nodeDetailsWithDistanct(dist uint64, commitHeight uint64) (*SafeNodeDetailSet, bool) {

	nodeDetails, ok := commitHeight2NodeDetailSetCache.Dup(commitHeight, w.utopia.db)
	if !ok {
		log.Info("no NodeDetailSet on height :", "height", commitHeight)
		return nil, false
	}

	if commitHeight < dist {
		return nodeDetails, true
	}

	if commitHeight-dist >= 1 {
		oldNodeDetails, ok := commitHeight2NodeDetailSetCache.Dup(commitHeight-dist, w.utopia.db)
		if !ok {
			log.Error("no NodeDetailSet on prevheight :", "prevheight", commitHeight-dist)
			//panic("no NodeDetailSet on prevheight")
		}

		for _, addressHex := range nodeDetails.Keys() {
			currentNodeDetail := nodeDetails.Get(addressHex)
			oldNodeDetail := oldNodeDetails.Get(addressHex)
			if oldNodeDetail == nil {
				continue
			}

			if int(currentNodeDetail.NumAssertionsTotal) - int(oldNodeDetail.NumAssertionsTotal) >= 0 {
				currentNodeDetail.NumAssertionsAccepted -= oldNodeDetail.NumAssertionsAccepted
				currentNodeDetail.NumAssertionsTotal -= oldNodeDetail.NumAssertionsTotal
				currentNodeDetail.NumBlocksAccepted -= oldNodeDetail.NumBlocksAccepted
				currentNodeDetail.NumBlocksAcceptedTotal -= oldNodeDetail.NumBlocksAcceptedTotal
			}else {
				continue
			}
		}
	}

	return nodeDetails, true
}

// called on create and broadcast commit block
func (w *utopiaWorker) calculateMembershipUpdates(commitHeight uint64) (minerAdditions []common.Address, minerDeletions []common.Address,
	nodeAdditions []common.Address, nodeDeletions []common.Address) {

	if commitHeight <= 0 {
		log.Warn("no need to calculate membership update for commit height 0")
		return nil, nil, nil, nil
	}

	if commitHeight == 1 {
		log.Warn("commit height 1 only has local node as miner addition")
		return []common.Address{w.utopia.signer}, nil, nil, nil
	}

	log.Info("calculating membership update for commit height:", "height", commitHeight)

	prevConsensusQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(commitHeight-2, w.utopia.db)
	if !ok {
		return nil, nil, nil, nil
	}
	log.Info("取前两个高度的共识委员会","height",commitHeight-2,"quorum",prevConsensusQuorum.Keys())

	prevNodeSet, ok := w.nodeDetailsWithDistanct(distantOfcdf, commitHeight-1)
	if !ok {
		log.Warn("failed to get node set for commmit height:", "height", commitHeight)
		return nil, nil, nil, nil
	}

	// get current nodes (active, capable and willing-to-contribute)
	currNodes,nodeAdditions,err := w.getNodeAdditions(commitHeight,prevNodeSet)
	if err != nil {
		return nil,nil,nil,nil
	}

	// order all nodes based on their qualification index
	//所有节点的活跃度
	currMiners := currNodes

	//移除失效的
	currMiners = removeFailureNode(currMiners)

	tempNode,err := w.removeActivityNotEnough(commitHeight,currMiners,prevConsensusQuorum)
	if err != nil {
		return nil,nil,nil,nil
	}

	currMiners = tempNode

	//拣选共识委员会成员大于%5的
	currMiners,err = w.pickUpLegalNodeIntoConsensQuorum(commitHeight,currMiners,prevConsensusQuorum,prevNodeSet)
	if err != nil {
		return nil,nil,nil,nil
	}

	// persist current consensus quorum into database
	currConsensusQuorum := quorum.NewNodeAddress()
	for _, v := range currMiners {
		//log.Info("挑选出来的", "地址", v.Address.Hex())
		currConsensusQuorum.Add(v.Address.Hex(), v.Address)
	}

	prevMiners,minerAdditions := getMinerAdditions(prevConsensusQuorum,currMiners)
	// for each in previous miner quorum, find deletions in new miner quorum

	minerDeletions = w.getMinerDeletions(prevMiners,currConsensusQuorum)
	log.Info("finished calculating membership update for commit height:", "commitHeight", commitHeight,
		"minerAdditions", len(minerAdditions), "minerDeletions", len(minerDeletions), "nodeAdditions", len(nodeAdditions), "nodeDeletions", len(nodeDeletions))

	return minerAdditions, minerDeletions, nodeAdditions, nodeDeletions
}


func(w *utopiaWorker) getNodeAdditions(commitHeight uint64,prevNodeSet *SafeNodeDetailSet) (byQualificationIndex,[]common.Address,error) {
	// get current nodes (active, capable and willing-to-contribute)
	currNodeSet, ok := w.nodeDetailsWithDistanct(distantOfcdf, commitHeight)
	if !ok {
		log.Warn("failed to get node set for commmit height:", "height", commitHeight)
		return nil,nil,errors.New("failed to get node set for commmit height:")
	}

	currNodes := make(byQualificationIndex, 0, currNodeSet.Len())

	//对应高度中活跃度差值中所有地址的活跃度
	for _,v := range currNodeSet.nodeMap  {
		if v == nil {
			continue
		}
		currNodes = append(currNodes, v)
	}

	// get previous nodes (active, capable and willing-to-contribute)
	//var prevNodeSet *SafeNodeDetailSet
	prevNodes := make(byQualificationIndex, 0, prevNodeSet.Len())
	for _, v := range prevNodeSet.nodeMap {
		if v == nil {
			continue
		}
		prevNodes = append(prevNodes, v)
	}

	// for each in new node quorum, find additions from previous node quorum
	nodeAdditions := make([]common.Address, 0)
	for _, v := range currNodes {
		n := prevNodeSet.Get(v.Address.Hex())
		if n == nil {
			nodeAdditions = append(nodeAdditions, v.Address)
		}
	}

	if len(nodeAdditions) == 0 {
		nodeAdditions = nil
	}
	return currNodes,nodeAdditions,nil
}

//移除失效的
func removeFailureNode(currMiners byQualificationIndex) byQualificationIndex {
	tempCurrNodes := make(byQualificationIndex, 0)
	for _, nodeDetail := range currMiners {
		if nodeDetail.DisqualifiedAt == 0 && nodeDetail.DisqualifiedReason == emptyString {
			//log.Info("currMiners中的地址", "地址", nodeDetail.Address, "活跃度", nodeDetail.NumAssertionsTotal)
			tempCurrNodes = append(tempCurrNodes, nodeDetail)
		}
	}
	//把要剔除的节点过滤
	if len(tempCurrNodes) != 0 {
		currMiners = tempCurrNodes
	}
	return currMiners
}

//移除活跃度不够的
func(w *utopiaWorker) removeActivityNotEnough (commitHeight uint64,currMiners byQualificationIndex,prevConsensusQuorum *quorum.NodeAddress) (byQualificationIndex,error) {
	//// remove ones that must be deferred qualification
	tempArray := make(byQualificationIndex, 0)
	// remove ones that must be deferred qualification
	for _, nodeDetail := range currMiners {
		if nodeDetail.QualifiedAt > commitHeight && nodeDetail.QualifiedAt != 0 || nodeDetail.QualifiedAt == 0 {
			//addressString := w.utopia.signer.String()
			if len(prevConsensusQuorum.Keys()) == 1 && prevConsensusQuorum.Keys()[0] == nodeDetail.Address.String() {
				//当共识节点俩列表里面只有自己的时候并不移除
				tempArray = append(tempArray,nodeDetail)
			}
		}else {
			tempArray = append(tempArray,nodeDetail)
		}
	}
	return tempArray,nil
}


func(w *utopiaWorker) pickUpLegalNodeIntoConsensQuorum(commitHeight uint64,currMiners byQualificationIndex,prevConsensusQuorum *quorum.NodeAddress,prevNodeSet *SafeNodeDetailSet) (byQualificationIndex,error) {
	// TODO pick the highest 5% of previous consensus quorum
	// get previous consensus quorum
	if commitHeight > 1 {
		prevNodes := make(byQualificationIndex, 0)
		for _, addressHex := range prevConsensusQuorum.Keys() {
			prevNode := prevNodeSet.Get(addressHex)
			if prevNode == nil {
				log.Warn("no this address nodeDetain on height:", "height", commitHeight-2)
				continue
			}
			prevNodes = append(prevNodes, prevNode)
		}

		if len(prevNodes) != 1 {
			//make shure sorted by qu
			if !sort.IsSorted(prevNodes) {
				sort.Sort(prevNodes)
			}
		}

		//从currMiners当中挑选出再上一共识委员会的活跃纪录加进来
		for _, nodeDet := range currMiners {
			for _, prevNodeDet := range prevNodes {
				if nodeDet.Address.String() == prevNodeDet.Address.String() {
					if !containts(currMiners,nodeDet) {
						currMiners = append(currMiners, nodeDet)
					}
				}
			}
		}
	}
	return currMiners,nil
}

func getMinerAdditions(prevConsensusQuorum *quorum.NodeAddress,currMiners byQualificationIndex) ([]common.Address,[]common.Address) {
	prevMiners := make([]common.Address, 0)
	for _, k := range prevConsensusQuorum.Keys() {
		v := prevConsensusQuorum.Get(k)
		if v == [20]byte{} {
			continue
		}
		prevMiners = append(prevMiners, v)
	}

	// for each in new miner quorum, find additions from previous miner quorum
	minerAdditions := make([]common.Address, 0)
	for _, v := range currMiners {
		//log.Info("最新挑选出来的", "地址", v.Address.Hex())
		n := prevConsensusQuorum.Get(v.Address.Hex())
		if n == [20]byte{} {
			minerAdditions = append(minerAdditions, v.Address)
		}
	}


	minerLen := len(minerAdditions)

	canMinerAddition := math.Ceil(float64(len(prevMiners))  * (float64(BecameQuorumLimt) /float64(100)))

	if len(prevMiners) < 10{
		canMinerAddition = 1.0
	}

	log.Info("看一下数据","canMinerAddition",canMinerAddition)
	if minerLen > int(canMinerAddition) {
		minerAdditions = minerAdditions[:int(canMinerAddition)]
	}

	if len(minerAdditions) == 0 {
		minerAdditions = nil
	}
	return prevMiners,minerAdditions
}


func (w *utopiaWorker)getMinerDeletions(prevMiners []common.Address,currConsensusQuorum *quorum.NodeAddress) []common.Address {
	minerDeletions := make([]common.Address, 0)
	for _, v := range prevMiners {
		n := currConsensusQuorum.Get(v.Hex())
		if n == [20]byte{} {
			minerDeletions = append(minerDeletions, v)
		}
	}

	if len(minerDeletions) == 0 {
		minerDeletions = nil
	}
	currentCommitHeight:=w.blockchain.CommitChain.CurrentBlock().NumberU64()
	currentQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(currentCommitHeight,w.utopia.db)
	if !ok {
		return minerDeletions
	}

	for _, del := range minerDeletions {
		currentQuorum.Del(del.String())
	}
	if currentQuorum.Len() == 0 {
		log.Error("不能删除所有节点增加一个")
		newMinerDeletions := make([]common.Address, 0)
		for _,add:=range minerDeletions{
			if add!=w.utopia.signer{
				newMinerDeletions=append(newMinerDeletions,add)
			}
		}
		return newMinerDeletions
	}



	return minerDeletions
}


func containts(currMiners []*NodeDetail,node *NodeDetail) bool {
	for _,v := range currMiners {
		if v == node {
			return true
		}
	}
	return false
}


func (w *utopiaWorker) clearnUpRecordByAddress(address string) bool {
	for _, height := range commitHeight2NodeDetailSetCache.Keys() {
		nodesDetails, ok := commitHeight2NodeDetailSetCache.Get(height, w.utopia.db)
		if !ok {
			return false
		} else {
			if isContant(nodesDetails.Keys(), address) {
				nodesDetails.Del(address)
				continue
			}
		}
	}
	return true
}

func isContant(targetArray []string, address string) bool {
	for _, value := range targetArray {
		if value == address {
			return true
		}
	}
	return false
}

func DeleteCommitHeight2NodeDetailSet(key string, db ethdb.Database) {
	for _, heightKey := range commitHeight2NodeDetailSetCache.Keys() {
		nodeDetail, ok := commitHeight2NodeDetailSetCache.Get(heightKey, db)
		if ok {
			for _, address := range nodeDetail.Keys() {
				if address != key {
					nodeDetail.Del(address)
				}
			}
			commitHeight2NodeDetailSetCache.Set(heightKey, nodeDetail, db)
		}
	}
}
