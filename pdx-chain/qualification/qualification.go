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
package qualification

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"pdx-chain/common"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"sort"
	"strconv"
	"sync"
	//"pdxc-chain/utopia/utils"
)

var (
	errClosed     = errors.New("object set is closed")
	errAlreadySet = errors.New("object is already set")
	errNotSet     = errors.New("object is not set")
)

const (
	//评判资格的区间距离  100
	DistantOfcdf uint64 = 3
	//活跃区间距离    1000
	ActiveDistant uint64 = 3
)

/* disqualifiedAtResaon */
const (
	LossOfConsensus = "The right to a loss of consensus"
	EmptyString     = ""
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
	NodeMap map[string]*NodeDetail
}

func NewNodeSet() *SafeNodeDetailSet {
	return &SafeNodeDetailSet{
		NodeMap: make(map[string]*NodeDetail),
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

	n.NodeMap[id] = p

	return nil
}

// Del removes a object from the active set.
func (n *SafeNodeDetailSet) Del(id string) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	_, ok := n.NodeMap[id]
	if !ok {
		return errNotSet
	}
	delete(n.NodeMap, id)

	return nil
}

// Get retrieves the registered object with the given id.
func (n *SafeNodeDetailSet) Get(id string) *NodeDetail {
	n.lock.RLock()
	defer n.lock.RUnlock()

	val, ok := n.NodeMap[id]

	if ok {
		return val
	}

	return nil
}

// Len returns if the current number of objects in the set.
func (n *SafeNodeDetailSet) Len() int {
	n.lock.RLock()
	defer n.lock.RUnlock()

	return len(n.NodeMap)
}

func (n *SafeNodeDetailSet) Keys() []string {
	n.lock.RLock()
	defer n.lock.RUnlock()
	keys := make([]string, 0)
	for k, _ := range n.NodeMap {
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
	for k, _ := range n.NodeMap {
		val := n.Get(k)
		if val != nil {
			set.Add(k, val)
		}
	}

	return set
}

func (n *SafeNodeDetailSet) Encode() ([]byte, error) {
	return json.Marshal(n.NodeMap)
}

func (n *SafeNodeDetailSet) Decode(data []byte) error {
	return json.Unmarshal(data, &n.NodeMap)
}

type ByQualificationIndex []*NodeDetail

func (q ByQualificationIndex) Len() int {
	return len(q)
}

func (q ByQualificationIndex) Less(i, j int) bool {
	return q[i].QualificationIndex > q[j].QualificationIndex
}

func (q ByQualificationIndex) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

var CommitHeight2NodeDetailSetCache = &CommitHeight2NodeDetailSet{Height2NodeDetailSet: make(map[uint64]*SafeNodeDetailSet)}

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

	nodeDetailsOld, ok := CommitHeight2NodeDetailSetCache.Get(commitHeightToRemove, db)

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
