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
	//"bytes"
	"errors"
	"math/big"
	"pdx-chain/cacheBlock"
	"pdx-chain/eth/fetcher"
	"pdx-chain/examineSync"
	"pdx-chain/p2p"
	"pdx-chain/quorum"
	"pdx-chain/utopia/utils/whitelist"
	//"math/rand"
	"sync"
	//"time"

	"pdx-chain/accounts"
	"pdx-chain/common"
	//"pdxc-chain/common/hexutil"
	"pdx-chain/consensus"
	//"pdxc-chain/consensus/misc"
	"pdx-chain/core/state"
	"pdx-chain/core/types"
	//"pdxc-chain/crypto"
	"pdx-chain/crypto/sha3"
	"pdx-chain/ethdb"
	//"pdxc-chain/log"
	"pdx-chain/params"
	"pdx-chain/rlp"
	"pdx-chain/rpc"

	"pdx-chain/core"

	"pdx-chain/event"
	utopia_types "pdx-chain/utopia/types"

	"pdx-chain/node"
	"pdx-chain/p2p/discover"

	"pdx-chain/crypto"
	"pdx-chain/log"
	"time"
)

var (
	// errUnknownBlock is returned when the list of signers is requested for a block
	// that is not part of the local blockchain.
	errUnknownBlock = errors.New("unknown block")

	// errInvalidCheckpointBeneficiary is returned if a checkpoint/epoch transition
	// block has a beneficiary set to non-zeroes.
	errInvalidCheckpointBeneficiary = errors.New("beneficiary in checkpoint block non-zero")

	// errInvalidVote is returned if a nonce value is something else that the two
	// allowed constants of 0x00..0 or 0xff..f.
	errInvalidVote = errors.New("vote nonce not 0x00..0 or 0xff..f")

	// errInvalidCheckpointVote is returned if a checkpoint/epoch transition block
	// has a vote nonce set to non-zeroes.
	errInvalidCheckpointVote = errors.New("vote nonce in checkpoint block non-zero")

	// errMissingVanity is returned if a block's extra-data section is shorter than
	// 32 bytes, which is required to store the signer vanity.
	errMissingVanity = errors.New("extra-data 32 byte vanity prefix missing")

	// errMissingSignature is returned if a block's extra-data section doesn't seem
	// to contain a 65 byte secp256k1 signature.
	errMissingSignature = errors.New("extra-data 65 byte suffix signature missing")

	// errExtraSigners is returned if non-checkpoint block contain signer data in
	// their extra-data fields.
	errExtraSigners = errors.New("non-checkpoint block contains extra signer list")

	// errInvalidCheckpointSigners is returned if a checkpoint block contains an
	// invalid list of signers (i.e. non divisible by 20 bytes, or not the correct
	// ones).
	errInvalidCheckpointSigners = errors.New("invalid signer list on checkpoint block")

	// errInvalidMixDigest is returned if a block's mix digest is non-zero.
	errInvalidMixDigest = errors.New("non-zero mix digest")

	// errInvalidUncleHash is returned if a block contains an non-empty uncle list.
	errInvalidUncleHash = errors.New("non empty uncle hash")

	// errInvalidDifficulty is returned if the difficulty of a block is not either
	// of 1 or 2, or if the value does not match the turn of the signer.
	errInvalidDifficulty = errors.New("invalid difficulty")

	// ErrInvalidTimestamp is returned if the timestamp of a block is lower than
	// the previous block's timestamp + the minimum block period.
	ErrInvalidTimestamp = errors.New("invalid timestamp")

	// errInvalidVotingChain is returned if an authorization list is attempted to
	// be modified via out-of-range or non-contiguous headers.
	errInvalidVotingChain = errors.New("invalid voting chain")

	// errUnauthorized is returned if a header is signed by a non-authorized entity.
	errUnauthorized = errors.New("unauthorized")

	// errWaitTransactions is returned if an empty block is attempted to be sealed
	// on an instant chain (0 second period). It's important to refuse these as the
	// block reward is zero, so an empty block just bloats the chain... fast.
	errWaitTransactions = errors.New("waiting for transactions")

	// Ethash proof-of-work protocol constants.
	FrontierBlockReward    *big.Int = big.NewInt(5e+18) // Block reward in wei for successfully mining a block
	ByzantiumBlockReward   *big.Int = big.NewInt(3e+18) // Block reward in wei for successfully mining a block upward from Byzantium
	maxUncles                       = 2                 // Maximum number of uncles allowed in a single block
	allowedFutureBlockTime          = 15 * time.Second  // Max time from current time allowed for blocks, before they're considered future blocks
)

// SignerFn is a signer callback function to request a hash to be signed by a
// backing account.
type SignerFn func(accounts.Account, []byte) ([]byte, error)

// sigHash returns the hash which is used as input for the proof-of-authority
// signing. It is the hash of the entire header apart from the 65 byte signature
// contained at the end of the extra data.
//
// Note, the method requires the extra data to be at least 65 bytes, otherwise it
// panics. This is done to avoid accidentally using both forms (signature present
// or not), which could be abused to produce different hashes for the same header.
func SigHash(header *types.Header) (hash common.Hash) {
	hasher := sha3.NewKeccak256()

	rlp.Encode(hasher, []interface{}{
		header.ParentHash,
		header.UncleHash,
		header.Coinbase,
		header.Root,
		header.TxHash,
		header.ReceiptHash,
		header.Bloom,
		header.Difficulty,
		header.Number,
		header.GasLimit,
		header.GasUsed,
		header.Time,
		header.Extra[:len(header.Extra)-65], // Yes, this will panic if extra is too short
		header.MixDigest,
		header.Nonce,
	})
	hasher.Sum(hash[:0])
	return hash
}

const (
	BlocksPerMasterTurn int = 1  // TODO configurable
	BlockCommitInterval int = 10 //one commit block after blockCommitInterval normal blocks
	NumMastersPerBlock  int = 10 // TODO configurable
)

type temporaryBlockBranch struct {
	// candidate for each block in section
	blocks [BlockCommitInterval]types.Block

	// rank of master for the ith block
	mranks [BlockCommitInterval]int32
}

// Utopia is the secure, fair, scalable and high performance consensus from jz@pdxc.ltd
type Utopia struct {
	config *params.UtopiaConfig // Consensus engine configuration parameters
	db     ethdb.Database       // Database to store and retrieve snapshot checkpoints
	blockchain *core.BlockChain
	MinerCh chan *utopia_types.BlockMiningReq //trigger block mining if/when needed
	exitCh chan struct{}
	assertCh chan *types.Block
	commitCh chan *types.Block
	mux *event.TypeMux
	signer common.Address // Ethereum address of the signing key
	signFn SignerFn       // Signer function to authorize hashes with
	lock   sync.RWMutex   // Protects the signer fields
	worker *utopiaWorker
	stack *node.Node
	Fetcher       *fetcher.Fetcher
	CommitFetcher *fetcher.CbFetcher
	AccountManager *accounts.Manager
	BlockChainNodeFeed event.Feed


}

// New creates a Utopia proof-of-authority consensus engine with the initial
// signers set to the ones provided by the user.
func New(config *params.UtopiaConfig, db ethdb.Database, stack *node.Node) *Utopia {
	// Set any missing consensus parameters to their defaults
	conf := *config
	utopia := &Utopia{
		config:   &conf,
		db:       db,
		exitCh:   make(chan struct{}),
		assertCh: make(chan *types.Block, 1),
		commitCh: make(chan *types.Block, 1),

		stack: stack,
	}

	if utopia.config.Cfd == 0 {
		utopia.config.Cfd = 5
	}
	Cnfw.SetInt64(utopia.config.Cfd)

	if utopia.config.NumMasters == 0 {
		utopia.config.NumMasters = 4
	}
	numMasters = utopia.config.NumMasters

	if utopia.config.BlockDelay == 0 {
		utopia.config.BlockDelay = 2000
	}
	BlockDelay = utopia.config.BlockDelay

	if utopia.config.ConsensusQuorumLimt == 0{
		utopia.config.ConsensusQuorumLimt = 100
	}
	ConsensusQuorumLimt = utopia.config.ConsensusQuorumLimt

	if utopia.config.BecameQuorumLimt == 0 {
		utopia.config.BecameQuorumLimt = 5
	}
	BecameQuorumLimt = utopia.config.BecameQuorumLimt

	Majority = utopia.config.Majority


	return utopia
}

func (c *Utopia) Start() {
	whitelist.InitTrustTxWhiteList()
	go c.worker.Start()
}

func (c *Utopia) Stop() {
	close(c.exitCh)
}

func (c *Utopia) Init() {
	c.worker = newWorker(c)
}

func (c *Utopia) DB() ethdb.Database {
	return c.db
}


func (c *Utopia) Worker() *utopiaWorker {
	return c.worker
}

func (c *Utopia) SetBlockchain(blockchain *core.BlockChain) {
	c.blockchain = blockchain
}

func (c *Utopia) SetEventMux(mux *event.TypeMux) {
	c.mux = mux
}

func (c *Utopia) SetMinerChannel(ch chan *utopia_types.BlockMiningReq) {
	c.MinerCh = ch
}

func (c *Utopia) Server() *p2p.Server{
	return c.stack.Server()
}



func (c *Utopia) SetIslandState(isIsland bool, db ethdb.Database) {
	IslandState.Store(isIsland)
	if db == nil {
		return
	}
	var value int64
	if isIsland {
		value = 1
	} else {
		value = 0
	}
	db.Put([]byte("islandState"), common.IntToHex(value))
}

func (c *Utopia) GetIslandState() bool {
	value := IslandState.Load()
	if value == nil {
		islandByte, err := c.db.Get([]byte("islandState"))
		if err != nil {
			return false
		}
		intValue := common.BytesToInt(islandByte)
		var result bool
		if intValue == 0 {
			result = false
		} else {
			result = true
		}
		IslandState.Store(result)
		return result
	}
	return value.(bool)
}

func (c *Utopia) SetIslandIDState(islandID string, db ethdb.Database) {
	IslandIDState.Store(islandID)
	if db == nil {
		return
	}
	db.Put([]byte("islandIDState"), []byte(islandID))
}

func (c *Utopia) GetIslandIDState() string {
	value := IslandIDState.Load()
	if value == nil || value == "" {
		islandIDByte, err := c.db.Get([]byte("islandIDState"))
		if err != nil {
			return ""
		}
		IslandIDState.Store(string(islandIDByte))
		return string(islandIDByte)
	}
	return value.(string)
}

func (c *Utopia) SetIslandCNum(cnm int, db ethdb.Database) {
	IslandCNum.Store(cnm)
	if db == nil {
		return
	}
	db.Put([]byte("islandCNum"), common.IntToHex(int64(cnm)))
}

func (c *Utopia) GetIslandCNum() int {
	value := IslandCNum.Load().(int)
	if value == 0 {
		numByte, err := c.db.Get([]byte("islandCNum"))
		if err != nil {
			return 0
		}

		return int(common.BytesToInt(numByte))
	}
	return value
}

func (c *Utopia) SetIslandQuorum(querum []string, db ethdb.Database) {
	IslandQuorum.Store(querum)
	if db == nil {
		return
	}
	quorumByte, err := rlp.EncodeToBytes(querum)
	if err != nil {
		log.Error("rlp编码失败")
		return
	}
	db.Put([]byte("islandQuorum"), quorumByte)
}

func (c *Utopia) GetIslandQuorum() []string {
	value:=IslandQuorum.Load().([]string)
	if value == nil {
		valueByte, err := c.db.Get([]byte("islandQuorum"))
		if err != nil {
			return []string{}
		}
		var quorum = make([]string, 0)
		err = rlp.DecodeBytes(valueByte, &quorum)
		if err != nil {
			log.Error("rlp解码失败", "error", err)
		}
		IslandQuorum.Store(quorum)
		value=quorum
	}
	return value
}

func (c *Utopia) GetIslandQuorumMap() map[string]int {
	quorum:=IslandQuorum.Load().([]string)
	if quorum == nil {
		valueByte, err := c.db.Get([]byte("islandQuorum"))
		if err != nil {
			return map[string]int{}
		}

		err = rlp.DecodeBytes(valueByte, &quorum)
		if err != nil {
			log.Error("rlp解码失败", "error", err)
		}
		IslandQuorum.Store(quorum)
	}

	quorumMap := make(map[string]int)
	for index,address := range quorum {
		quorumMap[address] = index
	}
	return quorumMap
}



func (c *Utopia) OnNormalBlock(block *types.Block) error {

	blockExtra := utopia_types.BlockExtraDecode(block)
	if blockExtra.IP == nil {
		log.Error("empty block")
		return errors.New("empty block")
	}

	c.connectPeer(blockExtra, block)

	ProcessNormalBlock(block, c, c.worker.processNormalBlock, c.BroadcastNormalBlock, false)

	return nil
}

func (c *Utopia) OnAssertBlock(block *types.Block) error {
	log.Info("收到assertion","地址",block.Coinbase().String(),"高度",block.NumberU64())
	return c.worker.processBlockAssert(block)
}

func (c *Utopia) OnCommitBlock(block *types.Block) error {
	blockExtra := utopia_types.BlockExtraDecode(block)

	c.connectPeer(blockExtra, block)

	ProcessCommitBlock(block, c, c.worker.ProcessCommitBlock, c.BroadcastCommitBlock, true)

	return nil
}

func (c *Utopia) BroadcastNormalBlock(block *types.Block) {
	id, state, _, _ := IslandLoad(c)
	if state && id != block.Coinbase().String() {
		return
	}
	cacheBlock.CacheBlocks.AddBlock(block)
	c.mux.Post(core.NewMinedBlockEvent{block})
}

func (c *Utopia) BroadcastCommitBlock(block *types.Block) {
	id, state, _, _ := IslandLoad(c)
	if state && id != block.Coinbase().String() {
		return
	}
	cacheBlock.CacheBlocks.AddBlock(block)
	log.Info("广播给其他人","number",block.NumberU64())
	c.mux.Post(utopia_types.NewCommitBlockEvent{block})
}

func (c *Utopia) connectPeer(blockExtra utopia_types.BlockExtra, block *types.Block) {

	quorum.BlockChainNodeSet.Set(block.Coinbase().String(), &quorum.BlockchainNode{ID: blockExtra.NodeID, IP: blockExtra.IP, TCP: blockExtra.Port,Address:block.Coinbase()}, c.db)
	examineSync.IDAndBlockAddress.AddIDAndAddress(blockExtra.NodeID.NodeIDString(),block.Coinbase().String())

	currentNode := &discover.Node{ID: blockExtra.NodeID, IP: blockExtra.IP, TCP: blockExtra.Port}

	selfId := discover.PubkeyID(&c.stack.Server().PrivateKey.PublicKey)

	if currentNode.ID != [64]byte{0}  && currentNode.ID != selfId {
		examineSync.DiscoverEnode.AddDiscoverEnode(currentNode.ID.String(),currentNode)
		c.PreConnectPeers()
	}
}

func (c *Utopia) PreConnectPeers() error {
	// if not already connected, connect, setup and add it to the p2p peerlist
	c.lock.Lock()
	defer c.lock.Unlock()
	peers := c.stack.Server().GetPeers()

	examineSync.DiscoverEnode.Lock.RLock()
	for idString, node := range examineSync.DiscoverEnode.DiscoverNode {
		if _, ok := peers[idString];!ok{
			if node != nil {
				log.Info("PreConnectPeers发送新链接")
				go c.stack.Server().AddPeer(node)
			}
		}
	}
	examineSync.DiscoverEnode.Lock.RUnlock()
	return nil
}

func (c *Utopia) MulticastAssertBlock(block *types.Block, nodes []*discover.Node) error {
	log.Info("多播给了几个节点","nodes",len(nodes))
	return c.mux.Post(utopia_types.NewAssertBlockEvent{block, nodes})
}

func (c *Utopia) MulticastBlockChainNodeFeed(node *quorum.BlockchainNode)  {
	c.BlockChainNodeFeed.Send(node)
	return
}


// Author implements consensus.Engine, returning the Ethereum address recovered
// from the signature in the header's extra-data section.
func (c *Utopia) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules.
func (c *Utopia) VerifyHeader(chain consensus.ChainReader, header *types.Header, seal bool) error {
	return c.verifyHeader(chain, header, nil)
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers. The
// method returns a quit channel to abort the operations and a results channel to
// retrieve the async verifications (the order is that of the input slice).
func (c *Utopia) VerifyHeaders(chain consensus.ChainReader, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	abort := make(chan struct{})
	results := make(chan error, len(headers))

	go func() {
		for i, header := range headers {
			err := c.verifyHeader(chain, header, headers[:i])

			select {
			case <-abort:
				return
			case results <- err:
			}
		}
	}()
	return abort, results
}

// verifyHeader checks whether a header conforms to the consensus rules.The
// caller may optionally pass in a batch of parents (ascending order) to avoid
// looking those up from the database. This is useful for concurrently verifying
// a batch of new headers.
func (c *Utopia) verifyHeader(chain consensus.ChainReader, header *types.Header, parents []*types.Header) error {

	if header.Number == nil {
		return errUnknownBlock
	}

	// Don't waste time checking blocks from the future
	//if header.Time.Cmp(big.NewInt(time.Now().Unix())) > 0 {
	//	return consensus.ErrFutureBlock
	//}

	// All basic checks passed, verify cascading fields
	//return c.verifyCascadingFields(chain, header, parents)
	return nil
}

// VerifyUncles implements consensus.Engine, always returning an error for any
// uncles as this consensus mechanism doesn't permit uncles.
func (c *Utopia) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	if len(block.Uncles()) > 0 {
		return errors.New("uncles not allowed")
	}
	return nil
}

// VerifySeal implements consensus.Engine, checking whether the signature contained
// in the header satisfies the consensus protocol requirements.
func (c *Utopia) VerifySeal(chain consensus.ChainReader, header *types.Header) error {
	return c.verifySeal(chain, header, nil)
}

// verifySeal checks whether the signature contained in the header satisfies the
// consensus protocol requirements. The method accepts an optional list of parent
// headers that aren't yet part of the local blockchain to generate the snapshots
// from.
func (c *Utopia) verifySeal(chain consensus.ChainReader, header *types.Header, parents []*types.Header) error {

	return nil
}

// Prepare implements consensus.Engine, preparing all the consensus fields of the
// header for running the transactions on top.
func (c *Utopia) Prepare(chain consensus.ChainReader, header *types.Header) error {

	return nil
}

// Finalize implements consensus.Engine, ensuring no uncles are set, nor block
// rewards given, and returns the final block.
func (c *Utopia) Finalize(chain consensus.ChainReader, header *types.Header, state *state.StateDB, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	// Accumulate any block and uncle rewards and commit the final state root
	if !c.config.NoRewards {
		//genesis文件配置
		accumulateRewards(chain.Config(), state, header, uncles)
	}
	header.Root = state.IntermediateRoot(chain.Config().IsEIP158(header.Number))
	// Header seems complete, assemble into a block and return
	return types.NewBlock(header, txs, uncles, receipts), nil
}

// Some weird constants to avoid constant memory allocs for them.
var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

// AccumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func accumulateRewards(config *params.ChainConfig, state *state.StateDB, header *types.Header, uncles []*types.Header) {
	// Select the correct block reward based on chain progression
	blockReward := FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ByzantiumBlockReward
	}
	// Accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)
	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		state.AddBalance(uncle.Coinbase, r)

		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}
	state.AddBalance(header.Coinbase, reward)
}

// Authorize injects a private key into the consensus engine to mint new blocks
// with.
func (c *Utopia) Authorize(signer common.Address, signFn SignerFn) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.signer = signer
	c.signFn = signFn
}

// Seal implements consensus.Engine, attempting to create a sealed block using
// the local signing credentials.
func (c *Utopia) Seal(chain consensus.ChainReader, block *types.Block, stop <-chan struct{}) (*types.Block, error) {
	header := block.Header()

	// Sealing the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return nil, errUnknownBlock
	}

	c.lock.RLock()
	signer, signFn := c.signer, c.signFn
	c.lock.RUnlock()
	data, err := rlp.EncodeToBytes(header)

	if err != nil {
		log.Error("EncodeToBytes出错")
		return nil, errors.New("EncodeToBytes出错")
	}

	hash := crypto.Keccak256Hash(data)
	sighash, err := signFn(accounts.Account{Address: signer}, hash.Bytes())
	if err != nil {
		return nil, err
	}

	header.Extra = append(header.Extra, sighash...)
	blockseal := block.WithSeal(header)

	return blockseal, nil
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have based on the previous blocks in the chain and the
// current signer.
func (c *Utopia) CalcDifficulty(chain consensus.ChainReader, time uint64, parent *types.Header) *big.Int {
	difficulty := big.NewInt(128)
	return difficulty
}

// Close implements consensus.Engine. It's a noop for Utopia as there is are no background threads.
func (c *Utopia) Close() error {
	return nil
}

// APIs implements consensus.Engine, returning the user facing RPC API to allow
// controlling the signer voting.
func (c *Utopia) APIs(chain consensus.ChainReader) []rpc.API {
	return []rpc.API{{
		Namespace: "Utopia",
		Version:   "1.0",
		Service:   &API{chain: chain, Utopia: c},
		Public:    false,
	}}
}
