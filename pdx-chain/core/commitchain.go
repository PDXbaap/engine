package core

import (
	"github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"math/big"
	"pdx-chain/cacheBlock"
	"pdx-chain/common"
	"pdx-chain/common/hexutil"
	"pdx-chain/consensus"
	"pdx-chain/core/rawdb"
	"pdx-chain/core/types"
	"pdx-chain/crypto"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"pdx-chain/metrics"
	utopia_types "pdx-chain/utopia/types"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MAXHEIGHTSIZE      = 100000
	HEIGHTSIZEINTERVAL = 10000
)

var sectionInsertTimer = metrics.NewRegisteredTimer("section/chain/inserts", nil)

type CommitChain struct {
	blockChain         *BlockChain
	db                 ethdb.Database
	currentBlock       atomic.Value
	currentBlockExtra  atomic.Value
	currentCommitExtra atomic.Value
	blockHeightMap     map[uint64][]uint64
	blockCache         *lru.Cache
	numberCache        *lru.Cache
	badBlocks          *lru.Cache
	genesisBlock       *types.Block
	mu                 sync.RWMutex
	chainmu            sync.RWMutex
	heightmu           sync.RWMutex
	wg                 sync.WaitGroup
	procInterrupt      int32
}

func NewCommitChain(blockChain *BlockChain) *CommitChain {
	blockCache, _ := lru.New(blockCacheLimit)
	badBlocks, _ := lru.New(badBlockLimit)
	numberCache, _ := lru.New(numberCacheLimit)
	blockHeightMap := make(map[uint64][]uint64)
	sc := &CommitChain{
		db:             blockChain.db,
		blockChain:     blockChain,
		blockCache:     blockCache,
		numberCache:    numberCache,
		badBlocks:      badBlocks,
		blockHeightMap: blockHeightMap,
	}
	sc.genesisBlock = sc.GetBlockByNum(0)
	sc.currentBlock.Store(sc.genesisBlock)
	err := sc.loadLastState()
	if err != nil {
		log.Error("loadLastState failed", "err", err.Error())
	}
	return sc
}

func (cc *CommitChain) updateHeightMap(normalBlockHeight, commitNum uint64) {
	cc.heightmu.Lock()
	defer cc.heightmu.Unlock()
	commitNums := cc.blockHeightMap[normalBlockHeight]
	cc.blockHeightMap[normalBlockHeight] = append(commitNums, commitNum)
	for index, commitNum := range cc.blockHeightMap[normalBlockHeight] {
		log.Info("+++++++++++++++++++", "normalHeight:", normalBlockHeight, "commitNum:", commitNum, "index:", index)
	}
}

//后来的
func UpdateHeightMap(normalBlockHeight, commitNum uint64, cc *CommitChain) {
	cc.heightmu.Lock()
	defer cc.heightmu.Unlock()
	commitNums := cc.blockHeightMap[normalBlockHeight]
	cc.blockHeightMap[normalBlockHeight] = append(commitNums, commitNum)
	for index, commitNum := range cc.blockHeightMap[normalBlockHeight] {
		log.Info("+++++++++++++++++++", "normalHeight:", normalBlockHeight, "commitNum:", commitNum, "index:", index)
	}
}

func (cc *CommitChain) DeleteHeightMap(normalBlockHeight uint64) {
	cc.heightmu.Lock()
	defer cc.heightmu.Unlock()
	delete(cc.blockHeightMap, normalBlockHeight)
}

func (cc *CommitChain) GetCommitNumByNormalHeight(normalBlockHeight uint64) []uint64 {
	cc.heightmu.RLock()
	defer cc.heightmu.RUnlock()
	log.Info("normal对应高度","normal",normalBlockHeight,"commit",cc.blockHeightMap[normalBlockHeight])
	return cc.blockHeightMap[normalBlockHeight]
}

func (cc *CommitChain) getBlockHeightMapSize() uint64 {
	cc.heightmu.RLock()
	defer cc.heightmu.RUnlock()
	return uint64(len(cc.blockHeightMap))
}

func (sc *CommitChain) getProcInterrupt() bool {
	return atomic.LoadInt32(&sc.procInterrupt) == 1
}

// Stop stops the commitchain service. If any imports are currently in progress
// it will abort them using the procInterrupt.
func (sc *CommitChain) Stop() {
	atomic.StoreInt32(&sc.procInterrupt, 1)
	sc.wg.Wait()
	log.Info("Commitchain manager stopped")
}

//PDX 只查找,不增加换的的GetBlock
func (sc *CommitChain) SearchingBlock(hash common.Hash, number uint64) *types.Block {
	// Short circuit if the block's already in the cache, retrieve otherwise
	block := rawdb.ReadBlock(sc.db, hash, number)
	if block == nil {
		return nil
	}
	return block
}

func (sc *CommitChain) GetBlock(hash common.Hash, number uint64) *types.Block {
	// Short circuit if the block's already in the cache, retrieve otherwise
	if block, ok := sc.blockCache.Get(hash); ok {
		return block.(*types.Block)
	}
	block := rawdb.ReadBlock(sc.db, hash, number)
	if block == nil {
		return nil
	}
	// Cache the found block for next time and return
	return block
}

func (sc *CommitChain) CommitChain() *CommitChain {
	return sc.blockChain.CommitChain
}

func (sc *CommitChain) GetBlockByNum(number uint64) *types.Block {
	hash := rawdb.ReadCanonicalCommitHash(sc.db, number)
	if hash == (common.Hash{}) {
		return nil
	}
	return sc.GetBlock(hash, number)
}

func (sc *CommitChain) GetBlockByHash(hash common.Hash) *types.Block {
	var number *uint64
	if cached, ok := sc.numberCache.Get(hash); ok {
		number = cached.(*uint64)
	} else {
		number = rawdb.ReadHeaderNumber(sc.db, hash)
		if number != nil {
			sc.numberCache.Add(hash, number)
		} else {
			return nil
		}
	}
	return sc.GetBlock(hash, *number)
}

func (sc *CommitChain) HasBlock(hash common.Hash, number uint64) bool {
	return rawdb.HasBody(sc.db, hash, number)
}

func (sc *CommitChain) CurrentBlock() *types.Block {
	return sc.currentBlock.Load().(*types.Block)
}

//后加的
func SetCurrentCommitBlock(sc *CommitChain, block *types.Block) error {
	sc.currentBlock.Store(block)

	extra := new(utopia_types.BlockExtra)
	err := extra.Decode(sc.CurrentBlock().Extra())
	if err != nil {
		return err
	}
	sc.currentBlockExtra.Store(extra)

	commitExtra := new(utopia_types.CommitExtra)
	err = commitExtra.Decode(extra.Extra)
	if err != nil {
		return err
	}

	sc.currentCommitExtra.Store(commitExtra)
	return nil
}

//原来的
func (sc *CommitChain) SetCurrentBlock(block *types.Block) error {
	sc.currentBlock.Store(block)

	extra := new(utopia_types.BlockExtra)
	err := extra.Decode(sc.CurrentBlock().Extra())
	if err != nil {
		return err
	}
	sc.currentBlockExtra.Store(extra)

	commitExtra := new(utopia_types.CommitExtra)
	err = commitExtra.Decode(extra.Extra)
	if err != nil {
		return err
	}
	sc.currentCommitExtra.Store(commitExtra)
	return nil
}

func (sc *CommitChain) CurrentBlockExtra() *utopia_types.BlockExtra {
	return sc.currentBlockExtra.Load().(*utopia_types.BlockExtra)
}

func (sc *CommitChain) CurrentCommitExtra() *utopia_types.CommitExtra {
	return sc.currentCommitExtra.Load().(*utopia_types.CommitExtra)
}

func (sc *CommitChain) loadLastState() error {
	// Restore the last known head block
	head := rawdb.ReadHeadCommitBlockHash(sc.db)
	if head == (common.Hash{}) {
		// Corrupt or empty database, init from scratch
		log.Warn("Empty database, resetting chain")
		return sc.Reset()
	}
	// Make sure the entire head block is available
	currentBlock := sc.GetBlockByHash(head)
	if currentBlock == nil {
		// Corrupt or empty database, init from scratch
		log.Warn("Head block missing, resetting chain", "hash", head)
		return sc.Reset()
	}
	// Everything seems to be fine, set as the head block
	err := sc.SetCurrentBlock(currentBlock)
	if err != nil {
		return err
	}
	size := sc.getBlockHeightMapSize()
	if size == 0 {
		currentNum := currentBlock.Number().Uint64()
		var i uint64
		for i = 0; i <= currentNum; i++ {
			block := sc.GetBlockByNum(i)

			extra := new(utopia_types.BlockExtra)
			extraErr := extra.Decode(block.Extra())
			if extraErr != nil {
				log.Error("decode err", "err", extraErr)
				return extraErr
			}

			commitExtra := new(utopia_types.CommitExtra)
			commitErr := commitExtra.Decode(extra.Extra)
			if commitErr != nil {
				log.Error("decode err", "err", commitErr)
				return commitErr
			}
			sc.updateHeightMap(commitExtra.NewBlockHeight.Uint64(), i)
		}
	}
	return nil
}

// Reset purges the entire commitchain, restoring it to its genesis state.
func (sc *CommitChain) Reset() error {
	return sc.ResetWithGenesisBlock(sc.genesisBlock)
}

// ResetWithGenesisBlock purges the entire commitchain, restoring it to the
// specified genesis state.
func (sc *CommitChain) ResetWithGenesisBlock(genesis *types.Block) error {
	// Dump the entire block chain and purge the caches
	if err := sc.SetHead(0); err != nil {
		return err
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()

	rawdb.WriteBlock(sc.db, genesis)

	sc.genesisBlock = genesis
	err := sc.insert(sc.genesisBlock)
	if err != nil {
		return err
	}
	err = sc.SetCurrentBlock(sc.genesisBlock)
	if err != nil {
		return err
	}

	return nil
}
func (sc *CommitChain) DeleteCacheBlock(hash common.Hash) {
	sc.blockCache.Remove(hash)
}

func (sc *CommitChain) SetHead(head uint64) error {
	log.Warn("Rewinding sectionchain", "target", head)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	height := uint64(0)
	if cb := sc.CurrentBlock(); cb != nil {
		height = cb.Header().Number.Uint64()
	}
	batch := sc.db.NewBatch()
	var err error
	for cb := sc.CurrentBlock(); cb != nil && cb.Header().Number.Uint64() > head; cb = sc.CurrentBlock() {
		hash := cb.Hash()
		num := cb.Header().Number.Uint64()
		rawdb.DeleteBody(batch, hash, num)
		rawdb.DeleteHeader(batch, hash, num)

		err = sc.SetCurrentBlock(sc.GetBlock(cb.Header().ParentHash, cb.Header().Number.Uint64()-1))
		if err != nil {
			return err
		}
	}
	// Roll back the canonical chain numbering
	for i := height; i > head; i-- {
		rawdb.DeleteCanonicalCommitHash(batch, i)
	}
	batch.Write()

	if sc.CurrentBlock() == nil {
		err = sc.SetCurrentBlock(sc.genesisBlock)
		if err != nil {
			return err
		}
	}

	rawdb.WriteHeadCommitHeaderHash(sc.db, sc.CurrentBlock().Hash())

	currentHeader := sc.CurrentBlock().Header()

	// Clear out any stale content from the caches
	sc.blockCache.Purge()
	sc.numberCache.Purge()

	// Rewind the block chain, ensuring we don't end up with a stateless head block
	if currentBlock := sc.CurrentBlock(); currentBlock != nil && currentHeader.Number.Uint64() < currentBlock.NumberU64() {
		err = sc.SetCurrentBlock(sc.GetBlock(currentHeader.Hash(), currentHeader.Number.Uint64()))
		if err != nil {
			return err
		}
	}
	// If either blocks reached nil, reset to the genesis state
	if currentBlock := sc.CurrentBlock(); currentBlock == nil {
		err = sc.SetCurrentBlock(sc.genesisBlock)
		if err != nil {
			return err
		}
	}
	currentBlock := sc.CurrentBlock()

	rawdb.WriteHeadCommitBlockHash(sc.db, currentBlock.Hash())

	return sc.loadLastState()
}

func (sc *CommitChain) InsertChain(block *types.Block) error {
	return sc.insertChain(block)
}

func (sc *CommitChain) insertChain(block *types.Block) error {
	// Sanity check that we have something meaningful to import
	if block == nil {
		return errors.New("block==nil")
	}
	// Pre-checks passed, start the full block imports
	sc.wg.Add(1)
	defer sc.wg.Done()
	sc.chainmu.Lock()
	defer sc.chainmu.Unlock()
	// If the chain is terminating, stop processing blocks
	if atomic.LoadInt32(&sc.procInterrupt) == 1 {
		log.Debug("Premature abort during blocks processing")
		return errors.New("Premature abort during blocks processing")
	}
	// If the header is a banned one, straight out abort
	if BadHashes[block.Hash()] {
		return ErrBlacklistedHash
	}
	// Wait for the block's verification to complete
	bstart := time.Now()
	if sc.HasBlock(block.Hash(), block.NumberU64()) {
		return errors.New("commit block has existed")
	}

	// Write the block to the chain and get the status.
	status, err := sc.WriteBlock(block)
	if err != nil {
		return err
	}
	switch status {
	case CanonStatTy:

		log.Debug("Inserted new commit block", "number", block.Number(), "Rank", utopia_types.BlockExtraDecode(block).Rank, "hash", block.Hash(), "uncles", len(block.Uncles()),
			"txs", len(block.Transactions()), "gas", block.GasUsed(), "elapsed", common.PrettyDuration(time.Since(bstart)))

		sc.blockCache.Add(block.Hash(), block)

		sectionInsertTimer.UpdateSince(bstart)

		cacheBlock.CacheBlocks.DelBlock(block)

	case SideStatTy:
		log.Debug("Inserted forked commit block", "number", block.Number(), "hash", block.Hash(), "diff", block.Difficulty(), "elapsed",
			common.PrettyDuration(time.Since(bstart)), "txs", len(block.Transactions()), "gas", block.GasUsed(), "uncles", len(block.Uncles()))

		sectionInsertTimer.UpdateSince(bstart)
	}
	return nil
}

func (sc *CommitChain) WriteBlock(block *types.Block) (status WriteStatus, err error) {
	sc.wg.Add(1)
	defer sc.wg.Done()
	parent := sc.GetBlock(block.ParentHash(), block.NumberU64()-1)
	if parent == nil {
		log.Error("unknow ancestor", "num", block.NumberU64(), "parentHash", block.ParentHash())
		return NonStatTy, consensus.ErrUnknownAncestor
	}
	currentBlock := sc.CurrentBlock()
	if currentBlock.NumberU64() >= block.NumberU64() {
		log.Error("当前commit已经存过")
		return NonStatTy, errors.New("当前commit已经存过")
	}

	sc.mu.Lock()
	defer sc.mu.Unlock()
	//根据是否是岛屿 判断主链还是侧链
	//commitExtra := utopia_types.CommitExtraDecode(block)
	rawdb.WriteBlock(sc.db, block)
	status = CanonStatTy
	//} else if currentBlock.NumberU64() == block.NumberU64() {
	//	currentExtra := utopia_types.BlockExtra{}
	//	err := currentExtra.Decode(currentBlock.Extra())
	//	if err!=nil {
	//		return NonStatTy,err
	//	}
	//	blockExtra := utopia_types.BlockExtra{}
	//	err = blockExtra.Decode(block.Extra())
	//	if err!=nil {
	//		return NonStatTy,err
	//	}
	//	//reorg
	//	if currentExtra.Rank > blockExtra.Rank {
	//		status = CanonStatTy
	//	} else {
	//		status = SideStatTy
	//	}
	if status == CanonStatTy {
		sc.insertCommit(block)

	}

	//err := sc.insertCommit(block)
	//	//sc.insert(block)
	//if err!=nil {
	//	return NonStatTy,err
	//}
	//sc.updateHeightMap(sc.CurrentCommitExtra().NewBlockHeight.Uint64(), block.NumberU64())
	//}
	return status, nil
}

func (sc *CommitChain) insertCommit(block *types.Block) {
	// Add the block to the canonical chain number scheme and mark as the head
	rawdb.WriteCanonicalCommitHash(sc.db, block.Hash(), block.NumberU64())
	rawdb.WriteHeadCommitBlockHash(sc.db, block.Hash())
	rawdb.WriteHeadCommitHeaderHash(sc.db, block.Hash())
}

func (sc *CommitChain) insert(block *types.Block) error {
	// Add the block to the canonical chain number scheme and mark as the head
	rawdb.WriteCanonicalCommitHash(sc.db, block.Hash(), block.NumberU64())
	rawdb.WriteHeadCommitBlockHash(sc.db, block.Hash())
	rawdb.WriteHeadCommitHeaderHash(sc.db, block.Hash())

	err := sc.SetCurrentBlock(block)
	if err != nil {
		return err
	}
	return nil
}

//init section chain genesis block
func (sc *CommitChain) SetupGenesisCommitBlock(db ethdb.Database) (common.Hash, error) {
	stored := rawdb.ReadCanonicalCommitHash(db, 0)
	//first save to db
	if (stored == common.Hash{}) {
		commitExtra := utopia_types.CommitExtra{NewBlockHeight: new(big.Int).SetUint64(0)}
		bytes, _ := commitExtra.Encode()
		blockExtra := utopia_types.BlockExtra{Rank: 0, CNumber: new(big.Int).SetUint64(0), Extra: bytes}
		extra, _ := blockExtra.Encode()
		head := &types.Header{
			Number:     new(big.Int).SetUint64(0),
			Nonce:      types.EncodeNonce(16045690984833335000),
			Time:       new(big.Int).SetUint64(0),
			ParentHash: common.BytesToHash(hexutil.MustDecode("0x0000000000000000000000000000000000000000000000000000000000000000")),
			Extra:      extra,
			GasLimit:   5000000,
			Difficulty: new(big.Int).SetUint64(1024),
			MixDigest:  common.BytesToHash(hexutil.MustDecode("0x0000000000000000000000000000000000000000000000000000000000000000")),
			Root:       crypto.Keccak256Hash(nil),
		}

		block := types.NewBlock(head, nil, nil, nil)

		rawdb.WriteBlock(db, block)
		rawdb.WriteCanonicalCommitHash(db, block.Hash(), block.NumberU64())
		rawdb.WriteHeadCommitBlockHash(db, block.Hash())
		rawdb.WriteHeadCommitHeaderHash(db, block.Hash())

		return block.Hash(), nil
	}
	//genesis block has existed
	return stored, nil
}

func DeleteCommitBlock(db rawdb.DatabaseDeleter, hash common.Hash, number uint64, commitChain CommitChain) {

	commitChain.DeleteCacheBlock(hash)

	rawdb.DeleteHeader(db, hash, number)
	log.Info("DeleteHeader当前commit高度删除完毕", "高度", number, "hash", hash)

	rawdb.DeleteBody(db, hash, number)
	log.Info("DeleteBody当前commit高度删除完毕", "高度", number, "hash", hash)

	rawdb.DeleteCanonicalCommitHash(db, number)
	log.Info("DeleteCanonicalCommitHash当前commit高度删除完毕", "高度", number, "hash", hash)

	rawdb.DeleteHeadCommitBlockHash(db)
	log.Info("DeleteHeadCommitBlockHash当前commit高度删除完毕", "高度", number, "hash", hash)

	rawdb.DeleteHeadCommitHeaderHash(db)
	log.Info("DeleteHeadCommitHeaderHash当前commit高度删除完毕", "高度", number, "hash", hash)

	rawdb.DeleteBlock(db, hash, number)

}
