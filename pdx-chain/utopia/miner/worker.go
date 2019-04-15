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
package miner

import (
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deckarep/golang-set"
	"pdx-chain/common"
	"pdx-chain/consensus"
	"pdx-chain/core"
	"pdx-chain/core/state"
	"pdx-chain/core/types"
	"pdx-chain/core/vm"
	"pdx-chain/event"
	"pdx-chain/log"
	"pdx-chain/params"
	"pdx-chain/rlp"
	utopia_engine "pdx-chain/utopia/engine"
	utopia_types "pdx-chain/utopia/types"
)

var utopiaEngine *utopia_engine.Utopia


const (
	// resultQueueSize is the size of channel listening to sealing result.
	resultQueueSize = 10

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096

	// chainHeadChanSize is the size of channel listening to ChainHeadEvent.
	chainHeadChanSize = 10

	// chainSideChanSize is the size of channel listening to ChainSideEvent.
	chainSideChanSize = 10

	// resubmitAdjustChanSize is the size of resubmitting interval adjustment channel.
	resubmitAdjustChanSize = 10

	// miningLogAtDepth is the number of confirmations before logging successful mining.
	miningLogAtDepth = 5

	// minRecommitInterval is the minimal time interval to recreate the mining block with
	// any newly arrived transactions.
	minRecommitInterval = 1 * time.Second

	// maxRecommitInterval is the maximum time interval to recreate the mining block with
	// any newly arrived transactions.
	maxRecommitInterval = 15 * time.Second

	// intervalAdjustRatio is the impact a single interval adjustment has on sealing work
	// resubmitting interval.
	intervalAdjustRatio = 0.1

	// intervalAdjustBias is applied during the new resubmit interval calculation in favor of
	// increasing upper limit or decreasing lower limit so that the limit can be reachable.
	intervalAdjustBias = 200 * 1000.0 * 1000.0
)

// environment is the worker's current environment and holds all of the current state information.
type environment struct {
	signer types.Signer

	state     *state.StateDB // apply state changes here
	ancestors mapset.Set     // ancestor set (used for checking uncle parent validity)
	family    mapset.Set     // family set (used for checking uncle invalidity)
	uncles    mapset.Set     // uncle set
	tcount    int            // tx count in cycle
	gasPool   *core.GasPool  // available gas used to pack transactions

	header   *types.Header
	txs      []*types.Transaction
	receipts []*types.Receipt
}

// task contains all information for consensus engine sealing and result submitting.
type task struct {
	receipts   []*types.Receipt
	state      *state.StateDB
	block      *types.Block
	createdAt  time.Time
	BlockExtra *utopia_types.BlockExtra
	timeStart  time.Time
}

const (
	commitInterruptNone int32 = iota
	commitInterruptNewHead
	commitInterruptResubmit
)

// newWorkReq represents a request for new sealing work submitting with relative interrupt notifier.
type newWorkReq struct {
	interrupt *int32
	noempty   bool
}

// intervalAdjust represents a resubmitting interval adjustment.
type intervalAdjust struct {
	ratio float64
	inc   bool
}

// utopiaWorker is the main object which takes care of submitting new work to consensus engine
// and gathering the sealing result.
type utopiaWorker struct {
	config *params.ChainConfig
	engine consensus.Engine
	eth    Backend
	chain  *core.BlockChain

	// Subscriptions
	mux          *event.TypeMux
	txsCh        chan core.NewTxsEvent
	txsSub       event.Subscription
	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription

	// Channels
	newWorkCh chan *utopia_types.BlockMiningReq
	taskCh    chan *task
	resultCh  chan *task
	startCh   chan struct{}
	exitCh    chan struct{}
	//cancelCh           chan struct{}
	resubmitIntervalCh chan time.Duration
	resubmitAdjustCh   chan *intervalAdjust

	current        *environment                 // An environment for current running cycle.
	possibleUncles map[common.Hash]*types.Block // A set of side blocks as the possible uncle blocks.
	unconfirmed    *unconfirmedBlocks           // A set of locally mined blocks pending canonicalness confirmations.

	mu       sync.RWMutex // The lock used to protect the coinbase and extra fields
	coinbase common.Address
	extra    []byte

	snapshotMu    sync.RWMutex // The lock used to protect the block snapshot and state snapshot
	snapshotBlock *types.Block
	snapshotState *state.StateDB

	// atomic status counters
	running int32 // The indicator whether the consensus engine is running or not.
	newTxs  int32 // New arrival transaction count since last sealing work submitting.
	// Test hooks
	newTaskHook  func(*task)                        // Method to call upon receiving a new sealing task.
	skipSealHook func(*task) bool                   // Method to decide whether skipping the sealing.
	fullTaskHook func()                             // Method to call before pushing the full sealing task.
	resubmitHook func(time.Duration, time.Duration) // Method to call upon updating resubmitting interval.
}

func newUtopiaWorker(config *params.ChainConfig, engine consensus.Engine, eth Backend, mux *event.TypeMux, recommit time.Duration) *utopiaWorker {
	utopiaWorker := &utopiaWorker{
		config:         config,
		engine:         engine,
		eth:            eth,
		mux:            mux,
		chain:          eth.BlockChain(),
		possibleUncles: make(map[common.Hash]*types.Block),
		unconfirmed:    newUnconfirmedBlocks(eth.BlockChain(), miningLogAtDepth),
		txsCh:          make(chan core.NewTxsEvent, txChanSize),
		chainHeadCh:    make(chan core.ChainHeadEvent, chainHeadChanSize),
		newWorkCh:      make(chan *utopia_types.BlockMiningReq, 1),
		taskCh:         make(chan *task),
		resultCh:       make(chan *task, resultQueueSize),
		exitCh:         make(chan struct{}),
		startCh:        make(chan struct{}, 1),
		//cancelCh:           make(chan struct{}, 1),
		resubmitIntervalCh: make(chan time.Duration),
		resubmitAdjustCh:   make(chan *intervalAdjust, resubmitAdjustChanSize),
	}

	var ok bool
	utopiaEngine, ok = engine.(*utopia_engine.Utopia)

	if !ok {
		panic("miner/worker & engine mismatch")
	}
	utopiaEngine.SetMinerChannel(utopiaWorker.newWorkCh)
	utopiaEngine.SetEventMux(mux)

	// Subscribe NewTxsEvent for tx pool
	//utopiaWorker.txsSub = eth.TxPool().SubscribeNewTxsEvent(utopiaWorker.txsCh)
	// Subscribe events for blockchain
	//utopiaWorker.chainHeadSub = eth.BlockChain().SubscribeChainHeadEvent(utopiaWorker.chainHeadCh)

	go utopiaWorker.mainLoop()
	go utopiaWorker.resultLoop()
	go utopiaWorker.taskLoop()
	// Submit first work to initialize pending state.
	//<-utopiaWorker.startCh


	return utopiaWorker
}

// setEtherbase sets the etherbase used to initialize the block coinbase field.
func (w *utopiaWorker) setEtherbase(addr common.Address) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.coinbase = addr
}

// setExtra sets the content used to initialize the block extra field.
func (w *utopiaWorker) setExtra(extra []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.extra = extra
}

// setRecommitInterval updates the interval for miner sealing work recommitting.
func (w *utopiaWorker) setRecommitInterval(interval time.Duration) {
	w.resubmitIntervalCh <- interval
}

// pending returns the pending state and corresponding block.
func (w *utopiaWorker) pending() (*types.Block, *state.StateDB) {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	if w.snapshotState == nil {
		return nil, nil
	}
	return w.snapshotBlock, w.snapshotState.Copy()
}

// pendingBlock returns pending block.
func (w *utopiaWorker) pendingBlock() *types.Block {
	// return a snapshot to avoid contention on currentMu mutex
	w.snapshotMu.RLock()
	defer w.snapshotMu.RUnlock()
	return w.snapshotBlock
}

// start sets the running status as 1 and triggers new work submitting.
func (w *utopiaWorker) start() {
	atomic.StoreInt32(&w.running, 1)
	log.Info("start miner")
	w.startCh <- struct{}{}
}

// stop sets the running status as 0.
func (w *utopiaWorker) stop() {
	atomic.StoreInt32(&w.running, 0)
}

// isRunning returns an indicator whether utopiaWorker is running or not.
func (w *utopiaWorker) isRunning() bool {
	return atomic.LoadInt32(&w.running) == 1
}

// close terminates all background threads maintained by the utopiaWorker and cleans up buffered channels.
// Note the utopiaWorker does not support being closed multiple times.
func (w *utopiaWorker) close() {
	close(w.exitCh)
	// Clean up buffered channels
	for empty := false; !empty; {
		select {
		case <-w.resultCh:
		default:
			empty = true
		}
	}
}

// mainLoop is a standalone goroutine to regenerate the sealing task based on the received event.
func (w *utopiaWorker) mainLoop() {
	//defer w.txsSub.Unsubscribe()
	//defer w.chainHeadSub.Unsubscribe()
	<-w.startCh
	log.Info("entering mainloop")

	for {
		select {
		case req := <-w.newWorkCh:
			timeMain:=time.Now()
			log.Info("received new work")
			w.commitNewWork(req,timeMain)

		case <-w.exitCh:
			log.Error("exit mainloop")
			return

		}
	}
}

// seal pushes a sealing task to consensus engine and submits the result.
func (w *utopiaWorker) seal(t *task, stop <-chan struct{}) {
	var (
		err error
		res *task
	)

	if w.skipSealHook != nil && w.skipSealHook(t) {
		return
	}

	if t.block, err = w.engine.Seal(w.chain, t.block, stop); t.block != nil {
		log.Info("Successfully sealed new block", "number", t.block.Number(), "hash", t.block.Hash(),
			"elapsed", common.PrettyDuration(time.Since(t.createdAt)))
		res = t
		if t.block.NumberU64() != w.chain.CurrentBlock().NumberU64()+1 {
			log.Debug("无效的块")
			return
		}
	} else {
		if err != nil {
			log.Warn("Block sealing failed", "err", err)
		}
		res = nil
	}
	res.timeStart=t.timeStart
	select {
	case w.resultCh <- res:
	case <-w.exitCh:
		log.Info("seal recv'd exit signal")
	}
}

// taskLoop is a standalone goroutine to fetch sealing task from the generator and
// push them to consensus engine.
func (w *utopiaWorker) taskLoop() {
	var (
		stopCh chan struct{}
		prev   common.Hash
	)

	// interrupt aborts the in-flight sealing task.
	interrupt := func() {
		if stopCh != nil {
			close(stopCh)
			stopCh = nil
		}
	}
	for {
		select {
		case task := <-w.taskCh:
			if w.newTaskHook != nil {
				w.newTaskHook(task)
			}

			// Reject duplicate sealing work due to resubmitting.
			if task.block.HashNoNonce() == prev {
				continue
			}
			interrupt()
			stopCh = make(chan struct{})
			prev = task.block.HashNoNonce()

			go w.seal(task, stopCh)
		case <-w.exitCh:
			interrupt()
			return
		}
	}
}

// resultLoop is a standalone goroutine to handle sealing result submitting
// and flush relative data to the database.
func (w *utopiaWorker) resultLoop() {
	for {

		select {
		case result := <-w.resultCh:
			log.Info("resultLoop----", "number", result.block.NumberU64(), "hash", result.block.Hash())
			timeStart := time.Since(result.timeStart).Seconds()*1000
			timeEnd := float64(time.Duration(utopia_engine.BlockDelay+300))
			log.Info("time","time", timeStart,"timeEnd",timeEnd)

			if timeStart > timeEnd {
				log.Error("打快超时", "高度", result.block.Number())
				//需要回滚交易
				break
			}

			if timeStart < timeEnd {
				log.Info("等待打快完成","时间",time.Duration(timeEnd - timeStart)*time.Millisecond)
				time.Sleep(time.Duration(timeEnd - timeStart)*time.Millisecond)
			}

			log.Info("OnNormalBlock----", "number", result.block.NumberU64(), "hash", result.block.Hash())

			if result.block.NumberU64() != w.chain.CurrentBlock().NumberU64()+1 {
				log.Debug("无效的块", "高度", result.block.NumberU64(), "hash", result.block.Hash())
			} else {
				blockExtra := utopia_types.BlockExtraDecode(result.block)
				utopiaEngine.Fetcher.Enqueue(fmt.Sprintf("%x", blockExtra.NodeID.Bytes()[:8]), result.block)
			}

		case <-w.exitCh:
			log.Info("exit resultloop")
			return
		}
		log.Info("another result loop")
	}
}

// makeCurrent creates a new environment for the current cycle.
func (w *utopiaWorker) makeCurrent(parent *types.Block, header *types.Header) error {
	state, err := w.chain.StateAt(parent.Root())
	if err != nil {
		return err
	}
	env := &environment{
		signer:    types.NewEIP155Signer(w.config.ChainID),
		state:     state,
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
		header:    header,
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range w.chain.GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}

	// Keep track of transactions which return errors so they can be removed
	env.tcount = 0
	w.current = env
	return nil
}

// updateSnapshot updates pending snapshot block and state.
// Note this function assumes the current variable is thread safe.
func (w *utopiaWorker) updateSnapshot() {
	w.snapshotMu.Lock()
	defer w.snapshotMu.Unlock()

	var uncles []*types.Header
	w.current.uncles.Each(func(item interface{}) bool {
		hash, ok := item.(common.Hash)
		if !ok {
			return false
		}
		uncle, exist := w.possibleUncles[hash]
		if !exist {
			return false
		}
		uncles = append(uncles, uncle.Header())
		return false
	})

	w.snapshotBlock = types.NewBlock(
		w.current.header,
		w.current.txs,
		uncles,
		w.current.receipts,
	)

	w.snapshotState = w.current.state.Copy()
}

func (w *utopiaWorker) commitTransaction(tx *types.Transaction, coinbase common.Address) ([]*types.Log, error) {
	//log.Info("commitTransaction......")
	snap := w.current.state.Snapshot()

	receipt, _, err := core.ApplyTransaction(w.config, w.chain, &coinbase, w.current.gasPool, w.current.state, w.current.header, tx, &w.current.header.GasUsed, vm.Config{})
	if err != nil {
		w.current.state.RevertToSnapshot(snap)
		return nil, err
	}
	w.current.txs = append(w.current.txs, tx)
	w.current.receipts = append(w.current.receipts, receipt)
	return receipt.Logs, nil
}

func (w *utopiaWorker) commitTransactions(txs *types.TransactionsByPriceAndNonce, coinbase common.Address,tstart time.Time) bool {
	// Short circuit if current is nil
	if w.current == nil {
		return true
	}

	if w.current.gasPool == nil {
		w.current.gasPool = new(core.GasPool).AddGas(w.current.header.GasLimit)

	}

	var coalescedLogs []*types.Log
	txsStartTime := time.Now()
	i:=0
	for {
		i++
		// In the following three cases, we will interrupt the execution of the transaction.
		// (1) new head block event arrival, the interrupt signal is 1
		// (2) worker start or restart, the interrupt signal is 1
		// (3) worker recreate the mining block with any newly arrived transactions, the interrupt signal is 2.
		// For the first two cases, the semi-finished work will be discarded.
		// For the third case, the semi-finished work will be submitted to the consensus engine.

		// If we don't have enough gas for any further transactions then we're done
		if w.current.gasPool.Gas() < params.TxGas {
			log.Error("Not enough gas for further transactions", "have", w.current.gasPool, "want", params.TxGas)
			break
		}
		// Retrieve the next transaction and abort if all done
		tx := txs.Peek()
		if tx == nil {
			break
		}
		// Error may be ignored here. The error has already been checked
		// during transaction acceptance is the transaction pool.
		//
		// We use the eip155 signer regardless of the current hf.
		from, _ := types.Sender(w.current.signer, tx)
		// Check whether the tx is replay protected. If we're not in the EIP155 hf
		// phase, start ignoring the sender until we do.
		if tx.Protected() && !w.config.IsEIP155(w.current.header.Number) {
			log.Warn("Ignoring reply protected transaction", "hash", tx.Hash(), "eip155", w.config.EIP155Block)

			txs.Pop()
			continue
		}
		// Start executing the transaction
		w.current.state.Prepare(tx.Hash(), common.Hash{}, w.current.tcount)

		txStartTime := time.Now()
		logs, err := w.commitTransaction(tx, coinbase)
		switch err {
		case core.ErrGasLimitReached:
			// Pop the current out-of-gas transaction without shifting in the next from the account
			log.Error("Gas limit exceeded for current block", "sender", from)
			txs.Pop()

		case core.ErrNonceTooLow:
			// New head notification data race between the transaction pool and miner, shift
			log.Error("Skipping transaction with low nonce", "sender", from, "nonce", tx.Nonce())
			txs.Shift()

		case core.ErrNonceTooHigh:
			// Reorg notification data race between the transaction pool and miner, skip account =
			log.Error("Skipping account with hight nonce", "sender", from, "nonce", tx.Nonce())
			txs.Pop()

		case nil:
			// Everything ok, collect the logs and shift in the next transaction from the same account
			log.Info("everything ok, shift the next transaction", "sender", from, "nonce", tx.Nonce())
			coalescedLogs = append(coalescedLogs, logs...)
			w.current.tcount++
			txs.Shift()

		default:
			// Strange error, discard the transaction and get the next in line (note, the
			// nonce-too-high clause will prevent us from executing in vain).
			log.Error("Transaction failed, account skipped", "hash", tx.Hash(), "err", err)
			txs.Shift()
		}
		log.Info("single tx elapsed", "collapse", time.Since(txStartTime))
		if utopia_engine.BlockDelay<=int32(time.Since(tstart).Seconds()*1000){
			log.Info("交易超时","交易时间",time.Since(tstart).Seconds()*1000)
			break

		}
	}
	log.Info("一个块执行了多少笔交易","数量",i)
	log.Info("all txs elapsed", "time", time.Since(txsStartTime))

	if !w.isRunning() && len(coalescedLogs) > 0 {
		// We don't push the pendingLogsEvent while we are mining. The reason is that
		// when we are mining, the worker will regenerate a mining block every 3 seconds.
		// In order to avoid pushing the repeated pendingLog, we disable the pending log pushing.

		// make a copy, the state caches the logs and these logs get "upgraded" from pending to mined
		// logs by filling in the block hash when the block was mined by the local miner. This can
		// cause a race condition if a log was "upgraded" before the PendingLogsEvent is processed.
		cpy := make([]*types.Log, len(coalescedLogs))
		for i, l := range coalescedLogs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		go w.mux.Post(core.PendingLogsEvent{Logs: cpy})
	}
	// Notify resubmit loop to decrease resubmitting interval if current interval is larger
	// than the user-specified one.

	return false
}

// commitNewWork generates several new sealing tasks based on the parent block.
func (w *utopiaWorker) commitNewWork(work *utopia_types.BlockMiningReq,timeMain time.Time) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	tstart := timeMain
	parent := w.chain.CurrentBlock()

	tstamp := tstart.Unix()
	var extraByte []byte
	var err error
	var extra utopia_types.BlockExtra
	if work != nil {
		////set header extra with work
		var extra utopia_types.BlockExtra

		switch work.Kind {

		case utopia_types.NORNAML_BLOCK:
			extra = utopia_types.BlockExtra{NodeID: work.NodeID, Rank: work.Rank, IP: *work.IP, Port: work.Port, CNumber: work.CNumber, IsLandID: work.IsLandID, Empty: work.Empty}

		default:
			log.Info("incorrect block type:", "type",work.Kind)
			return
		}

		extraByte, err = rlp.EncodeToBytes(extra)
		if err != nil {
			log.Info("extra encode error", "err", err.Error())
		}
	}

	num := parent.Number()
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent),
		//GasLimit:   0x745235280,
		Extra:      extraByte,
		Time:       big.NewInt(tstamp),
		Difficulty: big.NewInt(1024),
		//BlockExtra: types.BlockExtra{Rank:work.Rank},
	}
	log.Info("GasLimit的大小","GasLimit",header.GasLimit)
	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	if w.isRunning() {
		if w.coinbase == (common.Address{}) {
			log.Error("Refusing to mine without etherbase")
			return
		}
		header.Coinbase = w.coinbase
	}
	if err := w.engine.Prepare(w.chain, header); err != nil {
		log.Error("Failed to prepare header for mining", "err", err)
		return
	}
	// Could potentially happen if starting to mine in an odd state.
	err = w.makeCurrent(parent, header)
	if err != nil {
		log.Error("Failed to create mining context", "err", err)
		return
	}
	log.Info("work.Empty","work.Empty",work.Empty)
	if !work.Empty{ //不是master不执行交易
		log.Info("commitNewWork----1")
		txPool := w.eth.TxPool()
		// Fill the block with all available pending transactions.
		pending, err := txPool.Pending()
		if err != nil {
			log.Error("Failed to fetch pending transactions", "err", err)
			return
		}
		remoteTxs:=pending
		log.Info("commitNewWork----2")
		if len(remoteTxs) > 0 {
			txs := types.NewTransactionsByPriceAndNonce(w.current.signer, remoteTxs)
			if w.commitTransactions(txs, w.coinbase,tstart) {
				return
			}
		}
	}


	log.Info("commitNewWork----3")
	w.commit(w.fullTaskHook, true, tstart, &extra)
}

// commit runs any post-transaction state modifications, assembles the final block
// and commits new work if consensus engine is running.
func (w *utopiaWorker) commit(interval func(), update bool, start time.Time, blockExtra *utopia_types.BlockExtra) error {
	// Deep copy receipts here to avoid interaction between different tasks.
	receipts := make([]*types.Receipt, len(w.current.receipts))
	for i, l := range w.current.receipts {
		receipts[i] = new(types.Receipt)
		*receipts[i] = *l
	}
	log.Info("commitNewWork----4")
	s := w.current.state.Copy()
	block, err := w.engine.Finalize(w.chain, w.current.header, s, w.current.txs, nil, w.current.receipts)
	if err != nil {
		log.Error("Finalize错误","err",err)
		return err
	}

	if w.isRunning() {
		if interval != nil {
			interval()
		}
		log.Info("commitNewWork----5")
		select {
		case w.taskCh <- &task{receipts: receipts, state: s, block: block, createdAt: time.Now(), BlockExtra: blockExtra,timeStart:start}:
			w.unconfirmed.Shift(block.NumberU64() - 1)

			feesWei := new(big.Int)
			for i, tx := range block.Transactions() {
				feesWei.Add(feesWei, new(big.Int).Mul(new(big.Int).SetUint64(receipts[i].GasUsed), tx.GasPrice()))
			}
			feesEth := new(big.Float).Quo(new(big.Float).SetInt(feesWei), new(big.Float).SetInt(big.NewInt(params.Ether)))

			log.Info("Commit new mining work", "number", block.Number(), "txs", w.current.tcount,
				"gas", block.GasUsed(), "fees", feesEth, "elapsed", common.PrettyDuration(time.Since(start)))

		case <-w.exitCh:
			log.Info("Worker has exited")
		}
	}else {
		log.Error("miner启动失败")
	}
	if update {
		w.updateSnapshot()
	}
	return nil
}
