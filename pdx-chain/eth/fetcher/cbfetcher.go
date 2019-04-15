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

// Package fetcher contains the block announcement based synchronisation.
package fetcher

import (
	"math/rand"
	types2 "pdx-chain/utopia/types"
	"time"

	"gopkg.in/karalabe/cookiejar.v2/collections/prque"
	"pdx-chain/common"
	"pdx-chain/core/types"
	"pdx-chain/log"
)

//PDX
type RequestCommitsByNumberFn func(origin common.Hash, amount int, skip int, reverse bool) error

type CbAnnounce struct {
	hash   common.Hash   // Hash of the block being announced
	number uint64        // Number of the block being announced (0 = unknown | old protocol)
	header *types.Header // Header of the block partially reassembled (new protocol)
	time   time.Time     // Timestamp of the announcement

	origin string // Identifier of the peer originating the notification

	fetchHeader RequestCommitsByNumberFn // Fetcher function to retrieve the header of an announced block
}

// Fetcher is responsible for accumulating block announcements from various peers
// and scheduling them for retrieval.
type CbFetcher struct {
	// Various event channels
	notify chan *CbAnnounce
	inject chan *inject

	blockFilter  chan chan []*types.Block
	headerFilter chan chan *headerFilterTask
	bodyFilter   chan chan *bodyFilterTask
	done         chan common.Hash
	quit         chan struct{}

	Synced   chan struct{}
	InsertCh chan struct{}

	// Announce states
	announces  map[string]int                // Per peer announce counts to prevent memory exhaustion
	announced  map[common.Hash][]*CbAnnounce // Announced blocks, scheduled for fetching
	fetching   map[common.Hash]*CbAnnounce   // Announced blocks, currently fetching
	fetched    map[common.Hash][]*CbAnnounce // Blocks with headers fetched, scheduled for body retrieval
	completing map[common.Hash]*CbAnnounce   // Blocks with headers, currently body-completing

	// Block cache
	queue  *prque.Prque            // Queue containing the import operations (block number sorted)
	queues map[string]int          // Per peer block counts to prevent memory exhaustion
	queued map[common.Hash]*inject // Set of already queued blocks (to dedupe imports)

	// Callbacks
	getBlock        blockRetrievalFn // Retrieves a block from the local chain
	verifyHeader    headerVerifierFn // Checks if a block's headers have a valid proof of work
	chainHeight     chainHeightFn    // Retrieves the current chain's height
	normalNewHeight chainHeightFn
	insertChain     chainInsertFn // Injects a batch of blocks into the chain
	dropPeer        peerDropFn    // Drops a peer for misbehaving
}

// New creates a block fetcher to retrieve blocks based on hash announcements.
func NewCBFetcher(getBlock blockRetrievalFn, verifyHeader headerVerifierFn, chainHeight chainHeightFn, insertChain chainInsertFn, dropPeer peerDropFn, normalNewHeight chainHeightFn) *CbFetcher {

	return &CbFetcher{
		notify:          make(chan *CbAnnounce),
		inject:          make(chan *inject),
		blockFilter:     make(chan chan []*types.Block),
		headerFilter:    make(chan chan *headerFilterTask),
		bodyFilter:      make(chan chan *bodyFilterTask),
		done:            make(chan common.Hash),
		quit:            make(chan struct{}),
		Synced:          make(chan struct{}, 1),
		InsertCh:        make(chan struct{}, 1),
		announces:       make(map[string]int),
		announced:       make(map[common.Hash][]*CbAnnounce),
		fetching:        make(map[common.Hash]*CbAnnounce),
		fetched:         make(map[common.Hash][]*CbAnnounce),
		completing:      make(map[common.Hash]*CbAnnounce),
		queue:           prque.New(),
		queues:          make(map[string]int),
		queued:          make(map[common.Hash]*inject),
		getBlock:        getBlock,
		verifyHeader:    verifyHeader,
		chainHeight:     chainHeight,
		normalNewHeight: normalNewHeight,
		insertChain:     insertChain,
		dropPeer:        dropPeer,
	}
}

// Start boots up the announcement based synchroniser, accepting and processing
// hash notifications and block fetches until termination requested.
func (f *CbFetcher) Start() {
	go f.loop()
}

// Stop terminates the announcement based synchroniser, canceling all pending
// operations.
func (f *CbFetcher) Stop() {
	close(f.quit)
}

//PDX
func (f *CbFetcher) Notify(peer string, hash common.Hash, number uint64, time time.Time,
	headerFetcher RequestCommitsByNumberFn) error {
	block := &CbAnnounce{
		hash:        hash,
		number:      number,
		time:        time,
		origin:      peer,
		fetchHeader: headerFetcher,
	}
	select {
	case f.notify <- block:
		return nil
	case <-f.quit:
		return errTerminated
	}
}

// Enqueue tries to fill gaps the the fetcher's future import queue.
func (f *CbFetcher) Enqueue(peer string, blocks []*types.Block) error {
	for _, block := range blocks {
		op := &inject{
			origin: peer,
			block:  block,
		}
		select {
		case f.inject <- op:
			break
		case <-f.quit:
			return errTerminated
		}
	}
	return nil
}

// Loop is the main fetcher loop, checking and processing various notification
// events.
func (f *CbFetcher) loop() {
	// Iterate the block fetching until a quit is requested
	fetchTimer := time.NewTimer(0)

	for {
		// Clean up any expired block fetches
		for hash, announce := range f.fetching {
			if time.Since(announce.time) > fetchTimeout {
				f.forgetHash(hash)
			}
		}
		// Import any queued blocks that could potentially fit

		for !f.queue.Empty() {
			op := f.queue.PopItem().(*inject)
			log.Info("cbFetcher队列不为空")
			// If too high up the chain or phase, continue later
			number := op.block.NumberU64()
			height := f.chainHeight() //currentCommitBlock
			_,commitExtra := types2.CommitExtraDecode(op.block)
			newBlockHeight := commitExtra.NewBlockHeight.Uint64()
			normalCurrentNum := f.normalNewHeight() //currentNormal
			if number > height+1 || (newBlockHeight > normalCurrentNum && number >= 1) {
				log.Info("回去了")
				blockExtra := types2.BlockExtraDecode(op.block)
				f.queue.Push(op, -float32(number)+(-float32(blockExtra.Rank/100)))
				break
			}
			log.Info("------cbfetcher--------", "num", number, "currentHeight", height)
			f.insert(op.origin, op.block)
		}
		// Wait for an outside event to occur
		select {
		case <-f.quit:
			// Fetcher terminating, abort all operations
			return

		case op := <-f.inject:
			log.Debug("CBFetcher inject start","commitHeight",op.block.NumberU64())

			// A direct block insertion was requested, try and fill any pending gaps
			propBroadcastInMeter.Mark(1)
			f.enqueue(op.origin, op.block)

		case notification := <-f.notify:
			log.Debug("CBFetcher notify start","notification到commitNum",notification.number,"hash",notification.hash)

			count := f.announces[notification.origin] + 1

			// All is well, schedule the announce if block's not yet downloading
			if _, ok := f.fetching[notification.hash]; ok {
				break
			}
			if _, ok := f.completing[notification.hash]; ok {
				break
			}
			f.announces[notification.origin] = count
			f.announced[notification.hash] = append(f.announced[notification.hash], notification)
			//if len(f.announced) == 1 {
			//	f.rescheduleFetch(fetchTimer)
			//}

			// At least one block's timer ran out, check for needing retrieval
			request := make(map[string][]*CbAnnounce)
			for hash, announces := range f.announced {
				//if time.Since(announces[0].time) > arriveTimeout-gatherSlack {
				// Pick a random peer to retrieve from, reset all others
				announce := announces[rand.Intn(len(announces))]
				// If the block still didn't arrive, queue for fetching
				if f.getBlock(hash) == nil {
					request[announce.origin] = append(request[announce.origin], announce)
					f.fetching[hash] = announce
				}
				//}
			}
			// Send out all block header requests
			for peer, announce := range request {
				log.Trace("Fetching scheduled CommitBlock", "peer", peer, "list", announce)

				// Create a closure of the fetch and schedule in on a new thread
				fetchHeader, announce := f.fetching[announce[0].hash].fetchHeader, announce
				go func() {
					for _, announce := range announce {
						//type RequestCommitsByNumberFn func(origin uint64, amount int, skip int, reverse bool) error
						log.Info("fetchHeader开始广播", "高度", announce.number)
						fetchHeader(announce.hash, 1, 0, false) //只要一个
					}
				}()
			}
			// Schedule the next fetch if blocks are still pending
			f.rescheduleFetch(fetchTimer)
			log.Debug("fetcher notify end")



		case hash := <-f.done:
			// A pending import finished, remove all traces of the notification
			log.Info("cbFetcher的done")
			f.forgetHash(hash)
			f.forgetBlock(hash)


		case <-f.Synced:

		case <-f.InsertCh:

		}
	}
}

// rescheduleFetch resets the specified fetch timer to the next announce timeout.
func (f *CbFetcher) rescheduleFetch(fetch *time.Timer) {
	// Short circuit if no blocks are announced
	if len(f.announced) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, announces := range f.announced {
		if earliest.After(announces[0].time) {
			earliest = announces[0].time
		}
	}
	fetch.Reset(arriveTimeout - time.Since(earliest))
}

// rescheduleComplete resets the specified completion timer to the next fetch timeout.
func (f *CbFetcher) rescheduleComplete(complete *time.Timer) {
	// Short circuit if no headers are fetched
	if len(f.fetched) == 0 {
		return
	}
	// Otherwise find the earliest expiring announcement
	earliest := time.Now()
	for _, announces := range f.fetched {
		if earliest.After(announces[0].time) {
			earliest = announces[0].time
		}
	}
	complete.Reset(gatherSlack - time.Since(earliest))
}

// enqueue schedules a new future import operation, if the block to be imported
// has not yet been seen.
func (f *CbFetcher) enqueue(peer string, block *types.Block) {
	hash := block.Hash()

	// Ensure the peer isn't DOSing us
	count := f.queues[peer] + 1
	//TODO what's the right size for block limit and max queuedist
	if count > blockLimit*10 {
		log.Debug("Discarded propagated block, exceeded allowance", "peer", peer, "number", block.Number(), "hash", hash, "limit", blockLimit*10)
		propBroadcastDOSMeter.Mark(1)
		f.forgetHash(hash)
		return
	}
	// Discard any past or too distant blocks
	//if dist := int64(block.NumberU64()) - int64(f.chainHeight()); dist < -maxUncleDist || dist > maxQueueDist*10 {
	//	log.Debug("Discarded propagated block, too far away", "peer", peer, "number", block.Number(), "hash", hash, "distance", dist)
	//	propBroadcastDropMeter.Mark(1)
	//	f.forgetHash(hash)
	//	return
	//}
	// Schedule the block for future importing
	if _, ok := f.queued[hash]; !ok {
		op := &inject{
			origin: peer,
			block:  block,
		}
		f.queues[peer] = count
		f.queued[hash] = op
		blockExtra := types2.BlockExtraDecode(block)
		f.queue.Push(op, -float32(block.NumberU64())+(-float32(blockExtra.Rank/100)))

		log.Debug("Queued propagated  commitBlock", "peer", peer, "number", block.Number(), "hash", hash, "queued", f.queue.Size())
	}
}

// insert spawns a new goroutine to run a block insertion into the chain. If the
// block's number is at the same height as the current import phase, it updates
// the phase states accordingly.
func (f *CbFetcher) insert(peer string, block *types.Block) {
	hash := block.Hash()
	// Run the import on a new thread
	log.Debug("Importing propagated block", "peer", peer, "number", block.Number(), "hash", hash)
	go func() {
		defer func() { f.done <- hash }()

		// Quickly validate the header and propagate the block if it passes
		switch err := f.verifyHeader(block.Header()); err {
		case nil:
			// All ok, quickly propagate to our peers
			propBroadcastOutTimer.UpdateSince(block.ReceivedAt)

		default:
			// Something went very wrong, drop the peer
			log.Debug("Propagated block verification failed", "peer", peer, "number", block.Number(), "hash", hash, "err", err)
			f.dropPeer(peer)
			return
		}
		// Run the actual import and log any issues
		if _, err := f.insertChain(types.Blocks{block}); err != nil {
			log.Debug("Propagated block import failed", "peer", peer, "number", block.Number(), "hash", hash, "err", err)
			return
		}

		// If import succeeded, broadcast the block
		propAnnounceOutTimer.UpdateSince(block.ReceivedAt)

	}()
}

// forgetHash removes all traces of a block announcement from the fetcher's
// internal state.
func (f *CbFetcher) forgetHash(hash common.Hash) {
	// Remove all pending announces and decrement DOS counters
	for _, announce := range f.announced[hash] {
		f.announces[announce.origin]--
		if f.announces[announce.origin] == 0 {
			delete(f.announces, announce.origin)
		}
	}
	delete(f.announced, hash)

	if announce := f.fetching[hash]; announce != nil {
		f.announces[announce.origin]--
		if f.announces[announce.origin] == 0 {
			delete(f.announces, announce.origin)
		}
		delete(f.fetching, hash)
	}

	// Remove any pending completion requests and decrement the DOS counters
	for _, announce := range f.fetched[hash] {
		f.announces[announce.origin]--
		if f.announces[announce.origin] == 0 {
			delete(f.announces, announce.origin)
		}
	}
	delete(f.fetched, hash)

	// Remove any pending completions and decrement the DOS counters
	if announce := f.completing[hash]; announce != nil {
		f.announces[announce.origin]--
		if f.announces[announce.origin] == 0 {
			delete(f.announces, announce.origin)
		}
		delete(f.completing, hash)
	}
}

// forgetBlock removes all traces of a queued block from the fetcher's internal
// state.
func (f *CbFetcher) forgetBlock(hash common.Hash) {
	if insert := f.queued[hash]; insert != nil {
		f.queues[insert.origin]--
		if f.queues[insert.origin] == 0 {
			delete(f.queues, insert.origin)
		}
		delete(f.queued, hash)
	}
}

func (f *CbFetcher) fetchBlock(hash common.Hash) {
	//TODO
}
