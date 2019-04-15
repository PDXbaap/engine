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
package pdxcc

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"strings"
	"sync"
	"time"

	pb "pdx-chain/pdxcc/protos"

	"pdx-chain/pdxcc/util"

	"google.golang.org/grpc"
	"pdx-chain/log"
)

// this is basically the singleton that supports the
// entire chaincode framework. It does NOT know about
// chains. Chains are per-proposal entities that are
// setup as part of "join" and go through this object
// via calls to Execute and Deploy chaincodes.
var theChaincodeSupport *ChaincodeSupport

func registerChaincodeSupportServer(grpcServer *grpc.Server) {

	theChaincodeSupport = &ChaincodeSupport{
		runningChaincodes: &runningChaincodes{
			chaincodeMap:      make(map[string]*chaincodeRTEnv),
			launchStarted:     make(map[string]bool),
			chaincodeEndpoint: make(map[string]string),
		},
		keepalive: time.Second * 120,
	}

	pb.RegisterChaincodeSupportServer(grpcServer, theChaincodeSupport)
}

// chaincode runtime environment encapsulates handler and container environment
// This is where the VM that's running the chaincode would hook in
type chaincodeRTEnv struct {
	handler *Handler
}

// runningChaincodes contains maps of chaincodeIDs to their chaincodeRTEs
type runningChaincodes struct {
	// chaincode environment for each chaincode
	chaincodeMap      map[string]*chaincodeRTEnv
	chaincodeEndpoint map[string]string // 多个版本的chaincode，只有最新被选择的版本可以被调用

	// mark the starting of launch of a chaincode so multiple requests
	// do not attempt to start the chaincode at the same time
	launchStarted map[string]bool
}

// ChaincodeSupport responsible for providing interfacing with chaincodes from the Peer.
type ChaincodeSupport struct {
	sync.RWMutex
	runningChaincodes *runningChaincodes
	peerAddress       string
	keepalive         time.Duration
	executetimeout    time.Duration
}

// call this under lock
func (chaincodeSupport *ChaincodeSupport) chaincodeHasBeenLaunched(chaincode string) (*chaincodeRTEnv, bool) {
	chaincodeSupport.RLock()
	defer chaincodeSupport.RUnlock()

	chrte, hasbeenlaunched := chaincodeSupport.runningChaincodes.chaincodeMap[chaincode]
	return chrte, hasbeenlaunched
}

func (chaincodeSupport *ChaincodeSupport) chaincodeHasRan(chaincode string) (string, bool) {
	chaincodeSupport.RLock()
	defer chaincodeSupport.RUnlock()

	cc, hasRan := chaincodeSupport.runningChaincodes.chaincodeEndpoint[chaincode]
	return cc, hasRan
}

func (chaincodeSupport *ChaincodeSupport) DelCC(ccAddr string) {
	chaincodeSupport.Lock()
	defer chaincodeSupport.Unlock()

	delete(chaincodeSupport.runningChaincodes.chaincodeMap, ccAddr)
	delete(chaincodeSupport.runningChaincodes.chaincodeEndpoint, ccAddr)
	log.Info("del success")
}

// Register the bidi stream entry point called by chaincode to register with the Peer.
func (chaincodeSupport *ChaincodeSupport) Register(stream pb.ChaincodeSupport_RegisterServer) error {
	log.Info("Register..............")
	return chaincodeSupport.HandleChaincodeStream(stream.Context(), stream)
}

func (chaincodeSupport *ChaincodeSupport) deregisterHandler(chaincodehandler *Handler) error {
	key := chaincodehandler.ChaincodeID.Name //owner:name:version
	log.Info("deregister", "key", key, "ChaincodeID", *chaincodehandler.ChaincodeID, "ccAddr", util.EthAddress(key).Hex())

	ccAddr := strings.ToLower(util.EthAddress(key).Hex())
	chaincodeSupport.DelCC(ccAddr)
	return nil
}

func (chaincodeSupport *ChaincodeSupport) registerHandler(chaincodehandler *Handler) error {
	key := chaincodehandler.ChaincodeID.Name //owner:name:version
	log.Info("register", "key", key, "ChaincodeID", *chaincodehandler.ChaincodeID)

	chaincodeSupport.Lock()
	defer chaincodeSupport.Unlock()

	chrte2, ok := chaincodeSupport.runningChaincodes.chaincodeMap[key]
	if ok && chrte2.handler.registered == true {
		log.Debug(fmt.Sprintf("duplicate registered handler(key:%s) return error", key))
		// Duplicate, return error
		return newDuplicateChaincodeHandlerError(chaincodehandler)
	}
	if chrte2 != nil {
		chaincodehandler.readyNotify = chrte2.handler.readyNotify
		chrte2.handler = chaincodehandler
	} else {
		ccAddr := strings.ToLower(util.EthAddress(key).Hex())
		log.Info("!!!!register handler", "key", key, "ccAddr", ccAddr)
		chaincodeSupport.runningChaincodes.chaincodeMap[ccAddr] = &chaincodeRTEnv{handler: chaincodehandler}
		chaincodeSupport.runningChaincodes.chaincodeEndpoint[ccAddr] = key
	}

	chaincodehandler.registered = true

	// now we are ready to receive messages and send back responses
	chaincodehandler.txCtxs = make(map[string]*transactionContext)
	chaincodehandler.txidMap = make(map[string]bool)

	log.Debug(fmt.Sprintf("registered handler complete for chaincode %s %p", key, chaincodehandler))

	return nil
}

// HandleChaincodeStream Main loop for handling the associated Chaincode stream
func (chaincodeSupport *ChaincodeSupport) HandleChaincodeStream(ctxt context.Context, stream ChaincodeStream) error {
	return HandleChaincodeStream(chaincodeSupport, ctxt, stream)
}

// HandleChaincodeStream Main loop for handling the associated Chaincode stream
func HandleChaincodeStream(chaincodeSupport *ChaincodeSupport, ctxt context.Context, stream ChaincodeStream) error {
	deadline, ok := ctxt.Deadline()
	log.Debug(fmt.Sprintf("Current context deadline = %s, ok = %v", deadline, ok))
	handler := newChaincodeSupportHandler(chaincodeSupport, stream)
	log.Debug(fmt.Sprintf("New Handler %p", handler))
	return handler.processStream()
}

// Execute executes a ` and waits for it to complete until a timeout value.
func (chaincodeSupport *ChaincodeSupport) Execute(ctxt context.Context, cccid *CCContext,
	msg *pb.ChaincodeMessage, timeout time.Duration) (*pb.ChaincodeMessage, error) {
	log.Debug("Cc Execute Entry")
	defer log.Debug("Cc Execute Exit")
	canName := cccid.GetCanonicalName()
	// we expect the chaincode to be running... sanity check
	chrte, ok := chaincodeSupport.chaincodeHasBeenLaunched(canName)
	if !ok {
		log.Debug(fmt.Sprintf("cannot execute-chaincode is not running: %s", canName))
		return nil, fmt.Errorf("cannot execute transaction for %s", canName)
	}

	var notfy chan *pb.ChaincodeMessage
	var err error
	if notfy, err = chrte.handler.sendExecuteMessage(ctxt, msg, cccid); err != nil {
		return nil, fmt.Errorf("error sending %s: %s", msg.Type.String(), err)
	}
	// 这里阻塞等待结果通知、或者超时，届时将删除txid context
	var ccresp *pb.ChaincodeMessage
	select {
	case ccresp = <-notfy:
		// response is sent to user or calling chaincode. ChaincodeMessage_ERROR
		// are typically treated as error
		log.Info("ccresp notify....")
		if ccresp.Type == pb.ChaincodeMessage_ERROR {
			err = fmt.Errorf("ccresp error")
		}
	case <-time.After(timeout):
		log.Warn("Timeout expired while executing transaction")
		err = fmt.Errorf("timeout expired while executing transaction")
	}

	// our responsibility to delete transaction context if sendExecuteMessage succeeded
	chrte.handler.deleteTxContext(msg.ChannelId, msg.Txid)

	return ccresp, err
}

// NewCCContext just construct a new struct with whatever args
// func NewCCContext(cid, name, version, txid string, syscc bool, signedProp *pb.SignedProposal, prop *pb.Proposal,
// 	pdxData *PDXDataSupport, readOnly bool) *CCContext {
func NewCCContext(cid, key, txid string, syscc bool, signedProp *pb.SignedProposal, prop *pb.Proposal,
	pdxData *PDXDataSupport) (*CCContext, error) {
	// version CANNOT be empty. The chaincode namespace has to use version and chain name.
	// All system chaincodes share the same version given by utils.GetSysCCVersion. Note
	// that neither Chain Name or Version are stored in a chaincodes state on the ledger
	contents := strings.Split(key, ":") //owner:name:version
	if len(contents) != 3 {
		log.Error("key illegal", "key", key)
		return nil, errors.New("key illegal")
	}
	owner := contents[0]
	name := contents[1]
	version := contents[2]

	if version == "" {
		panic(fmt.Sprintf("---empty version---(chain=%s,chaincode=%s,version=%s,txid=%s,syscc=%t,proposal=%p", cid, name, version, txid, syscc, prop))
		log.Error("---empty version---", "chain", cid, "owner", owner, "chaincide", name, "version", version, "txid", txid, "syscc", syscc, "proposal", prop)
	}

	canName := strings.ToLower(util.EthAddress(key).Hex())

	cccid := &CCContext{PdxData: pdxData, ChainID: cid, Name: name, Version: version, TxID: txid, Syscc: syscc, SignedProposal: signedProp, Proposal: prop, canonicalName: canName}

	return cccid, nil
}

// GetCanonicalName returns the canonical name associated with the proposal context
func (cccid *CCContext) GetCanonicalName() string {
	if cccid.canonicalName == "" {
		panic(fmt.Sprintf("cccid not constructed using NewCCContext(chain=%s,chaincode=%s,version=%s,txid=%s,syscc=%t)",
			cccid.ChainID, cccid.Name, cccid.Version, cccid.TxID, cccid.Syscc))
	}

	return cccid.canonicalName
}

type DuplicateChaincodeHandlerError struct {
	ChaincodeID *pb.ChaincodeID
}

func (d *DuplicateChaincodeHandlerError) Error() string {
	return fmt.Sprintf("Duplicate chaincodeID error: %s", d.ChaincodeID)
}

func newDuplicateChaincodeHandlerError(chaincodeHandler *Handler) error {
	return &DuplicateChaincodeHandlerError{ChaincodeID: chaincodeHandler.ChaincodeID}
}
