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
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/looplab/fsm"
	"golang.org/x/net/context"

	pb "pdx-chain/pdxcc/protos"

	"pdx-chain/pdxcc/util"

	"pdx-chain/pdxcc/conf"

	"pdx-chain/log"
)

const (
	createdstate     = "created"     // start state
	establishedstate = "established" // in: CREATED, rcv:  REGISTER, send: REGISTERED, INIT
	readystate       = "ready"       // in:ESTABLISHED,TRANSACTION, rcv:COMPLETED
	endstate         = "end"         // in:INIT,ESTABLISHED, rcv: error, terminate container
)

type transactionContext struct {
	// readyOnly bool
	pdxData *PDXDataSupport

	chainID          string
	signedProp       *pb.SignedProposal
	proposal         *pb.Proposal
	responseNotifier chan *pb.ChaincodeMessage

	// tracks open iterators used for range queries
	nextMap    map[string]string
	endMap     map[string]string
	historyMap map[string]string
	versionMap map[string]int
}

type nextStateInfo struct {
	msg      *pb.ChaincodeMessage
	sendToCC bool

	// the only time we need to send synchronously is
	// when launching the chaincode to take it to ready
	// state (look for the panic when sending serial)
	sendSync bool
}

// Handler responsible for management of Peer's side of chaincode stream
type Handler struct {
	sync.RWMutex
	// peer to shim grpc serializer. User only in serialSend
	serialLock  sync.Mutex
	ChatStream  ChaincodeStream
	FSM         *fsm.FSM
	ChaincodeID *pb.ChaincodeID
	ccInstance  *ChaincodeInstance

	chaincodeSupport *ChaincodeSupport
	registered       bool
	readyNotify      chan bool
	// Map of tx txid to either invoke tx. Each tx will be
	// added prior to execute and remove when done execute
	txCtxs map[string]*transactionContext

	txidMap map[string]bool

	// used to do Send after making sure the state transition is complete
	nextState chan *nextStateInfo

	// JZ policyChecker policy.PolicyChecker
}

// ChaincodeInstance is unique identifier of chaincode instance
type ChaincodeInstance struct {
	ChainID          string
	ChaincodeName    string
	ChaincodeVersion string
}

// ChaincodeStream interface for stream between Peer and chaincode instance.
type ChaincodeStream interface {
	Send(*pb.ChaincodeMessage) error
	Recv() (*pb.ChaincodeMessage, error)
}

// CCContext pass this around instead of string of args
type CCContext struct {
	// ReadOnly bool
	PdxData *PDXDataSupport

	// ChainID chain id
	ChainID string

	// Name chaincode name
	Name string

	// Version used to construct the chaincode image and register
	Version string

	// TxID is the transaction id for the proposal (if any)
	TxID string

	// Syscc is this a system chaincode
	// always be false
	Syscc bool

	// SignedProposal for this invoke (if any)
	// this is kept here for access control and in case we need to pass something
	// from this to the chaincode
	SignedProposal *pb.SignedProposal

	// Proposal for this invoke (if any)
	// this is kept here just in case we need to pass something
	// from this to the chaincode
	Proposal *pb.Proposal

	// this is not set but computed (note that this is not exported. use GetCanonicalName)
	canonicalName string
}

func (handler *Handler) processStream() error {
	defer handler.deregister()
	// msg box
	msgAvail := make(chan *pb.ChaincodeMessage)

	var nsInfo *nextStateInfo
	var in *pb.ChaincodeMessage
	var err error

	// recv is used to spin Recv routine after previous received msg
	// has been processed
	recv := true

	// catch send errors and bail now that sends aren't synchronous
	errc := make(chan error, 1)

	for {
		in, err, nsInfo = nil, nil, nil
		if recv {
			recv = false
			go func() {
				var msg *pb.ChaincodeMessage
				msg, err = handler.ChatStream.Recv()
				msgAvail <- msg
			}()
		}
		select {
		case sendErr := <-errc:
			if sendErr != nil {
				return sendErr
			}
			// send was successful, just continue
			continue
		case in = <-msgAvail:
			if err == io.EOF {
				log.Debug(fmt.Sprintf("Received EOF, ending chaincode support stream, %s", err))
				return err
			} else if err != nil {
				log.Error(fmt.Sprintf("Error handling chaincode support stream: %s", err))
				return err
			} else if in == nil {
				err = errors.New("received nil message, ending chaincode support stream")
				log.Debug("Received nil message, ending chaincode support stream")
				return err
			}

			log.Debug(fmt.Sprintf("[%s]Received message %s from shim", shorttxid(in.Txid), in.Type.String()))
			if in.Type.String() == pb.ChaincodeMessage_ERROR.String() {
				log.Error(fmt.Sprintf("Got error: %s", string(in.Payload)))
			}

			// we can spin off another Recv again
			recv = true

			if in.Type == pb.ChaincodeMessage_KEEPALIVE {
				log.Debug("Received KEEPALIVE Response")
				// Received a keep alive message, we don't do anything with it for now
				// and it does not touch the state machine
				continue
			}
		case nsInfo = <-handler.nextState:
			// if next state info msg occur
			in = nsInfo.msg
			if in == nil {
				err = errors.New("next state nil message, ending chaincode support stream")
				log.Debug("Next state nil message, ending chaincode support stream")
				return err
			}
			log.Debug(fmt.Sprintf("[%s]Move state message %s", shorttxid(in.Txid), in.Type.String()))
		case <-handler.waitForKeepaliveTimer():
			if handler.chaincodeSupport.keepalive <= 0 {
				log.Debug(fmt.Sprintf("Invalid select: keepalive not on (keepalive=%d)", handler.chaincodeSupport.keepalive))
				continue
			}

			// if no error message from serialSend, KEEPALIVE happy, and don't care about error
			// (maybe it'll work later)
			handler.serialSendAsync(&pb.ChaincodeMessage{Type: pb.ChaincodeMessage_KEEPALIVE}, nil)
			continue
		}

		err = handler.HandleMessage(in)
		if err != nil {
			log.Error(fmt.Sprintf("[%s]Error handling message, ending stream: %s", shorttxid(in.Txid), err))
			return fmt.Errorf("error handling message, ending stream: %s", err)
		}

		if nsInfo != nil && nsInfo.sendToCC {
			// ready messages are sent sync
			if nsInfo.sendSync {
				if in.Type.String() != pb.ChaincodeMessage_READY.String() {
					panic(fmt.Sprintf("[%s]Sync send can only be for READY state %s\n", shorttxid(in.Txid), in.Type.String()))
				}
				if err = handler.serialSend(in); err != nil {
					return fmt.Errorf("[%s]Error sending ready  message, ending stream: %s", shorttxid(in.Txid), err)
				}
			} else {
				// if error bail in select
				log.Info("serialSendAsync", "txid", shorttxid(in.Txid))
				handler.serialSendAsync(in, errc)
			}
		}
	}
}

// 执行发送的逻辑
func (handler *Handler) sendExecuteMessage(ctxt context.Context,
	msg *pb.ChaincodeMessage, cccid *CCContext) (chan *pb.ChaincodeMessage, error) {

	log.Info("into sendExecuteMessage")
	// 如果交易存在，创建将发生err
	txctx, err := handler.createTxContext(ctxt, msg.Txid, cccid)
	if err != nil {
		return nil, err
	}

	// if security is disabled the context elements will just be nil
	if err = handler.setChaincodeProposal(cccid.SignedProposal, cccid.Proposal, msg); err != nil {
		return nil, err
	}

	// 这里将msg丢进nextState，handler将监听处理
	handler.triggerNextState(msg, true)
	// 返回此次执行的notify
	return txctx.responseNotifier, nil
}

// HandleMessage implementation of MessageHandler interface.  Peer's handling of Chaincode messages.
func (handler *Handler) HandleMessage(msg *pb.ChaincodeMessage) error {
	if (msg.Type == pb.ChaincodeMessage_COMPLETED || msg.Type == pb.ChaincodeMessage_ERROR) && handler.FSM.Current() == "ready" {
		// notify
		handler.notify(msg)
		return nil
	}

	if handler.FSM.Cannot(msg.Type.String()) {
		// Other errors
		return fmt.Errorf("[%s]Chaincode handler validator FSM cannot handle message (%s) with payload size (%d) while in state: %s",
			msg.Txid, msg.Type.String(), len(msg.Payload), handler.FSM.Current())
	}

	// 状态机处理
	eventErr := handler.FSM.Event(msg.Type.String(), msg)
	filteredErr := filterError(eventErr)
	if filteredErr != nil {
		log.Error(fmt.Sprintf("[%s]Failed to trigger FSM event %s: %s", msg.Txid, msg.Type.String(), filteredErr))
	}

	return filteredErr
}

// Filter the Errors to allow NoTransitionError and CanceledError to not propagate for cases where embedded Err == nil
func filterError(errFromFSMEvent error) error {

	if errFromFSMEvent != nil {
		if noTransitionErr, ok := errFromFSMEvent.(*fsm.NoTransitionError); ok {
			if noTransitionErr.Err != nil {
				// Squash the NoTransitionError
				return errFromFSMEvent
			}
			log.Debug(fmt.Sprintf("Ignoring NoTransitionError: %s", noTransitionErr))
		}
		if canceledErr, ok := errFromFSMEvent.(*fsm.CanceledError); ok {
			if canceledErr.Err != nil {
				// Squash the CanceledError
				return canceledErr
			}
			log.Debug(fmt.Sprintf("Ignoring CanceledError: %s", canceledErr))
		}
	}
	return nil
}

// beforeRegisterEvent is invoked when chaincode tries to register.
func (handler *Handler) beforeRegisterEvent(e *fsm.Event, state string) {
	log.Debug(fmt.Sprintf("Received %s in state %s", e.Event, state))
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}

	//chaincodeID 包含 owner/name/version
	chaincodeID := &pb.ChaincodeID{}
	err := proto.Unmarshal(msg.Payload, chaincodeID)
	if err != nil {
		e.Cancel(fmt.Errorf("error in received %s, could NOT unmarshal registration info: %s", pb.ChaincodeMessage_REGISTER, err))
		return
	}

	// Now register with the chaincodeSupport
	handler.ChaincodeID = chaincodeID
	err = handler.chaincodeSupport.registerHandler(handler)
	if err != nil {
		e.Cancel(err)
		handler.notifyDuringStartup(false)
		return
	}

	// getNextRange the component parts so we can use the root chaincode
	// name in keys
	handler.decomposeRegisteredName(handler.ChaincodeID)

	log.Debug(fmt.Sprintf("Got %s for chaincodeID = %s, sending back %s", e.Event, chaincodeID, pb.ChaincodeMessage_REGISTERED))
	if err := handler.serialSend(
		&pb.ChaincodeMessage{Type: pb.ChaincodeMessage_REGISTERED,
			Payload: []byte(handler.ChaincodeID.Name), ChannelId: msg.ChannelId}); err != nil {
		e.Cancel(fmt.Errorf("error sending %s: %s", pb.ChaincodeMessage_REGISTERED, err))
		handler.notifyDuringStartup(false)
		return
	}
}

func (handler *Handler) deregister() error {
	log.Info("handler deregister...")
	if handler.registered {
		handler.chaincodeSupport.deregisterHandler(handler)
	}
	return nil
}

// afterGetState handles a GET_STATE request from the chaincode.
func (handler *Handler) afterGetState(e *fsm.Event, state string) {
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("[%s]Received %s, invoking getNextRange state from ledger", shorttxid(msg.Txid), pb.ChaincodeMessage_GET_STATE))

	// Query ledger for state
	handler.handleGetState(msg)
}

// Handles query to ledger to getNextRange state
func (handler *Handler) handleGetState(msg *pb.ChaincodeMessage) {
	// The defer followed by triggering a go routine dance is needed to ensure that the previous state transition
	// is completed before the next one is triggered. The previous state transition is deemed complete only when
	// the afterGetState function is exited. Interesting bug fix!!
	go func() {
		// Check if this is the unique state request from this chaincode txid
		uniqueReq := handler.createTXIDEntry(msg.ChannelId, msg.Txid)
		if !uniqueReq {
			// Drop this request
			log.Error("Another state request pending for this Txid. Cannot process.")
			return
		}

		var serialSendMsg *pb.ChaincodeMessage
		var txContext *transactionContext
		txContext, serialSendMsg = handler.isValidTxSim(msg.ChannelId, msg.Txid,
			"[%s]No ledger context for GetState. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)

		defer func() {

			handler.deleteTXIDEntry(msg.ChannelId, msg.Txid)
			handler.serialSendAsync(serialSendMsg, nil)
		}()

		if txContext == nil {
			return
		}

		getState := &pb.GetState{}
		unmarshalErr := proto.Unmarshal(msg.Payload, getState)
		if unmarshalErr != nil {
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR,
				Payload: []byte(unmarshalErr.Error()),
				Txid:    msg.Txid, ChannelId: msg.ChannelId}
		}

		chaincodeID := handler.getCCRootName()

		res := txContext.pdxData.GetRaw(chaincodeID, getState.Key)

		// Send response msg back to chaincode. GetState will not trigger event
		serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RESPONSE, Payload: res, Txid: msg.Txid, ChannelId: msg.ChannelId}

	}()
}

// Handles request to ledger to put state
func (handler *Handler) enterBusyState(e *fsm.Event, state string) {
	go func() {
		msg, _ := e.Args[0].(*pb.ChaincodeMessage)
		log.Debug(fmt.Sprintf("[%s]state is %s", shorttxid(msg.Txid), state))
		// Check if this is the unique request from this chaincode txid
		uniqueReq := handler.createTXIDEntry(msg.ChannelId, msg.Txid)
		if !uniqueReq {
			// Drop this request
			log.Debug(fmt.Sprintf("Another request pending for this CC: %s, Txid: %s, ChannelID: %s. Cannot process.", handler.ChaincodeID.Name, msg.Txid, msg.ChannelId))
			return
		}

		var triggerNextStateMsg *pb.ChaincodeMessage
		var txContext *transactionContext

		txContext, triggerNextStateMsg = handler.getTxContextForMessage(msg.ChannelId, msg.Txid, msg.Type.String(), msg.Payload,
			"[%s]No ledger context for %s. Sending %s", shorttxid(msg.Txid), msg.Type.String(), pb.ChaincodeMessage_ERROR)

		defer func() {

			handler.deleteTXIDEntry(msg.ChannelId, msg.Txid)
			//log.Debug(fmt.Sprintf("[%s]enterBusyState trigger event %s", shorttxid(triggerNextStateMsg.Txid), triggerNextStateMsg.Type))
			handler.triggerNextState(triggerNextStateMsg, true)
		}()

		if txContext == nil {
			log.Warn(fmt.Sprintf("[%s]enterBusyState tx context is nil", shorttxid(triggerNextStateMsg.Txid)))
			return
		}

		errHandler := func(payload []byte, errFmt string, errArgs ...interface{}) {
			log.Error(fmt.Sprintf(errFmt, errArgs))
			triggerNextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChannelId: msg.ChannelId}
		}

		chaincodeID := handler.getCCRootName()
		var err error
		var res []byte

		if msg.Type.String() == pb.ChaincodeMessage_PUT_STATE.String() {
			putState := &pb.PutState{}
			unmarshalErr := proto.Unmarshal(msg.Payload, putState)
			if unmarshalErr != nil {
				errHandler([]byte(unmarshalErr.Error()), "[%s]Unable to decipher payload. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
				return
			}

			if conf.NoLimitCC[chaincodeID] == nil {
				num := txContext.pdxData.GetRawNum(chaincodeID)
				stateKeyNum := conf.CCViper.GetInt(conf.SandboxStateKeyNum)
				if num > stateKeyNum {
					log.Error(fmt.Sprintf("[%s]Failed to handle %s. Sending %s", shorttxid(msg.Txid), msg.Type.String(), pb.ChaincodeMessage_ERROR))
					errHandler([]byte(chaincodeID+" state num exceeds the maximum:"+strconv.Itoa(stateKeyNum)), "[%s]Failed to handle %s. Sending %s", shorttxid(msg.Txid), msg.Type.String(), pb.ChaincodeMessage_ERROR)
					return
				}

				length := len([]byte(putState.Key)) + len(putState.Value)
				stateSize := conf.CCViper.GetString(conf.SandboxStateSize)
				size, err := strconv.Atoi(stateSize[:len(stateSize)-1])
				if err != nil {
					errHandler([]byte(err.Error()), "[%s]Failed to handle %s. Sending %s", shorttxid(msg.Txid), msg.Type.String(), pb.ChaincodeMessage_ERROR)
					return
				}
				if length > size {
					log.Error(fmt.Sprintf("[%s]Unable to decipher payload. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR.String()))
					errHandler([]byte("state size(key+value) exceeds the maximum:"+stateSize), "%s Unable to decipher payload. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
					return
				}
			}

			// 目前 putState.collection 都是“” 即都为公共数据
			txContext.pdxData.PutRaw(chaincodeID, putState.Key, putState.Value, msg.Txid, msg.Timestamp)
		} else if msg.Type.String() == pb.ChaincodeMessage_DEL_STATE.String() {
			// Invoke ledger to delete state
			delState := &pb.DelState{}
			unmarshalErr := proto.Unmarshal(msg.Payload, delState)
			if unmarshalErr != nil {
				errHandler([]byte(unmarshalErr.Error()), "[%s]Unable to decipher payload. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
				return
			}

			txContext.pdxData.DelRaw(chaincodeID, delState.Key, msg.Txid, msg.Timestamp)
		} else if msg.Type.String() == pb.ChaincodeMessage_INVOKE_CHAINCODE.String() {
			err = errors.New("unsupported operation")
		}

		if err != nil {
			errHandler([]byte(err.Error()), "[%s]Failed to handle %s. Sending %s", shorttxid(msg.Txid), msg.Type.String(), pb.ChaincodeMessage_ERROR)
			return
		}

		// Send response msg back to chaincode.
		log.Debug(fmt.Sprintf("[%s]Completed %s. Sending %s", shorttxid(msg.Txid), msg.Type.String(), pb.ChaincodeMessage_RESPONSE))

		triggerNextStateMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RESPONSE, Payload: res, Txid: msg.Txid, ChannelId: msg.ChannelId}

	}()

}

// afterGetStateByRange handles a GET_STATE_BY_RANGE request from the chaincode.
func (handler *Handler) afterGetStateByRange(e *fsm.Event, state string) {
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("Received %s, invoking getNextRange state from ledger", pb.ChaincodeMessage_GET_STATE_BY_RANGE))

	// Query ledger for state
	handler.handleGetStateByRange(msg)
	log.Debug("Exiting GET_STATE_BY_RANGE")
}

// Handles query to ledger to rage query state
func (handler *Handler) handleGetStateByRange(msg *pb.ChaincodeMessage) {
	go func() {
		// Check if this is the unique state request from this chaincode txid
		uniqueReq := handler.createTXIDEntry(msg.ChannelId, msg.Txid)
		if !uniqueReq {
			// Drop this request
			log.Error("Another state request pending for this Txid. Cannot process.")
			return
		}

		var serialSendMsg *pb.ChaincodeMessage

		defer func() {

			handler.deleteTXIDEntry(msg.ChannelId, msg.Txid)
			log.Debug(fmt.Sprintf("[%s]handleGetStateByRange serial send %s", shorttxid(serialSendMsg.Txid), serialSendMsg.Type))
			handler.serialSendAsync(serialSendMsg, nil)
		}()

		getStateByRange := &pb.GetStateByRange{}
		unmarshalErr := proto.Unmarshal(msg.Payload, getStateByRange)
		if unmarshalErr != nil {
			payload := []byte(unmarshalErr.Error())
			log.Debug(fmt.Sprintf("Failed to unmarshall range query request. Sending %s", pb.ChaincodeMessage_ERROR))
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChannelId: msg.ChannelId}
			return
		}

		iterID := util.GenerateUUID()
		var txContext *transactionContext

		txContext, serialSendMsg = handler.isValidTxSim(msg.ChannelId, msg.Txid, "[%s]No ledger context for GetStateByRange. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
		if txContext == nil {
			return
		}

		chaincodeID := handler.getCCRootName()

		errHandler := func(err error, errFmt string, errArgs ...interface{}) {
			handler.cleanupQueryContext(txContext, iterID)
			payload := []byte(err.Error())
			log.Error(fmt.Sprintf(errFmt, errArgs...))
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChannelId: msg.ChannelId}
		}

		var err error

		handler.initializeRangeContext(txContext, iterID,
			getStateByRange.StartKey, getStateByRange.EndKey)

		startKey, endKey := handler.getNextRange(txContext, iterID)

		var payload *pb.QueryResponse
		payload, err = getRangeResponse(handler, txContext, iterID, chaincodeID,
			startKey, endKey,
		)
		if err != nil {
			errHandler(err, "Failed to getNextRange query result. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		var payloadBytes []byte
		payloadBytes, err = proto.Marshal(payload)
		if err != nil {
			errHandler(err, "Failed to marshal response. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		log.Debug(fmt.Sprintf("Got keys and values. Sending %s", pb.ChaincodeMessage_RESPONSE))
		serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RESPONSE, Payload: payloadBytes, Txid: msg.Txid, ChannelId: msg.ChannelId}

	}()
}

// afterGetQueryResult handles a GET_QUERY_RESULT request from the chaincode.
func (handler *Handler) afterGetQueryResult(e *fsm.Event, state string) {
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("Received %s, invoking getNextRange state from ledger", pb.ChaincodeMessage_GET_QUERY_RESULT))

	// Query ledger for state
	handler.handleGetQueryResult(msg)
	log.Debug("Exiting GET_QUERY_RESULT")
}

// Handles query to ledger to execute query state
func (handler *Handler) handleGetQueryResult(msg *pb.ChaincodeMessage) {
	go func() {
		// Check if this is the unique state request from this chaincode txid
		uniqueReq := handler.createTXIDEntry(msg.ChannelId, msg.Txid)
		if !uniqueReq {
			// Drop this request
			log.Error("Another state request pending for this Txid. Cannot process.")
			return
		}

		var serialSendMsg *pb.ChaincodeMessage

		defer func() {
			handler.deleteTXIDEntry(msg.ChannelId, msg.Txid)
			log.Debug(fmt.Sprintf("[%s]handleGetStateByRange serial send %s", shorttxid(serialSendMsg.Txid), serialSendMsg.Type))
			handler.serialSendAsync(serialSendMsg, nil)
		}()

		payload := []byte("unsupported operation")
		serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChannelId: msg.ChannelId}
	}()
}

// afterGetHistoryForKey handles a GET_HISTORY_FOR_KEY request from the chaincode.
func (handler *Handler) afterGetHistoryForKey(e *fsm.Event, state string) {
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("Received %s, invoking getNextRange state from ledger", pb.ChaincodeMessage_GET_HISTORY_FOR_KEY))

	// Query ledger history db
	handler.handleGetHistoryForKey(msg)
	log.Debug("Exiting GET_HISTORY_FOR_KEY")
}

// Handles query to ledger history db
func (handler *Handler) handleGetHistoryForKey(msg *pb.ChaincodeMessage) {
	go func() {
		// Check if this is the unique state request from this chaincode txid
		uniqueReq := handler.createTXIDEntry(msg.ChannelId, msg.Txid)
		if !uniqueReq {
			// Drop this request
			log.Error("Another state request pending for this Txid. Cannot process.")
			return
		}

		var serialSendMsg *pb.ChaincodeMessage

		defer func() {

			handler.deleteTXIDEntry(msg.ChannelId, msg.Txid)
			log.Debug(fmt.Sprintf("[%s]handleGetHistoryForKey serial send %s", shorttxid(serialSendMsg.Txid), serialSendMsg.Type))
			handler.serialSendAsync(serialSendMsg, nil)
		}()

		var iterID string
		var txContext *transactionContext

		errHandler := func(payload []byte, errFmt string, errArgs ...interface{}) {
			handler.cleanupQueryContext(txContext, iterID)
			log.Error(fmt.Sprintf(errFmt, errArgs...))
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChannelId: msg.ChannelId}
		}

		getHistoryForKey := &pb.GetHistoryForKey{}
		unmarshalErr := proto.Unmarshal(msg.Payload, getHistoryForKey)
		if unmarshalErr != nil {
			errHandler([]byte(unmarshalErr.Error()), "Failed to unmarshall query request. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		iterID = util.GenerateUUID()

		txContext, serialSendMsg = handler.isValidTxSim(msg.ChannelId, msg.Txid, "[%s]No ledger context for GetHistoryForKey. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
		if txContext == nil {
			return
		}
		chaincodeID := handler.getCCRootName()

		handler.initializeHisContext(txContext, iterID, getHistoryForKey.Key)

		var payload *pb.QueryResponse
		key, version := handler.getNextHis(txContext, iterID)

		payload, err := getHisResponse(handler, txContext, iterID, chaincodeID, key, version)

		if err != nil {
			errHandler([]byte(err.Error()), "Failed to get query result. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		var payloadBytes []byte
		payloadBytes, err = proto.Marshal(payload)
		if err != nil {
			errHandler([]byte(err.Error()), "Failed marshal response. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		log.Debug(fmt.Sprintf("Got keys and values. Sending %s", pb.ChaincodeMessage_RESPONSE))
		serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RESPONSE, Payload: payloadBytes, Txid: msg.Txid, ChannelId: msg.ChannelId}
	}()
}

// afterQueryStateNext handles a QUERY_STATE_NEXT request from the chaincode.
func (handler *Handler) afterQueryStateNext(e *fsm.Event, state string) {
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(errors.New("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("Received %s, invoking query state next from ledger", pb.ChaincodeMessage_QUERY_STATE_NEXT))

	// Query ledger for state
	handler.handleQueryStateNext(msg)
	log.Debug("Exiting QUERY_STATE_NEXT")
}

// Handles query to ledger for query state next
func (handler *Handler) handleQueryStateNext(msg *pb.ChaincodeMessage) {
	go func() {
		// Check if this is the unique state request from this chaincode txid
		uniqueReq := handler.createTXIDEntry(msg.ChannelId, msg.Txid)
		if !uniqueReq {
			// Drop this request
			log.Error("Another state request pending for this Txid. Cannot process.")
			return
		}

		var serialSendMsg *pb.ChaincodeMessage

		defer func() {

			handler.deleteTXIDEntry(msg.ChannelId, msg.Txid)
			log.Debug(fmt.Sprintf("[%s]handleGetStateByRange serial send %s", shorttxid(serialSendMsg.Txid), serialSendMsg.Type))
			handler.serialSendAsync(serialSendMsg, nil)
		}()

		var txContext *transactionContext
		var queryStateNext *pb.QueryStateNext

		errHandler := func(payload []byte, errFmt string, errArgs ...interface{}) {
			handler.cleanupQueryContext(txContext, queryStateNext.Id)
			log.Error(fmt.Sprintf(errFmt, errArgs...))
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChannelId: msg.ChannelId}
		}

		queryStateNext = &pb.QueryStateNext{}

		unmarshalErr := proto.Unmarshal(msg.Payload, queryStateNext)
		if unmarshalErr != nil {
			errHandler([]byte(unmarshalErr.Error()), "Failed to unmarshall state next query request. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		txContext = handler.getTxContext(msg.ChannelId, msg.Txid)
		if txContext == nil {
			errHandler([]byte("transaction context not found (timed out ?)"), "[%s]Failed to getNextRange transaction context. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
			return
		}

		chaincodeID := handler.getCCRootName()
		var payload *pb.QueryResponse
		var err error
		_, rangeExists := txContext.nextMap[queryStateNext.Id]
		if rangeExists {
			startKey, endKey := handler.getNextRange(txContext, queryStateNext.Id)
			payload, err = getRangeResponse(handler, txContext, queryStateNext.Id, chaincodeID, startKey, endKey)
		}

		_, hisExists := txContext.historyMap[queryStateNext.Id]
		if hisExists {
			key, version := handler.getNextHis(txContext, queryStateNext.Id)
			payload, err = getHisResponse(handler, txContext, queryStateNext.Id, chaincodeID, key, version)
		}

		if !rangeExists && !hisExists {
			err = errors.New("un know query range or his")
		}

		if err != nil {
			errHandler([]byte(err.Error()), "Failed to get query result. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		payloadBytes, err := proto.Marshal(payload)
		if err != nil {
			errHandler([]byte(err.Error()), "Failed to marshal response. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}
		log.Debug(fmt.Sprintf("Got keys and values. Sending %s", pb.ChaincodeMessage_RESPONSE))
		serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RESPONSE, Payload: payloadBytes, Txid: msg.Txid, ChannelId: msg.ChannelId}

	}()
}

// afterQueryStateClose handles a QUERY_STATE_CLOSE request from the chaincode.
func (handler *Handler) afterQueryStateClose(e *fsm.Event, state string) {
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("Received %s, invoking query state close from ledger", pb.ChaincodeMessage_QUERY_STATE_CLOSE))

	// Query ledger for state
	handler.handleQueryStateClose(msg)
	log.Debug("Exiting QUERY_STATE_CLOSE")
}

// Handles the closing of a state iterator
func (handler *Handler) handleQueryStateClose(msg *pb.ChaincodeMessage) {
	go func() {
		// Check if this is the unique state request from this chaincode txid
		uniqueReq := handler.createTXIDEntry(msg.ChannelId, msg.Txid)
		if !uniqueReq {
			// Drop this request
			log.Error("Another state request pending for this Txid. Cannot process.")
			return
		}

		var serialSendMsg *pb.ChaincodeMessage

		defer func() {

			handler.deleteTXIDEntry(msg.ChannelId, msg.Txid)
			log.Debug(fmt.Sprintf("[%s]handleQueryStateClose serial send %s", shorttxid(serialSendMsg.Txid), serialSendMsg.Type))
			handler.serialSendAsync(serialSendMsg, nil)
		}()

		errHandler := func(payload []byte, errFmt string, errArgs ...interface{}) {
			log.Error(fmt.Sprintf(errFmt, errArgs...))
			serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: payload, Txid: msg.Txid, ChannelId: msg.ChannelId}
		}

		queryStateClose := &pb.QueryStateClose{}
		unmarshalErr := proto.Unmarshal(msg.Payload, queryStateClose)
		if unmarshalErr != nil {
			errHandler([]byte(unmarshalErr.Error()), "Failed to unmarshall state query close request. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		txContext := handler.getTxContext(msg.ChannelId, msg.Txid)
		if txContext == nil {
			errHandler([]byte("transaction context not found (timed out ?)"), "[%s]Failed to getNextRange transaction context. Sending %s", shorttxid(msg.Txid), pb.ChaincodeMessage_ERROR)
			return
		}

		handler.cleanupQueryContext(txContext, queryStateClose.Id)

		payload := &pb.QueryResponse{HasMore: false, Id: queryStateClose.Id}
		payloadBytes, err := proto.Marshal(payload)
		if err != nil {
			errHandler([]byte(err.Error()), "Failed marshall resopnse. Sending %s", pb.ChaincodeMessage_ERROR)
			return
		}

		log.Debug(fmt.Sprintf("Closed. Sending %s", pb.ChaincodeMessage_RESPONSE))
		serialSendMsg = &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_RESPONSE, Payload: payloadBytes, Txid: msg.Txid, ChannelId: msg.ChannelId}

	}()
}

// getRangeResponse takes an iterator and fetch state to construct QueryResponse
func getRangeResponse(handler *Handler, txContext *transactionContext, iterID string, chaincodeID string, startKey, endKey string) (*pb.QueryResponse, error) {

	res, hasMore := txContext.pdxData.GetRawRange(chaincodeID, startKey, endKey)

	if res == nil {
		// nil response from iterator indicates end of query results
		handler.cleanupQueryContext(txContext, iterID)

		return &pb.QueryResponse{Results: nil, HasMore: hasMore != "", Id: iterID}, nil
	} else {
		result := make([]*pb.QueryResultBytes, len(res))
		for index, temp := range res {
			marshal, _ := proto.Marshal(temp)
			result[index] = &pb.QueryResultBytes{ResultBytes: marshal}
		}
		txContext.nextMap[iterID] = hasMore
		return &pb.QueryResponse{Results: result, HasMore: hasMore != "", Id: iterID}, nil
	}

}

func getHisResponse(handler *Handler, txContext *transactionContext, iterID string, chaincodeID string, key string, version int) (*pb.QueryResponse, error) {

	res, hasMore := txContext.pdxData.GetRawHis(chaincodeID, key, version)

	if res == nil {
		// nil response from iterator indicates end of query results
		handler.cleanupQueryContext(txContext, iterID)

		return &pb.QueryResponse{Results: nil, HasMore: hasMore > 0, Id: iterID}, nil
	} else {
		result := make([]*pb.QueryResultBytes, len(res))
		for index, temp := range res {
			marshal, _ := proto.Marshal(temp)
			result[index] = &pb.QueryResultBytes{ResultBytes: marshal}
		}
		txContext.versionMap[iterID] = hasMore
		return &pb.QueryResponse{Results: result, HasMore: hasMore > 0, Id: iterID}, nil
	}

}

// beforeCompletedEvent is invoked when chaincode has completed execution of init, invoke.
func (handler *Handler) beforeCompletedEvent(e *fsm.Event, state string) {
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(errors.New("received unexpected message type"))
		return
	}
	// Notify on channel once into READY state
	log.Debug(fmt.Sprintf("[%s]beforeCompleted - not in ready state will notify when in readystate", shorttxid(msg.Txid)))
	return
}

func (handler *Handler) enterEstablishedState(e *fsm.Event, state string) {
	handler.notifyDuringStartup(true)
}

func (handler *Handler) enterReadyState(e *fsm.Event, state string) {
	// Now notify
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("[%s]Entered state %s", shorttxid(msg.Txid), state))
	handler.notify(msg)
}

func (handler *Handler) enterEndState(e *fsm.Event, state string) {
	defer handler.deregister()
	// Now notify
	msg, ok := e.Args[0].(*pb.ChaincodeMessage)
	if !ok {
		e.Cancel(fmt.Errorf("received unexpected message type"))
		return
	}
	log.Debug(fmt.Sprintf("[%s]Entered state %s", shorttxid(msg.Txid), state))
	handler.notify(msg)
	e.Cancel(fmt.Errorf("entered end state"))
}

func (handler *Handler) setChaincodeProposal(signedProp *pb.SignedProposal, prop *pb.Proposal, msg *pb.ChaincodeMessage) error {
	// 目前以太坊实现，暂不支持签名交易传递，这里没有
	return nil
}

func (handler *Handler) notify(msg *pb.ChaincodeMessage) {
	handler.Lock()
	defer handler.Unlock()
	tctx := handler.txCtxs[handler.getTxCtxId(msg.ChannelId, msg.Txid)]
	if tctx == nil {
		log.Debug(fmt.Sprintf("notifier Txid:%s does not exist", msg.Txid))
	} else {
		//log.Debug(fmt.Sprintf("notifying Txid:%s", msg.Txid))
		tctx.responseNotifier <- msg
	}
}

func newChaincodeSupportHandler(chaincodeSupport *ChaincodeSupport, peerChatStream ChaincodeStream) *Handler {
	v := &Handler{
		ChatStream: peerChatStream,
	}
	v.chaincodeSupport = chaincodeSupport
	// we want this to block
	v.nextState = make(chan *nextStateInfo)

	v.FSM = fsm.NewFSM(
		createdstate,
		fsm.Events{
			// Send REGISTERED, then, if deploy { trigger INIT(via INIT) } else { trigger READY(via COMPLETED) }
			// when register request ----> establishedstate
			// then trigger enter_establishedstate and send ready message handle by handler
			// update fsm ----> readystate then trigger enter readystate
			{Name: pb.ChaincodeMessage_REGISTER.String(), Src: []string{createdstate}, Dst: establishedstate},
			{Name: pb.ChaincodeMessage_READY.String(), Src: []string{establishedstate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_PUT_STATE.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_DEL_STATE.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_INVOKE_CHAINCODE.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_COMPLETED.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_GET_STATE.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_GET_STATE_BY_RANGE.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_GET_QUERY_RESULT.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_GET_HISTORY_FOR_KEY.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_QUERY_STATE_NEXT.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_QUERY_STATE_CLOSE.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_ERROR.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_RESPONSE.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_INIT.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_TRANSACTION.String(), Src: []string{readystate}, Dst: readystate},
			{Name: pb.ChaincodeMessage_BAAP_QUERY.String(), Src: []string{readystate}, Dst: readystate},
		},
		fsm.Callbacks{
			"before_" + pb.ChaincodeMessage_REGISTER.String():           func(e *fsm.Event) { v.beforeRegisterEvent(e, v.FSM.Current()) },
			"enter_" + establishedstate:                                 func(e *fsm.Event) { v.enterEstablishedState(e, v.FSM.Current()) },
			"enter_" + readystate:                                       func(e *fsm.Event) { v.enterReadyState(e, v.FSM.Current()) },
			"before_" + pb.ChaincodeMessage_COMPLETED.String():          func(e *fsm.Event) { v.beforeCompletedEvent(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_GET_STATE.String():           func(e *fsm.Event) { v.afterGetState(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_GET_STATE_BY_RANGE.String():  func(e *fsm.Event) { v.afterGetStateByRange(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_GET_QUERY_RESULT.String():    func(e *fsm.Event) { v.afterGetQueryResult(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_GET_HISTORY_FOR_KEY.String(): func(e *fsm.Event) { v.afterGetHistoryForKey(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_QUERY_STATE_NEXT.String():    func(e *fsm.Event) { v.afterQueryStateNext(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_QUERY_STATE_CLOSE.String():   func(e *fsm.Event) { v.afterQueryStateClose(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_PUT_STATE.String():           func(e *fsm.Event) { v.enterBusyState(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_DEL_STATE.String():           func(e *fsm.Event) { v.enterBusyState(e, v.FSM.Current()) },
			"after_" + pb.ChaincodeMessage_INVOKE_CHAINCODE.String():    func(e *fsm.Event) { v.enterBusyState(e, v.FSM.Current()) },
			"enter_" + endstate:                                         func(e *fsm.Event) { v.enterEndState(e, v.FSM.Current()) },
			"before_" + pb.ChaincodeMessage_BAAP_QUERY.String():         func(e *fsm.Event) {},
		},
	)

	return v
}

// base method
// serialSend serializes msgs so gRPC will be happy
func (handler *Handler) serialSend(msg *pb.ChaincodeMessage) error {
	//log.Info("handler serialSend....")
	handler.serialLock.Lock()
	defer handler.serialLock.Unlock()

	var err error
	if err = handler.ChatStream.Send(msg); err != nil {
		err = fmt.Errorf("[%s]Error sending %s: %s", shorttxid(msg.Txid), msg.Type.String(), err)
		log.Error(fmt.Sprintf("%s", err.Error()))
	}
	return err
}

// serialSendAsync serves the same purpose as serialSend (serializ msgs so gRPC will
// be happy). In addition, it is also asynchronous so send-remoterecv--localrecv loop
// can be nonblocking. Only errors need to be handled and these are handled by
// communication on supplied error channel. A typical use will be a non-blocking or
// nil channel
func (handler *Handler) serialSendAsync(msg *pb.ChaincodeMessage, errc chan error) {
	//log.Info("handler serialSendAsync...")
	go func() {
		err := handler.serialSend(msg)
		if errc != nil {
			errc <- err
		}
	}()
}

func (handler *Handler) notifyDuringStartup(val bool) {
	// if USER_RUNS_CC readyNotify will be nil
	if handler.readyNotify != nil {
		log.Debug("Notifying during startup")
		handler.readyNotify <- val
	} else {
		// chaincodeLogger.Debug("nothing to notify (dev mode ?)")
		// In theory, we don't even need a devmode flag in the peer anymore
		// as the chaincode is brought up without any context (ledger context
		// in particular). What this means is we can have - in theory - a nondev
		// environment where we can attach a chaincode manually. This could be
		// useful .... but for now lets just be conservative and allow manual
		// chaincode only in dev mode (ie, peer started with --peer-chaincodedev=true)
		if val {
			log.Debug("sending READY")
			// i add the cc info for the proxy
			ccMsg := &pb.ChaincodeMessage{
				Type:    pb.ChaincodeMessage_READY,
				Payload: []byte(handler.ChaincodeID.Name),
			}
			go handler.triggerNextState(ccMsg, true)
		} else {
			log.Error("Error during startup .. not sending READY")
		}

	}
}

func (handler *Handler) triggerNextState(msg *pb.ChaincodeMessage, send bool) {
	// this will send Async
	log.Info("Handler triggerNextState")
	handler.nextState <- &nextStateInfo{msg: msg, sendToCC: send, sendSync: false}
}

func (handler *Handler) triggerNextStateSync(msg *pb.ChaincodeMessage) {
	// this will send sync
	log.Info("handler triggerNextStateSync...")
	handler.nextState <- &nextStateInfo{msg: msg, sendToCC: true, sendSync: true}
}

func (handler *Handler) createTXIDEntry(channelID, txid string) bool {
	if handler.txidMap == nil {
		return false
	}
	handler.Lock()
	defer handler.Unlock()
	txCtxID := handler.getTxCtxId(channelID, txid)
	if handler.txidMap[txCtxID] {
		return false
	}
	handler.txidMap[txCtxID] = true
	return handler.txidMap[txCtxID]
}

func (handler *Handler) deleteTXIDEntry(channelID, txid string) {
	handler.Lock()
	defer handler.Unlock()
	txCtxID := handler.getTxCtxId(channelID, txid)
	if handler.txidMap != nil {
		delete(handler.txidMap, txCtxID)
	} else {
		log.Warn(fmt.Sprintf("TXID %s not found!", txCtxID))
	}
}

func (handler *Handler) createTxContext(ctxt context.Context, txid string, cccid *CCContext) (*transactionContext, error) {
	if handler.txCtxs == nil {
		return nil, errors.New(fmt.Sprintf("cannot create notifier for txid: %s", txid))
	}
	handler.Lock()
	defer handler.Unlock()
	txCtxID := handler.getTxCtxId(cccid.ChainID, txid)

	if handler.txCtxs[txCtxID] != nil {
		return nil, errors.New(fmt.Sprintf("txid: %s(%s) exists", txid, cccid.ChainID))
	}
	txctx := &transactionContext{
		// readyOnly:  cccid.ReadOnly,
		pdxData:          cccid.PdxData,
		chainID:          cccid.ChainID,
		signedProp:       cccid.SignedProposal,
		proposal:         cccid.Proposal,
		responseNotifier: make(chan *pb.ChaincodeMessage, 1),
		nextMap:          make(map[string]string),
		endMap:           make(map[string]string),
		historyMap:       make(map[string]string),
		versionMap:       make(map[string]int),
	}

	handler.txCtxs[txCtxID] = txctx
	// txctx.historyQueryExecutor = getHistoryQueryExecutor(ctxt)

	return txctx, nil
}

func (handler *Handler) getTxContext(chainID, txid string) *transactionContext {
	handler.Lock()
	defer handler.Unlock()

	txCtxID := handler.getTxCtxId(chainID, txid)
	return handler.txCtxs[txCtxID]
}

func (handler *Handler) deleteTxContext(chainID, txid string) {
	handler.Lock()
	defer handler.Unlock()
	txCtxID := handler.getTxCtxId(chainID, txid)

	if handler.txCtxs != nil {
		delete(handler.txCtxs, txCtxID)
	}
}

// is this a txid for which there is a valid txsim
func (handler *Handler) isValidTxSim(channelID string, txid string, fmtStr string, args ...interface{}) (*transactionContext, *pb.ChaincodeMessage) {
	txContext := handler.getTxContext(channelID, txid)
	if txContext == nil {
		// Send error msg back to chaincode. No ledger context
		errStr := fmt.Sprintf(fmtStr, args...)
		log.Error(errStr)
		return nil, &pb.ChaincodeMessage{Type: pb.ChaincodeMessage_ERROR, Payload: []byte(errStr), Txid: txid, ChannelId: channelID}
	}
	return txContext, nil
}

func (handler *Handler) initializeRangeContext(txContext *transactionContext, queryID string, startKey, endKey string) {
	handler.Lock()
	defer handler.Unlock()
	txContext.nextMap[queryID] = startKey
	txContext.endMap[queryID] = endKey
}

func (handler *Handler) getNextRange(txContext *transactionContext, queryID string) (string, string) {
	handler.Lock()
	defer handler.Unlock()
	return txContext.nextMap[queryID], txContext.endMap[queryID]
}

func (handler *Handler) initializeHisContext(txContext *transactionContext, queryID string, key string) {
	handler.Lock()
	defer handler.Unlock()
	txContext.historyMap[queryID] = key
	txContext.versionMap[queryID] = 0
}

func (handler *Handler) getNextHis(txContext *transactionContext, queryID string) (string, int) {
	handler.Lock()
	defer handler.Unlock()
	return txContext.historyMap[queryID], txContext.versionMap[queryID]
}

func (handler *Handler) cleanupQueryContext(txContext *transactionContext, queryID string) {
	handler.Lock()
	defer handler.Unlock()
	delete(txContext.nextMap, queryID)
	delete(txContext.endMap, queryID)
	delete(txContext.historyMap, queryID)
	delete(txContext.versionMap, queryID)

}

func (handler *Handler) getTxContextForMessage(channelID string, txid string, msgType string, payload []byte,
	fmtStr string, args ...interface{}) (*transactionContext, *pb.ChaincodeMessage) {
	// if we have a channelID, just getNextRange the txsim from isValidTxSim
	// if this is NOT an INVOKE_CHAINCODE, then let isValidTxSim handle retrieving the txContext
	if channelID != "" || msgType != pb.ChaincodeMessage_INVOKE_CHAINCODE.String() {
		return handler.isValidTxSim(channelID, txid, fmtStr, args)
	}
	return nil, nil
}

func (handler *Handler) waitForKeepaliveTimer() <-chan time.Time {
	if handler.chaincodeSupport.keepalive > 0 {
		c := time.After(handler.chaincodeSupport.keepalive)
		return c
	}
	// no one will signal this channel, listen blocks forever
	c := make(chan time.Time, 1)
	return c
}

func shorttxid(txid string) string {
	if len(txid) < 8 {
		return txid
	}
	return txid[0:8]
}

// transaction context id should be composed of chainID and txid. While
// needed for CC-2-CC, it also allows users to concurrently send proposals
// with the same TXID to the SAME CC on multiple channels
func (handler *Handler) getTxCtxId(chainID string, txid string) string {
	return chainID + txid
}

// Check if the transactor is allow to call this chaincode on this channel
func (handler *Handler) checkACL(signedProp *pb.SignedProposal, proposal *pb.Proposal, ccIns *ChaincodeInstance) error {
	return nil
}

// gets chaincode instance from the canonical name of the chaincode.
// Called exactly once per chaincode when registering chaincode.
// This is needed for the "one-instance-per-chain" model when
// starting up the chaincode for each chain. It will still
// work for the "one-instance-for-all-chains" as the version
// and suffix will just be absent (also note that LSCC reserves
// "/:[]${}" as special chars mainly for such namespace uses)
func (handler *Handler) decomposeRegisteredName(cid *pb.ChaincodeID) {
	handler.ccInstance = getChaincodeInstance(cid.Name)
}

func getChaincodeInstance(ccName string) *ChaincodeInstance {
	log.Info("ccName", "is", ccName)
	b := []byte(ccName)
	ci := &ChaincodeInstance{}

	// compute suffix (ie, chain name)
	i := bytes.IndexByte(b, '/')
	if i >= 0 {
		if i < len(b)-1 {
			ci.ChainID = string(b[i+1:])
		}
		b = b[:i]
	}

	ccMsg := strings.Split(ccName, ":")
	if len(ccMsg) != 3 {
		ci.ChaincodeVersion = ""
	} else {
		ci.ChaincodeVersion = ccMsg[2]
	}

	// remaining is the chaincode name
	ci.ChaincodeName = ccName

	log.Info("!!!!!ci", "is", *ci)

	return ci
}

func (handler *Handler) getCCRootName() string {
	//return strings.ToLower(util.EthAddress(handler.ccInstance.ChaincodeName).Hex())
	return handler.ccInstance.ChaincodeName
}
