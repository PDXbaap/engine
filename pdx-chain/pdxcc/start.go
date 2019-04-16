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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"pdx-chain/common"
	"pdx-chain/core/types"
	"pdx-chain/ethdb"
	"strings"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/golang/protobuf/proto"

	pb "pdx-chain/pdxcc/protos"
	"pdx-chain/pdxcc/util"

	"pdx-chain/pdxcc/conf"

	"pdx-chain/common/hexutil"
	"pdx-chain/log"
)

func Apply(caller common.Address, extra map[string][]byte, txd []byte, stateFunc func(fcn string, key string, value []byte) []byte) error {
	txid := hexutil.Encode(extra[conf.BaapTxid][:])[2:]

	s := time.Now().UnixNano() / 1e6
	defer func() {
		log.Info(fmt.Sprintf("Wow exit pdxcc.start apply host %d ms", time.Now().UnixNano()/1e6-s))
	}()
	tx := &pb.Transaction{}
	err := proto.Unmarshal(txd, tx)
	if err != nil {
		log.Error(fmt.Sprintf("!!!!! %s", err.Error()))
		return err
	}
	var inv *pb.Invocation

	switch tx.Type {
	case types.Transaction_deploy://
		deploy := pb.Deployment{}
		err = proto.Unmarshal(tx.Payload, &deploy)
		if err != nil {
			log.Error("proto unmarshal deploy error", "err", err)
			return err
		}
		inv = deploy.Payload

		//check sender == owner
		log.Info("transaction deploy", "ownerAddr", deploy.Owner, "caller", caller.String())
		ownerAddr := common.HexToAddress(deploy.Owner)
		if caller != ownerAddr {
			log.Error("caller != contract owner")
			return errors.New("caller != contract owner")
		}
	case types.Transaction_invoke://start stop withdraw
		invocation := &pb.Invocation{}
		err = proto.Unmarshal(tx.Payload, invocation)
		if err != nil {
			log.Error("proto unmarshal invocation error", "err", err)
			return err
		}

		//check sender == owner
		switch invocation.Fcn {
		case "upload", "deploy", "start", "stop", "withdraw":
			log.Info("check sender == owner")
			if len(invocation.Args) > 0 {
				s := string(invocation.Args[0])
				cc := strings.Split(s, ":")
				if len(cc) > 0 {
					ownerAddr := common.HexToAddress(cc[0])
					if caller != ownerAddr {
						log.Error("caller != contract owner", "caller", caller.String(), "owner", ownerAddr.String())
						return errors.New("caller != contract owner")
					}
				}else {
					log.Error("chaincode name abnormal")//owner:name:version
					return errors.New("chaincode name abnormal")
				}
			}else {
				log.Error("args is empty")
				return errors.New("args is empty")
			}
		}

		inv = invocation
	default:
		return errors.New("transaction type error")
	}

	meta := inv.Meta

	for key := range meta {
		if extra[key] == nil || len(extra[key]) == 0 {
			extra[key] = meta[key]
		}
	}

	execTime := conf.ApplyTime

	contractAddr := strings.ToLower(common.BytesToAddress(extra[conf.BaapDst]).String())
	key, find := theChaincodeSupport.chaincodeHasRan(contractAddr)
	if !find {
		log.Error("!!!!!chaincode not register error", "contractAddr", contractAddr)
		return errors.New("chaincode not register")
	}

	ccEnv, _ := theChaincodeSupport.chaincodeHasBeenLaunched(contractAddr)
	if !(key != "" && ccEnv != nil) {
		log.Error("!!!!! chaincode not running", "contractAddr", contractAddr, "key", key)
		return errors.New("chaincode not running")
	}

	_, err = execMessage(txd, inv, txid, stateFunc, execTime, extra, key)
	if err != nil {
		log.Error("!!!!!!!!!!execMessage error", "err", err)
		return err
	}

	return nil
}

func cacheResult(txid string, ccresp *pb.ChaincodeMessage, errStr string) {
	db := *ethdb.ChainDb
	key := fmt.Sprintf("executedTx:%s", txid)

	result := ExecReslt{ccresp, errStr}
	value, err := json.Marshal(result)
	if err != nil {
		log.Error("json Marshal error", "err", err)
		return
	}
	err = db.Put([]byte(key), value)
	if err != nil {
		log.Error("db put error", "err", err)
		return
	}
}

func execMessage(payload []byte, tx *pb.Invocation, txId string,
	stateFunc func(fcn string, key string, value []byte) []byte, time time.Duration, extra map[string][]byte, key string) (ccresp *pb.ChaincodeMessage, err error) {

	cpy := make(map[string][]byte, len(extra))
	for k, v := range extra {
		b := make([]byte, len(v))
		copy(b, v)
		cpy[k] = b
	}

	cpy[conf.BaapSpbk] = []byte(hexutil.Encode(extra[conf.BaapSpbk][:])[2:])//transit hex

	param, err := proto.Marshal(&pb.Meta{Meta: cpy})

	if err != nil {
		return nil, nil
	}

	params := append(tx.Args, param)
	ccInput := &pb.ChaincodeInput{Args: util.MakeChaincodeArgs(tx.Fcn, params), Decorations: extra}
	finalData, err := proto.Marshal(ccInput)
	if err != nil {
		return nil, nil
	}

	baapTxType := string(tx.Meta[conf.BaapTxType])

	var chaincodeMessageType pb.ChaincodeMessage_Type

	switch baapTxType {
	case conf.Exec:
		chaincodeMessageType = pb.ChaincodeMessage_TRANSACTION
	case conf.Init:
		chaincodeMessageType = pb.ChaincodeMessage_INIT
	case conf.Query:
		chaincodeMessageType = pb.ChaincodeMessage_BAAP_QUERY
	default:
		return nil, nil
	}
	msg := &pb.ChaincodeMessage{Type: chaincodeMessageType, Payload: finalData, Txid: txId, ChannelId: conf.ChainId.String()}

	cccid, err := NewCCContext(conf.ChainId.String(), key, txId, false, nil, &pb.Proposal{Payload: payload}, &PDXDataSupport{stateFunc})
	ccresp, err = theChaincodeSupport.Execute(context.Background(), cccid, msg, time)
	return ccresp, err
}

func BaapQuery(qStr []byte) (string, error) {
	if qStr == nil || len(qStr) == 0 {
		return "", errors.New("qStr can't nil")
	}

	ptx := &pb.Transaction{}
	err := proto.Unmarshal(qStr, ptx)
	if err != nil {
		return "", err
	}

	inv := &pb.Invocation{}
	err = proto.Unmarshal(ptx.Payload, inv)
	if err != nil {
		log.Error("proto unmarshal inv error", "err", err)
		return "", err
	}
	inv.Meta[conf.BaapTxType] = []byte(conf.Query)
	extra := map[string][]byte{conf.BaapEngineID: []byte(conf.ChainId.String())}
	to := strings.ToLower(string(inv.Meta["to"]))
	if !strings.HasPrefix(to, "0x") {
		to = "0x" + to
	}
	log.Info("baap query to", "buf", inv.Meta["to"], "str", to)
	key, find := theChaincodeSupport.chaincodeHasRan(to)
	if !find {
		log.Error("!!!!!chaincode not register error", "to", to)
		return "", errors.New("chaincode not register")
	}

	ccresp, err := execMessage(qStr, inv, util.GenerateUUID(), nil, conf.ApplyTime, extra, key)
	if err != nil {
		return "", err
	}

	var resp pb.Response
	err = proto.Unmarshal(ccresp.Payload, &resp)
	if err != nil {
		return "", err
	}

	return hexutil.Encode(resp.Payload), nil
}

func Duplicate(txid string) (result ExecReslt, find bool) {
	if len(txid) > 0 {
		db := *ethdb.ChainDb
		key := fmt.Sprintf("executedTx:%s", txid)
		res, err := db.Get([]byte(key))
		if err != nil {
			log.Error("db get error", "err", err)
			return
		}

		err = json.Unmarshal(res, &result)
		if err != nil {
			log.Error("json unmarshal error", "err", err)
			return
		}

		find = true
		return
	}
	return
}

func CanExec(nameHash string) bool {
	contract, running := theChaincodeSupport.chaincodeHasRan(strings.ToLower(nameHash))
	if running {
		log.Info("running is ok", "contract", contract)
		return true
	}

	if ch := isHanding(nameHash); ch != nil {
		select {
		case <-ch:
			return true
		}
	}
	return false
}

func Start(port int) {
	conf.InitConf() //在init中，编译bootnode时，会报conf文件找不到，所以移到此处

	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
		if err != nil {
			log.Error(fmt.Sprintf("failed to listen: %v", err))
		} else {
			log.Info(fmt.Sprintf("Success to listen: %d", port))
		}

		var opts []grpc.ServerOption

		gRpcServer := grpc.NewServer(opts...)

		registerChaincodeSupportServer(gRpcServer)

		gRpcServer.Serve(lis)
	}()

	for {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			log.Info("!!!!! watch grpc server staring...")
			time.Sleep(time.Second * 2)
			continue
		}

		log.Info("!!!!! watch grpc server started")
		conn.Close()
		break
	}
}
