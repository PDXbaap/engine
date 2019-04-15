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
	"github.com/golang/protobuf/proto"
	"pdx-chain/common/hexutil"
	"pdx-chain/log"
	"pdx-chain/pdxcc/chaincode"
	pb "pdx-chain/pdxcc/protos"
	"pdx-chain/pdxcc/util"
	"sync"
)

var lruMap = util.NewLRUMap(1000)
var handingMap = make(map[string]chan int, 1)

func isHanding(nameHash string) chan int {
	return handingMap[nameHash]
}

func DeployQuery(ptx *pb.Transaction) (string, error) {
	deploy := &pb.Deployment{}
	err := proto.Unmarshal(ptx.Payload, deploy)
	if err != nil {
		log.Error("proto unmarshal")
		return "", err
	}

	var fs []*chaincode.DeployInfo
	switch deploy.Payload.Fcn {
	case "queryByPbk":
		fs = chaincode.Query("queryByPbk", string(deploy.Payload.Args[0]))
		break
	case "queryAll":
		fs = chaincode.Query("queryAll", nil)
		break
	case "queryByChaincodeId":
		fs = chaincode.Query("queryByChaincodeId", string(deploy.Payload.Args[0]))
		break
	case "queryRunningCcByName":
		fs = chaincode.Query("queryRunningCcByName", string(deploy.Payload.Args[0]), string(deploy.Payload.Args[1]))
		break

	}
	marshal, err := json.Marshal(fs)
	if err != nil {
		log.Error("json marshal error", "err", err)
		return "", err
	}
	return hexutil.Encode(marshal), nil
}

var streamService string
var once sync.Once

func getStreamService() (string, error) {
	once.Do(
		func() {
			pdxData := &PDXDataSupport{}
			value := pdxData.GetRaw("baap-base", "initConfig")

			m := make(map[string]interface{})
			err := json.Unmarshal(value, &m)
			if err != nil {
				log.Error("!!!!! unmarshal baap-stream-service-host err")
				return
			}

			streamService = m["baap-stream-service-host"].(string)
		})

	if streamService == "" {
		return "", errors.New("stream service is nil")
	}

	return streamService, nil
}
