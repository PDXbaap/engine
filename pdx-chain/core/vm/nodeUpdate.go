package vm

import (
	"encoding/json"
	"pdx-chain/common"
	"pdx-chain/ethdb"
	"pdx-chain/log"
	"pdx-chain/params"
	"pdx-chain/qualification"
	"pdx-chain/quorum"
)

const (
	consensus1 = 0
	observer   = 1
)

type NodeUpdate struct {
}

type ConsensusNodeUpdate struct {
	NodeType     int //consensus=0 ,observer=1
	FromChainID  uint64
	ToChainID    uint64
	Cert         string
	Address      []common.Address
	CommitHeight uint64
}

//
//type NodeInfo struct {
//	Address      []common.Address
//	CommitHeight uint64
//}

func (n *NodeUpdate) RequiredGas(input []byte) uint64 {
	return uint64(len(input)/192) * params.Bn256PairingPerPointGas
}

func (n *NodeUpdate) Run(ctx *PrecompiledContractContext, input []byte, extra map[string][]byte) ([]byte, error) {
	db := ethdb.ChainDb
	consensusNodeUpdate := ConsensusNodeUpdate{}
	json.Unmarshal(input, &consensusNodeUpdate)
	log.Info("consensusNodeUpdate的信息", "address", consensusNodeUpdate.Address[0].String(), "加入到高度", consensusNodeUpdate.CommitHeight)
	commitHeight := consensusNodeUpdate.CommitHeight
	for _, address := range consensusNodeUpdate.Address {
		commitQuorum, ok := quorum.CommitHeightToConsensusQuorum.Get(commitHeight, *db)
		if !ok {
			commitQuorum = quorum.NewNodeAddress()
		} else {

			commitQuorum.Add(address.Hex(), address)
		}
		for i := 0; i < int(qualification.ActiveDistant)+1; i++ {
			//nodeDetails, ok := qualification.CommitHeight2NodeDetailSetCache.Dup(commitHeight-uint64(i), *db)
			//if !ok {
			//	nodeDetails = qualification.NewNodeSet()
			//	nodeDetails.Add(address.Hex(), &qualification.NodeDetail{Address: address})
			//} else {
			//	minerDetail := nodeDetails.Get(address.Hex())
			//	if minerDetail == nil {
			//		nodeDetails.Add(address.Hex(), &qualification.NodeDetail{Address: address})
			//	}
			//}
			//nodeAddDetail := nodeDetails.Get(address.Hex())
			//nodeAddDetail.NumAssertionsAccepted++
			//nodeAddDetail.NumAssertionsTotal++
			//nodeDetails.Add(address.Hex(), nodeAddDetail)
			//qualification.CommitHeight2NodeDetailSetCache.Set(commitHeight-uint64(i), nodeDetails, *db)
			quorum.CommitHeightToConsensusQuorum.Set(commitHeight+uint64(i), commitQuorum, *db)
			nodeAddress, _ := quorum.CommitHeightToConsensusQuorum.Get(commitHeight+uint64(i), *db)
			log.Info("consensusNodeUpdate加入完成目前成员有", "成员", nodeAddress.KeysOrdered(), "高度", commitHeight)
		}
	}

	return nil, nil
}
