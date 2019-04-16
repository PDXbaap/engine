package vm

import (
	"encoding/json"
	"errors"
	"pdx-chain/log"
	"pdx-chain/utopia/utils/whitelist"
)

const (
	Add = 0
	Del = 1
)

type TrustTxWhiteList struct {
}

func (t *TrustTxWhiteList) RequiredGas(input []byte) uint64 {
	return 0
}

type WhiteList struct {
	Timestamp int64    `json:"timestamp"`
	Random    string   `json:"random"`
	TcAdmCrt  string   `json:"tcadmCrt"`
	Nodes     []string `json:"nodes"`
	Type      int      `json:"type"`
}

func (t *TrustTxWhiteList) Run(ctx *PrecompiledContractContext, input []byte, extra map[string][]byte) ([]byte, error) {
	log.Info("Trust-Tx-White-List run")

	var payload WhiteList
	err := json.Unmarshal(input, &payload)
	if err != nil {
		log.Error("unmarshal payload error", "err", err)
		return nil, err
	}

	if len(payload.Nodes) == 0 {
		return nil, nil
	}

	switch payload.Type {
	case Add:
		err = whitelist.AddWhite(payload.Nodes)
	case Del:
		err = whitelist.DelWhite(payload.Nodes)
	default:
		log.Error("type is error")
		err = errors.New("type is error")
	}

	return nil, err
}
