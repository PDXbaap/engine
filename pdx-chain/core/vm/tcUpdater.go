package vm

import (
	"pdx-chain/params"
	"pdx-chain/utopia/utils/tcUpdate"
)

type TcUpdate struct{}

func (c *TcUpdate) RequiredGas(input []byte) uint64 {
	return uint64(len(input)/192) * params.Bn256PairingPerPointGas
}

func (c *TcUpdate) Run(ctx *PrecompiledContractContext, input []byte, extra map[string][]byte) ([]byte, error) {
	trustChainUpdTxModel, err := tcUpdate.TrustChainUpdTxModelDecode(input)
	if err != nil {
		return nil, err
	}
	tcUpdate.TrustHosts.WriteTrustChain(trustChainUpdTxModel.PrevChainID, trustChainUpdTxModel.ChainId, trustChainUpdTxModel.HostList)

	return nil, nil
}
