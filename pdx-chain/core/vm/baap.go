package vm

import (
	"pdx-chain/log"
	"pdx-chain/params"
	"pdx-chain/pdxcc"
	"pdx-chain/pdxcc/conf"
	"pdx-chain/pdxcc/util"
)

type BaapConnector struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *BaapConnector) RequiredGas(input []byte) uint64 {
	return uint64(len(input)/192) * params.Bn256PairingPerPointGas
}

func (c *BaapConnector) Run(ctx *PrecompiledContractContext, input []byte, extra map[string][]byte) ([]byte, error) {
	log.Info("baap connector enter")

	stateFunc := func(fcn string, key string, value []byte) []byte {
		switch fcn {
		case conf.GetETH:
			return getState(ctx, key)
		case conf.GetPDX:
			return getPDXState(ctx, key)
		case conf.SetETH:
			setState(ctx, key, value)
			return nil
		case conf.SetPDX:
			setPDXState(ctx, key, value)
			return nil
		default:
			return nil
		}
	}

	extra[conf.BaapDst] = ctx.Contract.Address().Bytes()

	caller := ctx.Contract.CallerAddress
	err := pdxcc.Apply(caller, extra, input, stateFunc)

	return nil, err
}

func setState(ctx *PrecompiledContractContext, key string, value []byte) {
	keyHash := util.EthHash([]byte(key + conf.ETHKeyFlag))
	ctx.Evm.StateDB.SetState(ctx.Contract.Address(), keyHash, util.EthHash(value))
}

func setPDXState(ctx *PrecompiledContractContext, key string, value []byte) {
	keyHash := util.EthHash([]byte(key + conf.PDXKeyFlag))
	ctx.Evm.StateDB.SetPDXState(ctx.Contract.Address(), keyHash, value)
}

func getState(ctx *PrecompiledContractContext, key string) []byte {
	keyHash := util.EthHash([]byte(key + conf.ETHKeyFlag))
	hash := ctx.Evm.StateDB.GetState(ctx.Contract.Address(), keyHash)

	return hash.Bytes()
}

func getPDXState(ctx *PrecompiledContractContext, key string) []byte {
	keyHash := util.EthHash([]byte(key + conf.PDXKeyFlag))
	value := ctx.Evm.StateDB.GetPDXState(ctx.Contract.Address(), keyHash)

	return value
}
