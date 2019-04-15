package public

import "pdx-chain/core/types"

var BC PublicBlockChain

type PublicBlockChain interface {
	CurrentCommit() *types.Block
}
