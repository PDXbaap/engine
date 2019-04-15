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
package utils

import (
	"fmt"
	"pdx-chain/common"
	"pdx-chain/pdxcc/conf"
	"pdx-chain/pdxcc/util"
	"pdx-chain/utopia"
)

const (
	DepositContract  = "x-chain-transfer-deposit"
	WithdrawContract = "x-chain-transfer-withdraw"
	TcUpdater        = "tcUpdater"
	TrustTxWhiteList = "trust-tx-white-list"
	BaapTrustTree    = ":baap-trusttree:v1.0"
	NodeUpdate       = "consensus-node-update"
)

//DepositKeyHash store user has completed deposit tx
func DepositKeyHash(from common.Address, txhash common.Hash) common.Hash {
	key := fmt.Sprintf("%s:%s:%s:%s", conf.PDXKeyFlag, "deposit", from.String(), txhash.String())
	return util.EthHash([]byte(key))
}

//addr of stateDB for storing tx that user has withdrew
func WithdrawTxKeyHash(from common.Address) common.Hash {
	return util.EthHash([]byte(fmt.Sprintf("withdraw_txs:%s", from.String())))
}

//pdxc whether all tx is free
func IsFreeGas() bool {
	return utopia.Config.Bool("gasless")
}

//pdxc whether tx is sent to deposit address
func IsDeposit(to *common.Address) bool {
	if to != nil && *to == util.EthAddress(DepositContract) {
		return true
	}
	return false
}

func IsNodeUpdate(to *common.Address) bool {
	if to != nil && *to == util.EthAddress(NodeUpdate) {
		return true
	}
	return false
}

//set particular tx is free
func IsFreeTx(to *common.Address) bool {
	if IsDeposit(to) ||
		IsTcUpdater(to) ||
		IsBaapTrustTree(to) ||
		IsNodeUpdate(to) ||
		IsTrustTxWhiteList(to) {
		return true
	}
	return false
}

func IsTcUpdater(to *common.Address) bool {
	if to != nil && *to == util.EthAddress(TcUpdater) {
		return true
	}
	return false
}

func IsTrustTxWhiteList(to *common.Address) bool {
	if to != nil && *to == util.EthAddress(TrustTxWhiteList) {
		return true
	}
	return false
}

func IsBaapTrustTree(to *common.Address) bool {
	if to != nil && *to == util.EthAddress(BaapTrustTree) {
		return true
	}
	return false
}

//pdxc whether tx is sent to withdraw address
func IsWithdraw(to *common.Address) bool {
	if to != nil && *to == util.EthAddress(WithdrawContract) {
		return true
	}
	return false
}

func DepositContractAddr() common.Address {
	return util.EthAddress(DepositContract)
}
