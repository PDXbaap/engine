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
package chaincode

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"pdx-chain/log"
	"pdx-chain/pdxcc/conf"
)

type DeployInfo struct {
	FileId            string `json:"fileId"`
	FileName          string `json:"fileName"`
	FileHash          string `json:"fileHash"`
	Channel           string `json:"channel"`
	ChaincodeId       string `json:"chaincodeId"`
	ChaincodeAddress  string `json:"chaincodeAddress"`
	ChaincodeName     string `json:"chaincodeName"`
	ChaincodeNameHash string `json:"chaincodeNameHash "`
	Pbk               string `json:"pbk"`
	Desc              string `json:"desc"`
	Alias             string `json:"alias"`
	Path              string `json:"-"`
	Status            byte   `json:"status"` // ready 1 running 4 stop 5 error 6
	PId               int    `json:"-"`
	DeployTime        int64  `json:"deployTime"`
}

const (
	CC_READY   = 1
	CC_RUNNING = 4
	CC_STOP    = 5
	CC_ERROR   = 6
)

func Restart(chaincodeAddress string, info *DeployInfo, port string) bool {
	chaincodeAddress = strings.ToLower(chaincodeAddress)

	log.Info("!!!! enter restart")
	pid := isAlive(info.ChaincodeName)
	stopJar(pid)
	pid = runJar(info.Path, info.ChaincodeId, port)
	updatePid(pid, info.ChaincodeId, info.Channel)
	updateStatus(chaincodeAddress, CC_RUNNING, info.Channel)
	return true
}

//noinspection ALL
func Start(chaincodeAddress string, port, spbk string) bool {
	chaincodeAddress = strings.ToLower(chaincodeAddress)

	log.Info("!!!!! enter start")
	info := IsInDb(chaincodeAddress)
	if info == nil {
		return false
	}

	if info.Pbk != spbk {
		log.Warn(fmt.Sprintf("!!!!! sender is not deployer %s", spbk))
		return false
	}

	return start(chaincodeAddress, info, port)
}

func start(chaincodeAddress string, info *DeployInfo, port string) bool {

	if !(info.Status == CC_READY || info.Status == CC_STOP) {
		log.Warn(fmt.Sprintf("!!!!! status is %d", info.Status))
		return false
	}

	infos := selectByChaincodeName(info.Channel, info.ChaincodeName)

	for _, f := range infos {
		if f.Status == CC_RUNNING {
			log.Error(fmt.Sprintf("!!!!! already has chaincode %s running stop it", f.ChaincodeId))
			stop(f.ChaincodeAddress, f)
		}
	}

	updateStatus(chaincodeAddress, CC_RUNNING, info.Channel)

	return true
}

func Stop(chaincodeAddress string, spbk string) bool {
	chaincodeAddress = strings.ToLower(chaincodeAddress)
	log.Info("!!!!! enter stop")
	info := IsInDb(chaincodeAddress)
	if info == nil {
		return false
	}

	if info.Pbk != spbk {
		log.Warn(fmt.Sprintf("!!!!! sender is not deployer %s", spbk))
		return false
	}

	return stop(chaincodeAddress, info)
}

func stop(chaincodeAddress string, info *DeployInfo) bool {
	updateStatus(chaincodeAddress, CC_STOP, info.Channel)

	return true

}

func Withdraw(chaincodeAddress string, spbk string) bool {
	chaincodeAddress = strings.ToLower(chaincodeAddress)
	log.Info("!!!!! enter withdraw")
	info := IsInDb(chaincodeAddress)

	if info == nil {
		return false
	}

	if info.Pbk != spbk {
		log.Warn(fmt.Sprintf("!!!!! sender is not deployer %s", spbk))
		return false
	}

	if !(info.Status == CC_STOP || info.Status == CC_READY) {
		log.Warn(fmt.Sprintf("!!!!! chaincode status is %d", info.Status))
		return false
	}

	os.RemoveAll(conf.BaapHome + "/instance/" + info.Channel + "/" + info.ChaincodeId)
	deleteByAddress(chaincodeAddress)

	return true
}

func IsInDb(chaincodeAddress string) *DeployInfo {
	info := selectByAddress(chaincodeAddress)
	if info == nil || info.ChaincodeId == "" {
		log.Warn(fmt.Sprintf("!!!!! chaincode not in DB %s", chaincodeAddress))
		return nil
	} else {
		return info
	}
}

//noinspection ALL
func Query(queryType string, params ...interface{}) []*DeployInfo {
	db, err := initDb()
	if err != nil {
		return nil
	}
	defer db.Close()

	var sql string
	switch queryType {
	case "queryByPbk":
		sql = "SELECT chaincodeId,chaincodeName,chaincodeAddress,`desc`,alias,status,deployTime FROM deployinfo where pbk='" + params[0].(string) + "' order by deployTime desc"
		break
	case "queryAll":
		sql = "SELECT chaincodeId,chaincodeName,chaincodeAddress,`desc`,alias,status,deployTime FROM deployinfo where `status` in (1,4,5) order by deployTime desc"
		break
	case "queryByChaincodeId":
		sql = "SELECT chaincodeId,chaincodeName,chaincodeAddress,`desc`,alias,status,deployTime FROM deployinfo where chaincodeId ='" + strings.ToLower(params[0].(string)) + "' limit 1"
		break
	case "queryRunningCcByName":
		sql = "SELECT chaincodeId,chaincodeName,chaincodeAddress,`desc`,alias,status,deployTime FROM deployinfo where `status` = 4 and pbk='" + params[0].(string) + "' and chaincodeName='" + strings.ToLower(params[1].(string)) + "'"
		break
	default:
		return nil
	}
	rows, err := db.Query(sql)
	if err != nil {
		return nil
	}

	var infos []*DeployInfo
	for rows.Next() {
		var chaincodeId string
		var chaincodeName string
		var chaincodeAddress string
		var desc string
		var alias string
		var status byte
		var deployTime time.Time
		rows.Scan(&chaincodeId, &chaincodeName, &chaincodeAddress, &desc, &alias, &status, &deployTime)

		infos = append(infos, &DeployInfo{
			ChaincodeId:      chaincodeId,
			ChaincodeName:    chaincodeName,
			ChaincodeAddress: chaincodeAddress,
			Desc:             desc,
			Alias:            alias,
			Status:           status,
			DeployTime:       deployTime.Unix() * 1000,
		})
	}
	return infos
}

func initDb() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", conf.DbPath+"/.baap.db")
	if err != nil {
		log.Warn(err.Error())
		return nil, err
	}
	return db, nil
}

//noinspection ALL
func updatePid(pid string, chaincodeId string, channel string) bool {
	db, err := initDb()
	if err != nil {
		return false
	}
	defer db.Close()
	stmt, err := db.Prepare("UPDATE deployinfo SET pid=? WHERE chaincodeId=? AND channel=?")
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	defer stmt.Close()

	res, err := stmt.Exec(pid, chaincodeId, channel)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	affect, err := res.RowsAffected()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	if affect < 1 {
		fmt.Println("!!!!! update fail")
		return false
	}
	fmt.Println(fmt.Sprintf("update result : %d", affect))
	return true
}

//noinspection ALL
func selectByAddress(chaincodeAddress string) *DeployInfo {
	log.Info(fmt.Sprintf("!!!!! select by address %s", chaincodeAddress))
	db, err := initDb()
	if err != nil {
		return nil
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT fileId,fileName,fileHash,channel,chaincodeId,chaincodeAddress,chaincodeName,chaincodeNameHash,pbk,`desc`,alias,path,status,pId FROM deployinfo WHERE chaincodeAddress = ? LIMIT 1")
	if err != nil {
		return nil
	}

	defer stmt.Close()
	var info DeployInfo

	stmt.QueryRow(chaincodeAddress).Scan(&info.FileId, &info.FileName, &info.FileHash, &info.Channel, &info.ChaincodeId, &info.ChaincodeAddress, &info.ChaincodeName, &info.ChaincodeNameHash,
		&info.Pbk, &info.Desc, &info.Alias, &info.Path, &info.Status, &info.PId)
	return &info
}

func selectByChaincodeName(channel string, chaincodeName string) []*DeployInfo {
	log.Info(fmt.Sprintf("!!!!! select by chaincodeName %s", chaincodeName))
	db, err := initDb()
	if err != nil {
		return nil
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT fileId,fileName,fileHash,channel,chaincodeId,chaincodeAddress,chaincodeName,chaincodeNameHash,pbk,`desc`,alias,path,status,pId FROM deployInfo WHERE channel = ? AND chaincodeName = ?")
	if err != nil {
		log.Error(err.Error())
		return nil
	}

	defer stmt.Close()
	rows, _ := stmt.Query(channel, chaincodeName)

	var infos []*DeployInfo
	for rows.Next() {
		info := &DeployInfo{}
		rows.Scan(&info.FileId, &info.FileName, &info.FileHash, &info.Channel, &info.ChaincodeId, &info.ChaincodeAddress, &info.ChaincodeName, &info.ChaincodeNameHash,
			&info.Pbk, &info.Desc, &info.Alias, &info.Path, &info.Status, &info.PId)

		infos = append(infos, info)
	}
	return infos
}

//noinspection ALL
func updateStatus(chaincodeAddress string, status byte, channel string) bool {
	log.Info(fmt.Sprintf("!!!!! update deploy info %s --> %d", chaincodeAddress, status))
	db, err := initDb()
	if err != nil {
		return false
	}
	defer db.Close()
	stmt, err := db.Prepare("UPDATE deployinfo SET status=? WHERE chaincodeAddress=? and channel=?")
	if err != nil {
		log.Error(err.Error())
		return false
	}

	defer stmt.Close()

	res, err := stmt.Exec(status, strings.ToLower(chaincodeAddress), channel)
	if err != nil {
		log.Error(err.Error())
		return false
	}
	affect, err := res.RowsAffected()
	if err != nil {
		log.Error(err.Error())
		return false
	}
	if affect < 1 {
		log.Error("!!!!! update fail")
		return false
	}
	log.Info(fmt.Sprintf("!!!!! update result : %d", affect))
	return true
}

//noinspection ALL
func deleteByAddress(chaincodeAddress string) bool {
	log.Info(fmt.Sprintf("!!!!! delete deploy info %s", chaincodeAddress))

	db, err := initDb()
	if err != nil {
		return false
	}
	defer db.Close()
	stmt, err := db.Prepare("DELETE FROM deployinfo WHERE chaincodeAddress=?")
	if err != nil {
		log.Error(err.Error())
		return false
	}

	defer stmt.Close()

	res, err := stmt.Exec(strings.ToLower(chaincodeAddress))
	if err != nil {
		log.Error(err.Error())
		return false
	}
	affect, err := res.RowsAffected()
	if err != nil {
		log.Error(err.Error())
		return false
	}
	if affect < 1 {
		log.Warn("!!!!! delete fail")
		return false
	}
	log.Info(fmt.Sprintf("!!!!! delete result : %d", affect))
	return true
}

func save(deployInfo *DeployInfo) bool {
	log.Info(fmt.Sprintf("!!!!! save deploy info : %s", deployInfo.ChaincodeId))
	db, err := initDb()
	if err != nil {
		return false
	}
	defer db.Close()

	stmt, err := db.Prepare("INSERT INTO deployinfo(fileId,fileName,fileHash,channel,chaincodeId,chaincodeName,chaincodeNameHash,chaincodeAddress,pbk,`desc`,alias,path,status,deployTime) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
	if err != nil {
		log.Error(err.Error())
		return false
	}
	defer stmt.Close()

	res, err := stmt.Exec(deployInfo.FileId, deployInfo.FileName, deployInfo.FileHash, deployInfo.Channel, deployInfo.ChaincodeId, deployInfo.ChaincodeName, deployInfo.ChaincodeNameHash, deployInfo.ChaincodeAddress, deployInfo.Pbk, deployInfo.Desc, deployInfo.Alias, deployInfo.Path, deployInfo.Status, deployInfo.DeployTime)
	if err != nil {
		log.Error(err.Error())
		return false
	}

	affect, err := res.RowsAffected()

	if err != nil {
		log.Error(err.Error())
		return false
	}

	if affect < 1 {
		log.Error("!!!!! save fail")
		return false
	}

	return true
}
