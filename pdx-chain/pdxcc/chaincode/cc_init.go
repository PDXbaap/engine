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
	//_ "github.com/mattn/go-sqlite3"
)

/*func init() {
	db, err := sql.Open("sqlite3", conf.DbPath+"/.baap.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	updateDB(db)
}*/

func updateDB(db *sql.DB) {
	if exist(db) == 0 {
		for _, s := range initSql() {
			execSql(s, db)
		}
	}
}

func exist(db *sql.DB) int {

	stmt, err := db.Prepare("SELECT count(*) AS num FROM sqlite_master WHERE type='table' AND name='DEPLOYINFO'")
	if err != nil {
		return 0
	}

	defer stmt.Close()

	var num int
	stmt.QueryRow().Scan(&num)

	return num
}

func execSql(sql string, db *sql.DB) {

	stmt, err := db.Prepare(sql)

	if err != nil {
		panic(err)
	}

	defer stmt.Close()

	res, err := stmt.Exec()

	if err != nil {
		panic(err)
	}

	_, err = res.RowsAffected()

	if err != nil {
		panic(err)
	}

}

func initSql() []string {
	return []string{
		"CREATE TABLE IF NOT EXISTS DEPLOYINFO (" +
			"fileId           	VARCHAR(50) NOT NULL," +
			"fileName         	VARCHAR(50)," +
			"fileHash         	VARCHAR(100)," +
			"channel          	VARCHAR(50) NOT NULL," +
			"chaincodeId      	VARCHAR(50) NOT NULL," +
			"chaincodeAddress 	VARCHAR(80)," +
			"chaincodeName    	VARCHAR(50)," +
			"chaincodeNameHash  VARCHAR(100)," +
			"pbk              	VARCHAR(80)," +
			"`desc`           	TEXT," +
			"alias            	VARCHAR(50)," +
			"path             	TEXT," +
			"status           	TINYINT," +
			"pId              	BIGINT," +
			"deployTime       	TIMESTAMP)",
		"CREATE INDEX index1 on DEPLOYINFO(chaincodeID)", "CREATE INDEX index2 on DEPLOYINFO(chaincodeName)",
	}
}
