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
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"testing"
)

func TestPdxDb(t *testing.T) {

	db, err := leveldb.OpenFile("/home/bochenlong/ethgo/data/pdxc/mycc_his", nil)
	if err != nil {
		fmt.Println(err.Error())
	}

	iter := db.NewIterator(nil, nil)
	//iter := db.NewIterator(&util.Range{Start: []byte("student:1"), Limit: []byte("student:4")}, nil)
	for iter.Next() {
		// Remember that the contents of the returned slice should not be modified, and
		// only valid until the next call to Next.
		key := iter.Key()
		value := iter.Value()
		fmt.Println(string(key), string(value))
	}

	//for ok := iter.Seek([]byte("student:1")); ok; ok = iter.Next() {
	//	key := iter.Key()
	//	value := iter.Value()
	//	fmt.Println(string(key), string(value))
	//}
	iter.Release()
	err = iter.Error()
}
