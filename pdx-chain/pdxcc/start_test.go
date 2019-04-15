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
	"crypto/elliptic"
	"fmt"
	"net"
	"net/url"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"pdx-chain/crypto"
)

const NodeIDBits = 512

type NodeID [NodeIDBits / 8]byte

func TestStart(t *testing.T) {
	key, err := crypto.HexToECDSA("00c26f660420119b13495f7bc930c75355a0ecec6fec46c1ee04ec7c38cbe03092")
	if err != nil {
		panic(err)
	}
	pub := key.PublicKey
	var id NodeID
	pbytes := elliptic.Marshal(pub.Curve, pub.X, pub.Y)
	if len(pbytes)-1 != len(id) {
		panic(fmt.Errorf("need %d bit pubkey, got %d bits", (len(id)+1)*8, len(pbytes)))
	}
	copy(id[:], pbytes[1:])

	fmt.Println(fmt.Sprintf("%x", id[:]))

	u := url.URL{Scheme: "enode"}
	addr := net.TCPAddr{IP: []byte("10.0.0.8"), Port: 3306}
	u.User = url.User(fmt.Sprintf("%x", id[:]))
	u.Host = addr.String()

	fmt.Println(u)
}
