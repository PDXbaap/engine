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
package examineSync

import "sync"

var IDAndBlockAddress IDAndAddress

type IDAndAddress struct {
	IDAndAddressMap map[string]string
	Lock sync.RWMutex
}

func NewIDAndAddress() IDAndAddress {
	return IDAndAddress{IDAndAddressMap:make(map[string]string)}
}

func (d *IDAndAddress) AddIDAndAddress(id ,address string)  {
	d.Lock.Lock()
	defer d.Lock.Unlock()
	d.IDAndAddressMap[id]=address

}



func (d *IDAndAddress) GetAddIDAndAddress(id string) string {
	d.Lock.RLock()
	defer d.Lock.RUnlock()
	return d.IDAndAddressMap[id]
}