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


var PeerExamineSync ExamineSync

type ExamineSync struct {
	PeersID map[string]string
	Lock sync.RWMutex
}

func NewExamineSync() ExamineSync {
	return ExamineSync{PeersID:make(map[string]string)}
}

func (e *ExamineSync) AddPeer(peer string)  {
	e.Lock.Lock()
	defer e.Lock.Unlock()
	e.PeersID[peer]=peer
}

func (e *ExamineSync) DelPeer(peer string)()  {
	e.Lock.Lock()
	defer e.Lock.Unlock()
	delete(e.PeersID,peer)
}

func (e *ExamineSync) ExaminePeer(peer string) bool {
	e.Lock.Lock()
	defer e.Lock.Unlock()
	_,ok:=e.PeersID[peer]
	return ok
}