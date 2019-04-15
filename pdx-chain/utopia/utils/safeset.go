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
	"encoding/json"
	"errors"
	"sort"
	"sync"
)

var (
	ErrClosed     = errors.New("object set is closed")
	ErrAlreadySet = errors.New("object is already set")
	ErrNotSet     = errors.New("object is not set")
)

// SafeSet represents the collection of active objects.
type SafeSet struct {
	hmap map[string]interface{}
	lock sync.RWMutex
}

// NewSafeSet creates a new set to track the active objects.
func NewSafeSet() *SafeSet {
	return &SafeSet{
		hmap: make(map[string]interface{}),
	}
}

// Add injects a new object into the working set, or returns an error if the
// object is already known.
func (ps *SafeSet) Add(id string, p interface{}) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	if _, ok := ps.hmap[id]; ok {
		return ErrAlreadySet
	}
	ps.hmap[id] = p

	return nil
}

// Del removes a object from the active set.
func (ps *SafeSet) Del(id string) error {
	ps.lock.Lock()
	defer ps.lock.Unlock()

	_, ok := ps.hmap[id]
	if !ok {
		return ErrNotSet
	}
	delete(ps.hmap, id)

	return nil
}

// Get retrieves the registered object with the given id.
func (ps *SafeSet) Get(id string) interface{} {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	val, ok := ps.hmap[id]

	if ok {
		return val
	}

	return nil
}

// Len returns if the current number of objects in the set.
func (ps *SafeSet) Len() int {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	return len(ps.hmap)
}

func (ps *SafeSet) Keys() []string {
	ps.lock.RLock()
	defer ps.lock.RUnlock()
	keys := make([]string, 0)
	for k, _ := range ps.hmap {
		keys = append(keys, k)
	}

	return keys
}

func (ps *SafeSet) KeysOrdered() []string {
	keys := ps.Keys()

	sort.Strings(keys)

	return keys
}

func (ps *SafeSet) Copy() *SafeSet {
	ps.lock.RLock()
	defer ps.lock.RUnlock()

	set := NewSafeSet()
	for k, _ := range ps.hmap {
		val := ps.Get(k)
		if val != nil {
			set.Add(k, val)
		}
	}

	return set
}

func (ps *SafeSet) Encode() ([]byte, error) {
	return json.Marshal(ps.hmap)
}

func (ps *SafeSet) Decode(data []byte) error {
	return json.Unmarshal(data, &ps.hmap)
}
