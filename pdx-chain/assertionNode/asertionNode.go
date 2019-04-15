package assertionNode

import (
	"pdx-chain/log"
	"sync"
)


var AssertionNode assertionNode

type assertionNode struct {
	assertionNodeMap map[uint64][]string
	lock sync.RWMutex
}

func NewAssertionNode()assertionNode {
	return assertionNode{assertionNodeMap:make(map[uint64][]string)}

}


func (n assertionNode)AddNode(nodeID string,commitHeight uint64)  {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.assertionNodeMap[commitHeight]=append(n.assertionNodeMap[commitHeight],nodeID)
}

func (n assertionNode)ContainNode(commitHeight uint64,node string)bool  {
	n.lock.Lock()
	defer n.lock.Unlock()
	for _,nodeId:=range n.assertionNodeMap[commitHeight]{
		log.Info("nodeId","nodeId",nodeId,"node传入",node)
		if nodeId==node{
			return false
		}

	}
	return true
}
