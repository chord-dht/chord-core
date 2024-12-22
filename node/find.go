package node

import (
	"fmt"
	"math/big"

	"github.com/chord-dht/chord-core/tools"
)

// maxSteps variable, used in findSuccessorIter (find_successor).
const maxSteps = 10

// Iterative implementation of the find_successor function, used as an entrance.
// Asks the node (nodeInfo) to FindSuccessorIter the successor of the identifier.
// Theoretically speaking, this function will not fail.
// But in practice, it may fail due to the network or other reasons.
//  1. return (empty NodeInfo, handleCall error) if handleCall (its warp) failed.
//  2. return (empty NodeInfo, custom error) if the successor is not found within maxSteps steps.
//  3. return (found NodeInfo, nil) if the successor is found.
func (nodeInfo *NodeInfo) FindSuccessorIter(identifier *big.Int) (*NodeInfo, error) {
	found := false
	nextNode := nodeInfo // start from itself

	for i := 0; !found && i < maxSteps; i++ {
		reply, err := nextNode.FindSuccessor(identifier)
		if err != nil {
			return nil, err
		}
		found = reply.Found
		nextNode = &reply.NodeInfo
	}
	if found {
		return nextNode, nil
	} else {
		return nil, fmt.Errorf("failed to findSuccessorIter the successor within maxSteps")
	}
}

// FindSuccessor : asks the node to find the successor of the identifier
func (node *Node) FindSuccessor(identifier *big.Int) (bool, *NodeInfo) {
	// id is in (n, successor)
	successor := node.GetFirstSuccessor()
	if tools.ModIntervalCheck(identifier, node.info.Identifier, successor.Identifier, false, true) {
		return true, successor
	} else {
		return false, node.closestPrecedingNode(identifier)
	}
}

// Search the local table for highest predecessor of the identifier.
func (node *Node) closestPrecedingNode(identifier *big.Int) *NodeInfo {
	// first search in the local finger table
	fingerEntry := node.findNearestNodeInFingers(identifier)

	// also search the successor list for the most immediate predecessor of id, which is the fingerEntry
	successors, err := fingerEntry.GetSuccessors()
	if err != nil {
		return fingerEntry
	}

	// then search in the fingerEntry's successors
	successorEntry := fingerEntry.findNearestNode(identifier, successors)

	return successorEntry
}

// Specially designed for the finger table, to ensure we read one of them a time.
// For simplicity, you may choose to read all of them and them process them.
func (node *Node) findNearestNodeInFingers(identifier *big.Int) *NodeInfo {
	for i := node.identifierLength - 1; i >= 0; i-- {
		finger := node.GetFingerEntry(i)
		if finger.Empty() {
			continue
		}
		if !tools.ModIntervalCheck(finger.Identifier, node.info.Identifier, identifier, false, false) {
			continue
		}
		// finger is in (n, id)
		return finger
	}
	return &node.info
}

// Find the nearest node in the nodeList to the identifier.
// Only used in the closestPrecedingNode function.
func (nodeInfo *NodeInfo) findNearestNode(identifier *big.Int, nodeList NodeInfoList) *NodeInfo {
	for i := len(nodeList) - 1; i >= 0; i-- {
		if nodeList[i].Empty() {
			continue
		}
		if !tools.ModIntervalCheck(nodeList[i].Identifier, nodeInfo.Identifier, identifier, false, false) {
			continue
		}
		// nodeList[i] is in (n, id)
		return nodeList[i]
	}
	return nodeInfo
}

/*                             RPC Part                             */

// FindSuccessor a wrap of FindSuccessorRPC method.
func (nodeInfo *NodeInfo) FindSuccessor(identifier *big.Int) (*FindSuccessorReply, error) {
	reply := &FindSuccessorReply{}
	err := nodeInfo.callRPC("FindSuccessorRPC", identifier, reply)
	return reply, err
}

// FindSuccessorRPC : asks the node to findSuccessorIter the successor of the identifier
func (handler *RPCHandler) FindSuccessorRPC(identifier *big.Int, reply *FindSuccessorReply) error {
	found, nodeInfo := localNode.FindSuccessor(identifier)
	reply.Found = found
	reply.NodeInfo = *nodeInfo
	return nil
}

/*                             RPC Part                             */
