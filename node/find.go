package node

import (
	"chord/log"
	"chord/tools"
	"fmt"
	"math/big"
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
	defer log.LogFunction()()

	found := false
	nextNode := nodeInfo // start from itself

	for i := 0; !found && i < maxSteps; i++ {
		log.Info("Step %d: Execute %v.find_successor(%v)", i, nextNode, identifier)
		reply, err := nextNode.FindSuccessor(identifier)
		if err != nil {
			log.Error("%v.FindSuccessor(%v) failed", nextNode, identifier)
			return nil, err
		}
		found = reply.Found
		nextNode = &reply.NodeInfo
		log.Info("Step %d: FindSuccessor reply: found = %t, nextNode = %v", i, found, nextNode)
	}
	if found {
		log.Info("Successor is found: %v", nextNode)
		return nextNode, nil
	} else {
		log.Info("maxSteps reached, nextNode now is %v, but the successor is not found", nextNode)
		return nil, fmt.Errorf("failed to findSuccessorIter the successor within maxSteps")
	}
}

// FindSuccessor : asks the node to find the successor of the identifier
func (node *Node) FindSuccessor(identifier *big.Int) (bool, *NodeInfo) {
	log.Info("%v.find_successor(%v)", node.info, identifier)
	// id is in (n, successor)
	successor := node.GetFirstSuccessor()
	if tools.ModIntervalCheck(identifier, node.info.Identifier, successor.Identifier, false, true) {
		log.Info("%s is in (%v, %v], find the successor!", identifier, node.info, successor)
		return true, successor
	} else {
		log.Info("%v is not in (%v, %v], go to %v.closestPrecedingNode(%v)", identifier, node.info, successor, node.info, identifier)
		return false, node.closestPrecedingNode(identifier)
	}
}

// Search the local table for highest predecessor of the identifier.
func (node *Node) closestPrecedingNode(identifier *big.Int) *NodeInfo {
	defer log.LogFunction()()

	// first search in the local finger table
	log.Info("Search in the local finger table")
	fingerEntry := node.findNearestNodeInFingers(identifier)
	log.Info("The fingerEntry is %v", fingerEntry)

	// also search the successor list for the most immediate predecessor of id, which is the fingerEntry
	successors, err := fingerEntry.GetSuccessors()
	if err != nil {
		log.Error("Failed to get the fingerEntry's successors")
		return fingerEntry
	}

	// then search in the fingerEntry's successors
	log.Info("Search in the fingerEntry's successors")
	successorEntry := fingerEntry.findNearestNode(identifier, successors)
	log.Info("The successorEntry is %v", successorEntry)

	return successorEntry
}

// Specially designed for the finger table, to ensure we read one of them a time.
// For simplicity, you may choose to read all of them and them process them.
func (node *Node) findNearestNodeInFingers(identifier *big.Int) *NodeInfo {
	defer log.LogFunction()()

	for i := node.identifierLength - 1; i >= 0; i-- {
		finger := node.GetFingerEntry(i)
		if finger.Empty() {
			log.Info("finger[%d] is empty", i)
			continue
		}
		if !tools.ModIntervalCheck(finger.Identifier, node.info.Identifier, identifier, false, false) {
			log.Info("finger[%d]: %v is not in (%v, %v]", i, finger, node.info, identifier)
			continue
		}
		// finger is in (n, id)
		log.Info("finger[%d]: %v is in (%v, %v], return this entry", i, finger, node.info, identifier)
		return finger
	}
	log.Info("No nearest node found, return %v itself", node.info)
	return &node.info
}

// Find the nearest node in the nodeList to the identifier.
// Only used in the closestPrecedingNode function.
func (nodeInfo *NodeInfo) findNearestNode(identifier *big.Int, nodeList NodeInfoList) *NodeInfo {
	defer log.LogFunction()()

	for i := len(nodeList) - 1; i >= 0; i-- {
		if nodeList[i].Empty() {
			log.Info("nodeList[%d] is empty", i)
			continue
		}
		if !tools.ModIntervalCheck(nodeList[i].Identifier, nodeInfo.Identifier, identifier, false, false) {
			log.Info("nodeList[%d]: %v is not in (%v, %v]", i, nodeList[i], nodeInfo, identifier)
			continue
		}
		// nodeList[i] is in (n, id)
		log.Info("nodeList[%d]: %v is in (%v, %v], return this entry", i, nodeList[i], nodeInfo, identifier)
		return nodeList[i]
	}
	log.Info("No nearest node found, return %v itself", nodeInfo)
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
	defer log.LogFunction()()
	found, nodeInfo := localNode.FindSuccessor(identifier)
	reply.Found = found
	reply.NodeInfo = *nodeInfo
	return nil
}

/*                             RPC Part                             */
