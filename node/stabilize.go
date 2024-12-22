package node

import (
	"chord/log"
	"chord/tools"
)

var next = 0

// Periodic Background task - stabilize.
func (node *Node) stabilize() {
	defer log.LogFunction()()

	// update the successor list and backup files
	_ = node.updateReplica()

	// successor.notify(n)
	successor := node.GetFirstSuccessor()
	log.Info("Execute %v.notify(%v)", successor, node.info)
	if err := successor.Notify(&node.info); err != nil {
		log.Error("Failed to notify the successor %v", successor)
		return
	}
}

// Periodic Background task - fixFingers.
func (node *Node) fixFingers() {
	defer log.LogFunction()()

	next++
	if next > node.identifierLength-1 {
		next = 0
	}
	// next \in [0, IdentifierLength-1]
	// the meaning of next is like i, not the real next
	// finger[next] = find_successor(n + 2^next)
	// finger[0] = find_successor(n + 2^0)
	// finger[1] = find_successor(n + 2^1)
	// ...
	// finger[IdentifierLength-1] = find_successor(n + 2^(IdentifierLength-1))
	nextIdentifier := node.fingerIndex[next]
	log.Info("Execute %v.find_successor(%v) for finger[%d]", node.info, nextIdentifier, next)

	tempResult, err := node.info.FindSuccessorIter(nextIdentifier)
	if err != nil {
		log.Error("%v.find_successor(%v) failed, error: %v", node.info, nextIdentifier, err)
		node.SetFingerEntry(next, NewNodeInfo())
		return
	}
	if err := tempResult.LiveCheck(); err != nil {
		log.Error("The result of %v.find_successor(%v): %v", node.info, nextIdentifier, err)
		node.SetFingerEntry(next, NewNodeInfo())
		return
	}
	log.Info("The result of %v.find_successor(%v) is %v", node.info, nextIdentifier, tempResult)
	node.SetFingerEntry(next, tempResult)
}

// Periodic Background task - checkPredecessor.
func (node *Node) checkPredecessor() {
	defer log.LogFunction()()

	oldPredecessor := node.GetPredecessor()

	if err := oldPredecessor.LiveCheck(); err != nil {
		log.Info("Predecessor: %v", err)
		node.SetPredecessor(NewNodeInfo())
		return
	}

	// the predecessor may changed, should get the predecessor again
	log.Info("Predecessor %v is still alive", node.GetPredecessor())
}

// NotifyLog : node n is notified by n' (nodeInfo) to check if n' should be its predecessor
// just for debugging, the real function is Notify function below
// Unused in the final version
func (node *Node) NotifyLog(nodeInfo *NodeInfo) {
	defer log.LogFunction()()

	// first we need to check the nodeInfo
	// but actually, we shoulde check it just before we set the predecessor
	// so we don't need to check if we don't need to set the predecessor, which is the normal case
	if err := nodeInfo.LiveCheck(); err != nil {
		log.Error("n' (nodeInfo): %v, do nothing", err)
		return
	}
	log.Info("%v.Notify(%v)", node.info, nodeInfo)

	// if oldPredecessor is nil or n' in (oldPredecessor, n)
	oldPredecessor := node.GetPredecessor()
	if oldPredecessor.Empty() {
		log.Info("Predecessor is empty")
		log.Info("Notify successfully should sets predecessor to %v", nodeInfo)
		node.SetPredecessor(nodeInfo)
	} else if tools.ModIntervalCheck(nodeInfo.Identifier, oldPredecessor.Identifier, node.info.Identifier, false, false) {
		log.Info("%v is in (%v, %v), should set the predecessor %v", nodeInfo, oldPredecessor, node.info, nodeInfo)
		node.SetPredecessor(nodeInfo)
	} else {
		log.Info("%v is not in (%v, %v), do nothing", nodeInfo, oldPredecessor, node.info)
		return // in this case, the predecessor is not changed, so we don't need to transfer files
	}

	// now the predecessor is set, the node should check its files, try to find the files that should be transferred to the new predecessor
	node.transferFilesToPredecessor(oldPredecessor)
}

// Notify : node n is notified by n' (nodeInfo) to check if n' should be its predecessor
func (node *Node) Notify(nodeInfo *NodeInfo) {
	oldPredecessor := node.GetPredecessor()
	// if oldPredecessor is nil or n' in (oldPredecessor, n)
	if oldPredecessor.Empty() || tools.ModIntervalCheck(nodeInfo.Identifier, oldPredecessor.Identifier, node.info.Identifier, false, false) {
		// before setting we need to check the nodeInfo
		if err := nodeInfo.LiveCheck(); err != nil {
			return
		}
		node.SetPredecessor(nodeInfo)
		// now the predecessor is set, the node should check its files, try to find the files that should be transferred to the new predecessor
		node.transferFilesToPredecessor(oldPredecessor)
	}
	// in this case, the predecessor is not changed, so we don't need to transfer files
}

// Helper function for Notify
// Transfer the chosen files.
// Only invoked by the Notify function.
func (node *Node) transferFilesToPredecessor(oldPredecessor *NodeInfo) {
	defer log.LogFunction()()

	predecessor := node.GetPredecessor()
	// self check: if the predecessor is itself, then do nothing
	if predecessor.Identifier.Cmp(node.info.Identifier) == 0 {
		log.Info("The predecessor is itself, do nothing")
		return
	}

	if oldPredecessor.Empty() || oldPredecessor.LiveCheck() != nil {
		// if the oldPredecessor is nil or not alive, then do nothing
		log.Info("The oldPredecessor is nil, do nothing")
		return
	}

	// first extract the chosen files
	extractFileList, err := node.ExtractFilesByFilter(func(filename string) bool {
		// if oldPredecessor is not nil, we select filename ID with (oldPredecessor, predecessor]
		return tools.ModIntervalCheck(tools.GenerateIdentifier(filename), oldPredecessor.Identifier, predecessor.Identifier, false, true)
	})
	if err != nil {
		log.Error("Failed to extract files: %v", err)
		// for this error, we don't need to return, we just log it and keep going on
		// it means that we lost some files due to the storage system
		// but we still need to keep on, as we need to send the rest of the files to the predecessor
	}

	// finally, we send the file list to the predecessor
	reply, err := predecessor.StoreFiles(extractFileList)
	if err != nil || !reply.Success {
		log.Error("Failed to StoreFileList: %v", err)
		// for this error, we need to store these files back to the node's storage system again
		// so that when another notify comes, the node can transfer these files
		if err := node.StoreFiles(extractFileList); err != nil {
			log.Error("Failed to store files back to the node's storage system: %v", err)
		}
		return
	}
	log.Info("Successfully transfer files to the predecessor %v", predecessor)
}

/*                             RPC Part                             */

// Notify A wrap of NotifyRPC method
// Notify the node to check if it should be its predecessor
func (nodeInfo *NodeInfo) Notify(predecessor *NodeInfo) error {
	return nodeInfo.callRPC("NotifyRPC", predecessor, &Empty{})
}

// NotifyRPC node n is notified by n' (nodeInfo) to check if n' should be its predecessor
func (handler *RPCHandler) NotifyRPC(nodeInfo *NodeInfo, reply *Empty) error {
	defer log.LogFunction()()
	asyncHandleRPC(func() {
		localNode.Notify(nodeInfo)
	})
	return nil
}

/*                             RPC Part                             */
