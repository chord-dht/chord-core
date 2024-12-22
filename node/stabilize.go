package node

import (
	"chord/tools"
)

var next = 0

// Periodic Background task - stabilize.
func (node *Node) stabilize() {
	// update the successor list and backup files
	_ = node.updateReplica()

	// successor.notify(n)
	successor := node.GetFirstSuccessor()
	if err := successor.Notify(&node.info); err != nil {
		return
	}
}

// Periodic Background task - fixFingers.
func (node *Node) fixFingers() {
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

	tempResult, err := node.info.FindSuccessorIter(nextIdentifier)
	if err != nil {
		node.SetFingerEntry(next, NewNodeInfo())
		return
	}
	if err := tempResult.LiveCheck(); err != nil {
		node.SetFingerEntry(next, NewNodeInfo())
		return
	}
	node.SetFingerEntry(next, tempResult)
}

// Periodic Background task - checkPredecessor.
func (node *Node) checkPredecessor() {
	oldPredecessor := node.GetPredecessor()

	if err := oldPredecessor.LiveCheck(); err != nil {
		node.SetPredecessor(NewNodeInfo())
		return
	}
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
	predecessor := node.GetPredecessor()
	// self check: if the predecessor is itself, then do nothing
	if predecessor.Identifier.Cmp(node.info.Identifier) == 0 {
		return
	}

	if oldPredecessor.Empty() || oldPredecessor.LiveCheck() != nil {
		// if the oldPredecessor is nil or not alive, then do nothing
		return
	}

	// first extract the chosen files
	extractFileList, err := node.ExtractFilesByFilter(func(filename string) bool {
		// if oldPredecessor is not nil, we select filename ID with (oldPredecessor, predecessor]
		return tools.ModIntervalCheck(tools.GenerateIdentifier(filename), oldPredecessor.Identifier, predecessor.Identifier, false, true)
	})
	if err != nil {
		// for this error, we don't need to return, we just log it and keep going on
		// it means that we lost some files due to the storage system
		// but we still need to keep on, as we need to send the rest of the files to the predecessor
	}

	// finally, we send the file list to the predecessor
	reply, err := predecessor.StoreFiles(extractFileList)
	if err != nil || !reply.Success {
		// for this error, we need to store these files back to the node's storage system again
		// so that when another notify comes, the node can transfer these files
		if err := node.StoreFiles(extractFileList); err != nil {
		}
		return
	}
}

/*                             RPC Part                             */

// Notify A wrap of NotifyRPC method
// Notify the node to check if it should be its predecessor
func (nodeInfo *NodeInfo) Notify(predecessor *NodeInfo) error {
	return nodeInfo.callRPC("NotifyRPC", predecessor, &Empty{})
}

// NotifyRPC node n is notified by n' (nodeInfo) to check if n' should be its predecessor
func (handler *RPCHandler) NotifyRPC(nodeInfo *NodeInfo, reply *Empty) error {
	asyncHandleRPC(func() {
		localNode.Notify(nodeInfo)
	})
	return nil
}

/*                             RPC Part                             */
