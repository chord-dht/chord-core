package node

// Quit the node and do some cleaning work
func (node *Node) Quit() {
	// 1. stop the periodical tasks by closing the shutdown channel
	node.Close()
	// 2. notify the predecessor and successor
	node.notifyLeave()
	// we don't need to transfer the files to the successor,
	// because we have the backup mechanism,
	// the node's predecessor will send the files to the node's successors
}

// stop the periodical tasks by closing the shutdown channel
func (node *Node) Close() {
	select {
	case <-node.shutdownCh:
		// channel already closed, do nothing
	default:
		close(node.shutdownCh)
	}
}

// Notify the node's predecessor and successor it is leaving the ring.
// Only invoked by the quit function, and should close the listener before calling this function.
func (node *Node) notifyLeave() {
	// The method below don't have return value
	// notify the predecessor to update its successor list
	node.GetPredecessor().NotifyPredecessor()
	// notify the successor to update its predecessor, you can send your predecessor to it
	node.GetFirstSuccessor().NotifySuccessor(node.GetPredecessor())
}

// NotifySuccessorLeave : Notify the node that its successor is leaving
func (node *Node) NotifySuccessorLeave() {
	// for the node, its successor is leaving, this successor views the node as its predecessor
	// this successor won't give any Information to the node, instead, the node will should update the successor list itself
	indexOfFirstLiveSuccessor, err := node.findFirstLiveSuccessor()
	if err != nil {
		node.Close()
		return
	}
	_ = node.updateReplica(indexOfFirstLiveSuccessor)
}

// NotifyPredecessorLeave : Notify the node that its predecessor is leaving
func (node *Node) NotifyPredecessorLeave(predecessor *NodeInfo) {
	// for the node, its predecessor is leaving, this predecessor views the node as its successor
	// this predecessor will give its predecessor to the node, so the node can update its predecessor

	// and we need to check the predecessor
	if predecessor.LiveCheck() != nil {
		return
	}

	node.SetPredecessor(predecessor)
}

/*                             RPC Part                             */

// NotifyPredecessor A wrap of NotifySuccessorLeave method.
// Notify the predecessor that its successor is leaving.
// But this function is invoked locally, for the node itself, it's notifying the predecessor.
// Don't need return value.
func (nodeInfo *NodeInfo) NotifyPredecessor() {
	_ = nodeInfo.callRPC("NotifySuccessorLeaveRPC", &Empty{}, &Empty{})
}

// NotifySuccessorLeaveRPC : Notify the node that its successor is leaving
func (handler *RPCHandler) NotifySuccessorLeaveRPC(args *Empty, reply *Empty) error {
	// Empty reply, don't need the caller to wait for the reply,
	// so we can use the asyncHandleRPC function to handle the RPC logic
	asyncHandleRPC(func() {
		localNode.NotifySuccessorLeave()
	})
	return nil
}

// NotifySuccessor A wrap of NotifyPredecessorLeave method.
// Notify the successor that its predecessor is leaving.
// But this function is invoked locally, for the node itself, it's notifying the successor.
// Don't need return value.
func (nodeInfo *NodeInfo) NotifySuccessor(predecessor *NodeInfo) {
	_ = nodeInfo.callRPC("NotifyPredecessorLeaveRPC", predecessor, &Empty{})
}

// NotifyPredecessorLeaveRPC : Notify the node that its predecessor is leaving
func (handler *RPCHandler) NotifyPredecessorLeaveRPC(predecessor *NodeInfo, reply *Empty) error {
	asyncHandleRPC(func() {
		localNode.NotifyPredecessorLeave(predecessor)
	})
	return nil
}

/*                             RPC Part                             */
