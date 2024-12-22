package node

// GetLength A wrap of GetLengthRPC method, call it and return the reply and error originally
func (nodeInfo *NodeInfo) GetLength() (*GetLengthReply, error) {
	reply := &GetLengthReply{}
	err := nodeInfo.callRPC("GetLengthRPC", &Empty{}, reply)
	return reply, err
}

// GetLengthRPC : get the node's Info
func (handler *RPCHandler) GetLengthRPC(args *Empty, reply *GetLengthReply) error {
	reply.IdentifierLength = localNode.identifierLength
	reply.SuccessorsLength = localNode.successorsLength
	return nil
}

// GetNodeInfo A wrap of GetInfoRPC method, call it and return the reply and error originally
func (nodeInfo *NodeInfo) GetNodeInfo() (*NodeInfo, error) {
	reply := &NodeInfo{}
	err := nodeInfo.callRPC("GetInfoRPC", &Empty{}, reply)
	return reply, err
}

// GetInfoRPC : get the node's Info
func (handler *RPCHandler) GetInfoRPC(args *Empty, reply *NodeInfo) error {
	*reply = localNode.info
	return nil
}

// GetPredecessor A wrap of GetPredecessorRPC method, call it and return the reply and error originally
func (nodeInfo *NodeInfo) GetPredecessor() (*NodeInfo, error) {
	reply := &NodeInfo{}
	err := nodeInfo.callRPC("GetPredecessorRPC", &Empty{}, reply)
	return reply, err
}

// GetPredecessorRPC : get the node's predecessor
func (handler *RPCHandler) GetPredecessorRPC(args *Empty, reply *NodeInfo) error {
	*reply = *localNode.GetPredecessor()
	return nil
}

// GetSuccessors A wrap of GetSuccessorsRPC method, call it and return the reply and error originally
func (nodeInfo *NodeInfo) GetSuccessors() (NodeInfoList, error) {
	reply := NodeInfoList{}
	err := nodeInfo.callRPC("GetSuccessorsRPC", &Empty{}, &reply)
	return reply, err
}

// GetSuccessorsRPC : get the node's successors
func (handler *RPCHandler) GetSuccessorsRPC(args *Empty, reply *NodeInfoList) error {
	*reply = localNode.GetSuccessors()
	return nil
}
