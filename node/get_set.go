package node

func (node *Node) GetInfo() *NodeInfo {
	return &node.info
}

// We don't provide SetInfo method because the node's Info should not be changed after the node is created.

// GetPredecessor : get the node's predecessor
func (node *Node) GetPredecessor() *NodeInfo {
	node.muPre.RLock()
	defer node.muPre.RUnlock()
	return node.predecessor
}

func (node *Node) SetPredecessor(predecessor *NodeInfo) {
	node.muPre.Lock()
	defer node.muPre.Unlock()
	node.predecessor = predecessor
}

// GetSuccessors : get the node's successors
func (node *Node) GetSuccessors() NodeInfoList {
	node.muSuc.RLock()
	defer node.muSuc.RUnlock()
	return node.successors
}

// SetSuccessors : set the node's successors
func (node *Node) SetSuccessors(successors NodeInfoList) {
	node.muSuc.Lock()
	defer node.muSuc.Unlock()
	node.successors = successors
}

// GetSuccessor : get the node's successor by index
func (node *Node) GetSuccessor(index int) *NodeInfo {
	node.muSuc.RLock()
	defer node.muSuc.RUnlock()
	return node.successors[index]
}

// GetFirstSuccessor : get the first successor (index 0)
// It is specially designed for the first successor to boost the performance.
// As the first successor is the most frequently used one, we provide a special method for it.
func (node *Node) GetFirstSuccessor() *NodeInfo {
	node.muSuc.RLock()
	defer node.muSuc.RUnlock()
	return node.successors[0]
}

// SetSuccessor : set the node's successor by index
func (node *Node) SetSuccessor(index int, successor *NodeInfo) {
	node.muSuc.Lock()
	defer node.muSuc.Unlock()
	node.successors[index] = successor
}

// SetFirstSuccessor : set the first successor (index 0)
// It is specially designed for the first successor to boost the performance.
// As the first successor is the most frequently used one, we provide a special method for it.
func (node *Node) SetFirstSuccessor(successor *NodeInfo) {
	node.muSuc.Lock()
	defer node.muSuc.Unlock()
	node.successors[0] = successor
}

// GetFingerEntry : get the node's finger table entry
func (node *Node) GetFingerEntry(index int) *NodeInfo {
	node.muFin.RLock()
	defer node.muFin.RUnlock()
	return node.fingerTable[index]
}

// SetFingerEntry : set the node's finger table entry by index
func (node *Node) SetFingerEntry(index int, fingerEntry *NodeInfo) {
	node.muFin.Lock()
	defer node.muFin.Unlock()
	node.fingerTable[index] = fingerEntry
}

func (node *Node) GetFingerTable() NodeInfoList {
	node.muFin.RLock()
	defer node.muFin.RUnlock()
	return node.fingerTable
}

// We don't provide SetFingertable method because we won't set the whole finger table at once.
