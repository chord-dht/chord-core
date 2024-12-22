package node

import (
	"fmt"
	"time"
)

// Initialize begins the node, create or join
func (node *Node) Initialize(mode, joinAddress, joinPort string) error {
	switch mode {
	case "create":
		node.create()
	case "join":
		if err := node.joinRing(joinAddress, joinPort); err != nil {
			return fmt.Errorf("joinRing failed, error: %v", err)
		}
	}

	// register it in rpc and start the server
	if err := node.startServer(); err != nil {
		return fmt.Errorf("startServer failed, error: %v", err)
	}

	// start the periodic tasks
	node.StartPeriodicTasks()

	return nil
}

// Create a new ring.
func (node *Node) create() {
	// predecessor = nil
	// successor = node itself
	node.SetFirstSuccessor(&node.info)
}

func (node *Node) joinRing(joinAddress, joinPort string) error {
	// get full Info of join node
	joinNode := NewNodeInfoWithAddress(joinAddress, joinPort)
	joinNode, err := joinNode.GetNodeInfo()
	if err != nil {
		return fmt.Errorf("try to get join node Info failed, error: %v", err)
	}

	// They should have the same IdentifierLength and SuccessorsLength
	// Otherwise, the join operation will fail
	reply, err := joinNode.GetLength()
	if err != nil {
		return fmt.Errorf("try to get join node length failed, error: %v", err)
	}
	if reply.IdentifierLength != node.identifierLength || reply.SuccessorsLength != node.successorsLength {
		return fmt.Errorf("the join node has different IdentifierLength or SuccessorsLength")
	}

	// join the chord ring
	if err := node.join(joinNode); err != nil {
		return fmt.Errorf("join Chord Ring failed, error: %v", err)
	}

	return nil
}

// Join an existing Chord ring containing node n' (joinNode).
func (node *Node) join(joinNode *NodeInfo) error {
	// predecessor = nil
	// successor = n'.find_successor(n)
	nodeInfo, err := joinNode.FindSuccessorIter(node.info.Identifier)
	if err != nil {
		return fmt.Errorf("%v.find_successor(%v) failed, error: %v", joinNode, node.info, err)
	}
	if err := nodeInfo.LiveCheck(); err != nil {
		return fmt.Errorf("%v.find_successor(%v) has bad result: %v", joinNode, node.info, err)
	}

	node.SetFirstSuccessor(nodeInfo)
	return nil
}

func (node *Node) StartPeriodicTasks() {
	go node.periodicStabilize(node.stabilizeTime)
	go node.periodicFixFingers(node.fixFingersTime)
	go node.periodicCheckPredecessor(node.checkPredecessorTime)

	// Sleep for a duration to allow periodic tasks to stabilize
	time.Sleep(5 * time.Second) // Adjust the duration as needed
}

func (node *Node) periodicStabilize(stabilizeTime time.Duration) {
	ticker := time.NewTicker(stabilizeTime * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			node.stabilize()
		case <-node.shutdownCh:
			ticker.Stop()
			return
		}
	}
}

func (node *Node) periodicFixFingers(fixFingersTime time.Duration) {
	ticker := time.NewTicker(fixFingersTime * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			node.fixFingers()
		case <-node.shutdownCh:
			ticker.Stop()
			return
		}
	}
}

func (node *Node) periodicCheckPredecessor(checkPredecessorTime time.Duration) {
	ticker := time.NewTicker(checkPredecessorTime * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			node.checkPredecessor()
		case <-node.shutdownCh:
			ticker.Stop()
			return
		}
	}
}
