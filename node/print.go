package node

import (
	"fmt"
	"math/big"

	"github.com/chord-dht/chord-core/tools"
)

type NodeState struct {
	Info               NodeInfo     `json:"info"`
	Predecessor        *NodeInfo    `json:"predecessor"`
	Successors         NodeInfoList `json:"successors"`
	FingerTable        NodeInfoList `json:"fingerTable"`
	FingerIndex        []*big.Int   `json:"fingerIndex"`
	LocalStorageName   []string     `json:"localStorageName"`
	BackupStoragesName [][]string   `json:"backupStoragesName"`
}

func (node *Node) GetState() *NodeState {
	return &NodeState{
		Info:               node.info,
		Predecessor:        node.GetPredecessor(),
		Successors:         node.GetSuccessors(),
		FingerTable:        node.GetFingerTable(),
		FingerIndex:        node.fingerIndex,
		LocalStorageName:   node.GetFilesName(),
		BackupStoragesName: node.GetAllBackupFilesName(),
	}
}

// Print the node information.
// If the node information is empty, print "Empty".
func (nodeInfo *NodeInfo) PrintInfo() {
	if nodeInfo.Empty() {
		fmt.Println("Empty")
		return
	}
	fmt.Printf(
		"Identifier: %s, IP Address: %s, Port: %s\n",
		nodeInfo.Identifier.String(),
		nodeInfo.IpAddress,
		nodeInfo.Port,
	)
}

func printFile(filename string) {
	fmt.Printf("Identifier: %s, filename: %s\n", tools.GenerateIdentifier(filename).String(), filename)
}

// PrintState prints the state (all information) of the node.
func (nodeState *NodeState) PrintState() {
	fmt.Println("Self:")
	fmt.Printf("  ")
	nodeState.Info.PrintInfo()

	fmt.Println("Predecessor:")
	fmt.Printf("  ")
	nodeState.Predecessor.PrintInfo()

	fmt.Println("Successors:")
	for i, successor := range nodeState.Successors {
		fmt.Printf("  %d ", i)
		successor.PrintInfo()
	}
	fmt.Println("Finger Table:")
	for i, finger := range nodeState.FingerTable {
		fmt.Printf("  %d ", i)
		fmt.Printf("Node %s + 2^%d = %s ", nodeState.Info.Identifier.String(), i, nodeState.FingerIndex[i].String())
		finger.PrintInfo()
	}

	fmt.Println("Files:")
	if len(nodeState.LocalStorageName) == 0 {
		fmt.Println("  No file in the storage")
	}
	for _, filename := range nodeState.LocalStorageName {
		fmt.Printf("  ")
		printFile(filename)
	}

	fmt.Println("Backup Files:")
	for i, backupStorageName := range nodeState.BackupStoragesName {
		fmt.Printf("  %d: ", i)
		nodeState.Successors[i].PrintInfo()
		if len(backupStorageName) == 0 {
			fmt.Println("  No file in the storage")
		}
		for _, filename := range backupStorageName {
			fmt.Printf("  ")
			printFile(filename)
		}
	}
}
