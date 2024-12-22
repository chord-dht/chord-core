package node

import (
	"fmt"

	"github.com/chord-dht/chord-core/tools"
)

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

// Print the files' name in the node.
func (node *Node) printFilesname() {
	filesname := node.GetFilesName()
	if len(filesname) == 0 {
		fmt.Println("  No file in the storage")
	}
	for _, filename := range filesname {
		fmt.Printf("  ")
		printFile(filename)
	}
}

// Print the backup files' name in one of the backup storages.
func (node *Node) printBackupFilesname(index int) {
	filesname := node.GetBackupFilesName(index)
	if len(filesname) == 0 {
		fmt.Printf("    No file in the backup storage %d\n", index)
	}
	for _, filename := range filesname {
		fmt.Printf("    ")
		printFile(filename)
	}
}

// PrintState prints the state (all information) of the node.
func (node *Node) PrintState() {
	fmt.Println("Self:")
	fmt.Printf("  ")
	node.info.PrintInfo()

	fmt.Println("Predecessor:")
	fmt.Printf("  ")
	node.GetPredecessor().PrintInfo()

	fmt.Println("Successors:")
	for i := 0; i < node.successorsLength; i++ {
		fmt.Printf("  %d ", i)
		node.GetSuccessor(i).PrintInfo()
	}

	fmt.Println("Finger Table:")
	for i := 0; i < node.identifierLength; i++ {
		fmt.Printf("  %d ", i)
		fmt.Printf("Node %s + 2^%d = %s ", node.info.Identifier.String(), i, node.fingerIndex[i].String())
		node.GetFingerEntry(i).PrintInfo()
	}

	fmt.Println("Files:")
	node.printFilesname()

	fmt.Println("Backup Files:")
	for i := range node.backupStorages {
		fmt.Printf("  %d: ", i)
		node.successors[i].PrintInfo()
		node.printBackupFilesname(i)
	}
}
