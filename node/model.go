package node

import (
	"crypto/tls"
	"fmt"
	"math/big"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/chord-dht/chord-core/storage"
	"github.com/chord-dht/chord-core/tools"
)

/*                             NodeInfo Part                             */

type NodeInfo struct {
	Identifier *big.Int // true identifier
	IpAddress  string   // use for network
	Port       string   // use for network
}

// NewNodeInfo uses Infinity as the identifier, which is not valid, so the return is an empty NodeInfo
func NewNodeInfo() *NodeInfo {
	return &NodeInfo{Identifier: tools.Infinity}
}

func NewNodeInfoWithAddress(ipAddress string, port string) *NodeInfo {
	return &NodeInfo{
		Identifier: tools.Infinity,
		IpAddress:  ipAddress,
		Port:       port,
	}
}

// Empty checks if the node information is empty, empty means the node information is not valid
func (nodeInfo *NodeInfo) Empty() bool {
	// first check if the nodeInfo is nil
	if nodeInfo == nil {
		return true
	}
	// identifier is not in [0, 2^m-1]
	b1 := !tools.InInterval(nodeInfo.Identifier, big.NewInt(0), tools.TwoM, true, false)
	b2 := nodeInfo.IpAddress == ""
	b3 := nodeInfo.Port == ""
	return b1 || b2 || b3
}

// Check if two NodeInfo are equal, the equality is defined by the identifier or the network address
// If the identifier is equal, then the two NodeInfo are equal
// If the network address is equal, then the two NodeInfo are equal
func InfoEqual(nodeInfo1 *NodeInfo, nodeInfo2 *NodeInfo) bool {
	idBool := nodeInfo1.Identifier.Cmp(nodeInfo2.Identifier) == 0
	addressBool := nodeInfo1.IpAddress == nodeInfo2.IpAddress && nodeInfo1.Port == nodeInfo2.Port
	return idBool || addressBool
}

/*                             NodeInfo Part                             */

/*                             Node Part                             */

type NodeInfoList []*NodeInfo

// Node Full information of a chord node.
type Node struct {
	identifierLength int // Important
	successorsLength int // Important

	info        NodeInfo
	predecessor *NodeInfo
	successors  NodeInfoList
	fingerTable NodeInfoList
	fingerIndex []*big.Int

	muPre sync.RWMutex
	muSuc sync.RWMutex
	muFin sync.RWMutex

	localStorage   storage.Storage   // Storage for this node
	backupStorages []storage.Storage // Storages for successor nodes

	stabilizeTime        time.Duration
	fixFingersTime       time.Duration
	checkPredecessorTime time.Duration

	shutdownCh chan struct{} // channel for shutdown

	tlsBool         bool
	serverTLSConfig *tls.Config
	clientTLSConfig *tls.Config
}

func NewNode(
	identifierLength int,
	successorsLength int,
	ipAddress string,
	port string,
	storageFactory func(string) (storage.Storage, error),
	storagePath string,
	backupPath string,
	stabilizeTime time.Duration,
	fixFingersTime time.Duration,
	checkPredecessorTime time.Duration,
	tlsBool bool,
	serverTLSConfig *tls.Config,
	clientTLSConfig *tls.Config,
) (*Node, error) {
	// you have to set the identifier length for the tools package first
	tools.SetIdentifierLength(identifierLength)

	networkAddress := ipAddress + ":" + port
	identifier := tools.GenerateIdentifier(networkAddress)

	nodeInfo := NodeInfo{
		Identifier: identifier,
		IpAddress:  ipAddress,
		Port:       port,
	}

	localStorage, err := storageFactory(storagePath)
	if err != nil {
		return nil, fmt.Errorf("error creating storage: %w", err)
	}

	backupStorages := make([]storage.Storage, successorsLength)
	for i := 0; i < successorsLength; i++ {
		backupPathI := filepath.Join(backupPath, strconv.Itoa(i))
		backupStorages[i], err = storageFactory(backupPathI)
		if err != nil {
			return nil, fmt.Errorf("error creating backup storage %d: %w", i, err)
		}
	}

	node := &Node{
		identifierLength:     identifierLength,
		successorsLength:     successorsLength,
		info:                 nodeInfo,
		predecessor:          NewNodeInfo(),
		successors:           make(NodeInfoList, successorsLength), // fixed size, should not use append later, but use index
		fingerTable:          make(NodeInfoList, identifierLength), // fixed size, should not use append later, but use index
		fingerIndex:          make([]*big.Int, identifierLength),
		localStorage:         localStorage,
		backupStorages:       backupStorages,
		stabilizeTime:        stabilizeTime,
		fixFingersTime:       fixFingersTime,
		checkPredecessorTime: checkPredecessorTime,
		shutdownCh:           make(chan struct{}),
		tlsBool:              tlsBool,
		serverTLSConfig:      serverTLSConfig,
		clientTLSConfig:      clientTLSConfig,
	}

	// Initialize each NodeInfo
	for i := 0; i < successorsLength; i++ {
		node.successors[i] = NewNodeInfo()
	}
	for i := 0; i < identifierLength; i++ {
		node.fingerTable[i] = NewNodeInfo()
		node.fingerIndex[i] = fingerEntryId(&node.info, i)
	}

	// Record it in the localNode
	localNode = node

	return node, nil
}

// fingerEntryId calculates the finger table's entry's (ideal) identifier.
func fingerEntryId(nodeInfo *NodeInfo, i int) *big.Int {
	// (node.Identifier + 2^i) mod 2^m
	twoI := new(big.Int).Exp(big.NewInt(2), big.NewInt(int64(i)), nil)
	nTwoI := new(big.Int).Add(nodeInfo.Identifier, twoI)
	return nTwoI.And(nTwoI, tools.TwoMMinusOne)
}

/*                             Node Part                             */

/*                             global local node                             */

var localNode *Node

/*                             global local node                             */
