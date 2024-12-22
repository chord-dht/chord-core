package node

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"
)

const pingTimeout = 1 * time.Second

// GetInfoCheck Check if the node's Info is empty or not alive
func (nodeInfo *NodeInfo) LiveCheck() error {
	if nodeInfo == nil {
		return fmt.Errorf("NodeInfo is nil")
	}
	if nodeInfo.Empty() {
		return fmt.Errorf("%v is empty", nodeInfo)
	}

	if nodeInfo.Ping() != nil {
		return fmt.Errorf("%v is not alive", nodeInfo)
	}

	return nil
}

// Ping checks if the remote node can be connected.
func (nodeInfo *NodeInfo) Ping() error {
	address := nodeInfo.IpAddress + ":" + nodeInfo.Port

	var conn net.Conn = nil
	var err error = nil
	if localNode.tlsBool {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: pingTimeout}, "tcp", address, localNode.clientTLSConfig)
	} else {
		conn, err = net.DialTimeout("tcp", address, pingTimeout)
	}
	if err != nil {
		return err
	}
	defer conn.Close()
	return nil
}
