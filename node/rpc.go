package node

import (
	"crypto/tls"
	"net"
	"net/rpc"
)

// RPCHandler is the RPC handler for Chord node communication.
// It is safer to use handler rather than use node itself, as we don't want to expose the node's internal functions.
type RPCHandler int

const RPCHandlerPrefix = "RPCHandler."

// startServer starts the rpc server for the node.
// Use TLS if `node.TLSBool` is true, otherwise use normal TCP.
// The RPCHandler will be:
//  1. registered as an RPC server.
//  2. isten on the port specified in the node's Info.
//  3. serve RPC requests in a separate goroutine.
func (node *Node) startServer() error {
	handler := new(RPCHandler)
	if err := rpc.Register(handler); err != nil {
		return err
	}

	var listener net.Listener = nil
	var err error = nil
	if node.tlsBool {
		listener, err = tls.Listen("tcp", ":"+node.info.Port, node.serverTLSConfig)
	} else {
		listener, err = net.Listen("tcp", ":"+node.info.Port)
	}
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-node.shutdownCh:
				listener.Close()
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					continue
				}
				go rpc.ServeConn(conn)
			}
		}
	}()

	return nil
}

// callRPC makes an RPC call to the node.
func (nodeInfo *NodeInfo) callRPC(method string, args interface{}, reply interface{}) error {
	rpcMethod := RPCHandlerPrefix + method
	address := nodeInfo.IpAddress + ":" + nodeInfo.Port

	var conn net.Conn = nil
	var err error = nil
	if localNode.tlsBool {
		conn, err = tls.Dial("tcp", address, localNode.clientTLSConfig)
	} else {
		conn, err = net.Dial("tcp", address)
	}
	if err != nil {
		return err
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	if err := client.Call(rpcMethod, args, reply); err != nil {
		return err
	}
	return nil
}

// asyncHandleRPC abstracts the common logic for handling RPC calls with empty replies asynchronously.
// We could simply use a goroutine in the RPC func, but we emphasize the asynchronous procedure here.
func asyncHandleRPC(handler func()) {
	go func() {
		handler()
	}()
}
