package node

import (
	"chord/storage"
	"chord/tools"
	"fmt"
)

// All r successors would have to simultaneously fail in order to disrupt the Chord ring,
// an event that can be made very improbable with modest values of r.
// @Return: the index of the first live successor and the error
func (node *Node) findFirstLiveSuccessor() (int, error) {
	for index := 0; index < node.successorsLength; index++ {
		successor := node.GetSuccessor(index)
		if successor.LiveCheck() == nil {
			node.SetFirstSuccessor(successor) // set it immediately
			return index, nil
		}
	}
	return -1, fmt.Errorf("all successors are dead")
}

// Helper function for stabilize(), now used in updateSuccessors()
// used to handle the successor's predecessor
func (node *Node) handleX() {
	successor := node.GetFirstSuccessor()
	x, err := successor.GetPredecessor() // x = successor.predecessor
	if err != nil {
		return
	}

	if x.LiveCheck() != nil {
		return // it's ok if x is dead, we simply don't need to update the successor[0]!
	}

	if tools.ModIntervalCheck(x.Identifier, node.info.Identifier, successor.Identifier, false, false) {
		node.SetFirstSuccessor(x)
	}
}

// Update successors of the node.
// Node n reconciles its list with its successor s by copying s's successor list, removing its last entry, and prepending s to it.
// If node n notices that its successor has failed, it replaces it with the first live entry in its successor list and reconciles its successor list with its new successor.
func (node *Node) updateSuccessors() error {
	successor := node.GetFirstSuccessor()

	// 1. get this successor's successor list
	sSuccessors, err := successor.GetSuccessors()
	if err != nil {
		return err
	}
	if len(sSuccessors) != node.successorsLength {
		return fmt.Errorf("successor's successors is not equal to the node's SuccessorsLength")
	}

	// 2. reconcile (update) the node's successor list
	//  1) we just need SuccessorsLength-1 items
	truncatedSSuccessors := (sSuccessors)[:node.successorsLength-1]
	// 	2) prepend s to the list
	nSuccessors := append(NodeInfoList{successor}, truncatedSSuccessors...)
	// 	3) reconcile its successor list with its new successor
	node.SetSuccessors(nSuccessors)

	return nil
}

func (node *Node) DeleteAllBackupFiles() error {
	for i := 0; i < node.successorsLength; i++ {
		if err := node.backupStorages[i].Clear(); err != nil {
			return err
		}
	}
	return nil
}

func (node *Node) GetSuccessorFiles(successor *NodeInfo) (storage.FileList, error) {
	sFilesReply, err := successor.GetAllFiles()
	if err != nil {
		return nil, err
	}
	if !sFilesReply.Success {
		return nil, fmt.Errorf("failed to get successor[0]'s files")
	}
	sFileList := sFilesReply.FileList
	return sFileList, nil
}

func (node *Node) GetSuccessorBackupFiles(successor *NodeInfo) ([]storage.FileList, error) {
	sBackupFilesReply, err := successor.GetAllBackupFiles()
	if err != nil {
		return nil, err
	}
	if !sBackupFilesReply.Success {
		return nil, fmt.Errorf("failed to get successor[0]'s all backup files")
	}
	backupFileLists := sBackupFilesReply.FileLists
	if len(backupFileLists) != node.successorsLength {
		return nil, fmt.Errorf("successor[0]'s backup files is not equal to the node's SuccessorsLength")
	}
	return backupFileLists, nil
}

// Update the node's backup files.
// When updating the backup files, there is one thing to note:
// Finally, the backup files on the local disk should be consistent with the successors.
// If we can't stay consistent, then we need to clear the relevant backup storages on the local disk.
//  1. if we can't get the successor[0]'s files, then we can't do the following steps, and we need to clear all the backup files on the local disk
//  2. if we can't get the successor[0]'s all backup files, then we need to log it, and record the error, but we can still do the following steps, because we have already got the successor[0]'s files, and we can place them into backupStorages[0]
func (node *Node) updateBackupFiles() error {
	var finalErr error

	successor := node.GetFirstSuccessor()

	// 1. get successor[0]'s files
	sFileList, err := node.GetSuccessorFiles(successor)
	if err != nil {
		// if we can't get the successor[0]'s files, then we can't do the following steps
		// and we need to clear all the backup files on the local disk, otherwise the backup files will be inconsistent with the successors
		if clearErr := node.DeleteAllBackupFiles(); clearErr != nil {
			return clearErr
		}
		return err
	}

	var nFileLists []storage.FileList

	// 2. get successor[0]'s all backup files
	backupFileLists, err := node.GetSuccessorBackupFiles(successor)
	if err != nil {
		// if we can't get the successor[0]'s all backup files,
		// we need to log it, and record the error,
		// but we can still do the following steps, because we have already got the successor[0]'s files, and we can place them into backupStorages[0]
		finalErr = err                             // we record the error, and return it at the end, so we can still do the following steps and let the caller know the error
		nFileLists = []storage.FileList{sFileList} // just store the successor[0]'s files
	} else {
		// 3. if we can get the successor[0]'s all backup files, then we need to reconcile the node's backup files
		//  we just need IdentifierLength-1 items
		truncatedBackupFileLists := backupFileLists[:node.successorsLength-1] // keep in mind that in go, the slice [start, end] is [start, end)
		// 	prepend sFileList to the list
		nFileLists = append([]storage.FileList{sFileList}, truncatedBackupFileLists...)
	}

	//  delete all backup files on the local disk
	if err := node.DeleteAllBackupFiles(); err != nil {
		return err
	}
	//  and then store the new backup files
	if err := node.StoreBackupFiles(nFileLists); err != nil {
		return err
	}

	return finalErr // here we return the finalErr, which is the error of getting the successor[0]'s all backup files
}

// Send the old backup files to the new successor.
// It will only be called when the first successor is dead and oldBackupFileList is not empty.
func (node *Node) sendBackupFiles(oldBackupFileList storage.FileList) error {
	successor := node.GetFirstSuccessor()
	reply, err := successor.StoreFiles(oldBackupFileList)
	if err != nil {
		return err
	}
	if !reply.Success {
		return fmt.Errorf("failed to send the old backup files to %v", successor)
	}
	return nil
}

// Update both successors and backup files of the node.
func (node *Node) updateReplica() error {
	// check if the first successor is alive or not
	// it will determine whether we need to send the old backup files to the new successor
	// at the same time, we will find the first live successor
	indexOfFirstLiveSuccessor, err := node.findFirstLiveSuccessor()
	if err != nil {
		return err
	}
	firstSuccessorIsDead := indexOfFirstLiveSuccessor != 0

	var oldBackupFileList storage.FileList
	// if the first successor is dead, then we need to send the old backup files to the new successor, from 0 to indexOfFirstLiveSuccessor
	if firstSuccessorIsDead {
		// first we keep them for later use
		oldBackupFileList, _ = node.GetBackupFilesUpToIndex(indexOfFirstLiveSuccessor)
	}

	// now we have the successors[0] alive
	// deal with the successor's predecessor, aka x
	// it may change the successor[0] to x, if x (alive) is in (node, successor[0])
	node.handleX()

	// from this time, we truly have the successor[0] ready for use

	// 1. if the first successor is dead, then we need to send these old backup files to the new successor
	if firstSuccessorIsDead && oldBackupFileList != nil && len(oldBackupFileList) > 0 {
		if node.sendBackupFiles(oldBackupFileList) != nil {
			// if this send call fails, then we need to store these old backup files to the node's storage
			// so that the new successor can get them later through notifying (the node), and the node will send them again!
			_ = node.StoreFiles(oldBackupFileList)
		}
	}

	// 2. update successors
	if err := node.updateSuccessors(); err != nil {
		// this function will only fail if we can't get the successor's successors
		// theoretically, it should not happen, because we have already checked the first live successor
		// but if it happens, then the node's successor list will just remain the same (not updated)
		// finally, we choose to return the error here, without doing updateBackupFiles()
		return err
	}

	// 3. update backup files
	if err := node.updateBackupFiles(); err != nil {
		return err
	}

	return nil
}
