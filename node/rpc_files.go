package node

import (
	"chord/log"
	"chord/storage"
)

/*                             single file part                             */

// StoreFile is a wrap of StoreFileRPC method
func (nodeInfo *NodeInfo) StoreFile(filename string, fileContent []byte) (*StoreFileReply, error) {
	file := storage.File{
		Key:   filename,
		Value: fileContent,
	}
	args := &StoreFileArgs{
		File: file,
	}
	reply := &StoreFileReply{}
	err := nodeInfo.callRPC("StoreFileRPC", args, reply)
	return reply, err
}

// StoreFileRPC : Store the file in the node's storage
func (handler *RPCHandler) StoreFileRPC(args *StoreFileArgs, reply *StoreFileReply) error {
	defer log.LogFunction()()

	file := args.File

	err := localNode.StoreFile(file.Key, file.Value)
	if err != nil {
		reply.Success = false
	} else {
		reply.Success = true
	}
	return nil
}

// GetFile is a wrap of GetFileRPC method
// get the file from the node (nodeInfo)
func (nodeInfo *NodeInfo) GetFile(filename string) (*GetFileReply, error) {
	args := &GetFileArgs{
		Filename: filename,
	}
	reply := &GetFileReply{}
	err := nodeInfo.callRPC("GetFileRPC", args, reply)
	return reply, err
}

// GetFileRPC : Get the file from the node
func (handler *RPCHandler) GetFileRPC(args *GetFileArgs, reply *GetFileReply) error {
	defer log.LogFunction()()

	fileContent, err := localNode.GetFile(args.Filename)
	if err != nil {
		reply.Success = false
		reply.FileContent = nil
	} else {
		reply.Success = true
		reply.FileContent = fileContent
	}
	return nil
}

/*                             single file part                             */

/*                             multiple files part                             */

// GetAllFiles is a wrap of GetAllFilesRPC method
func (nodeInfo *NodeInfo) GetAllFiles() (*GetFileListReply, error) {
	reply := &GetFileListReply{}
	err := nodeInfo.callRPC("GetAllFilesRPC", &Empty{}, reply)
	return reply, err
}

// GetAllFilesRPC : Get the files from the node
// Point to note: ONLY StorageDir
func (handler *RPCHandler) GetAllFilesRPC(args *Empty, reply *GetFileListReply) error {
	defer log.LogFunction()()

	if fileList, err := localNode.GetAllFiles(); err != nil {
		log.Error("GetAllFiles() failed: %v", err)
		reply.Success = false
		reply.FileList = nil
	} else {
		log.Info("Read file list successfully")
		reply.Success = true
		reply.FileList = fileList
	}
	return nil
}

// GetAllBackupFiles is a wrap of GetAllBackupFilesRPC method
func (nodeInfo *NodeInfo) GetAllBackupFiles() (*GetFileListsReply, error) {
	reply := &GetFileListsReply{}
	err := nodeInfo.callRPC("GetAllBackupFilesRPC", &Empty{}, reply)
	return reply, err
}

// GetAllBackupFilesRPC : Get the backup file lists from the node
func (handler *RPCHandler) GetAllBackupFilesRPC(args *Empty, reply *GetFileListsReply) error {
	defer log.LogFunction()()

	if fileLists, err := localNode.GetAllBackupFiles(); err != nil {
		log.Error("GetAllBackupFiles() failed: %v", err)
		reply.Success = false
		reply.FileLists = nil
	} else {
		log.Info("Read backup file lists successfully")
		reply.Success = true
		reply.FileLists = fileLists
	}
	return nil
}

// StoreFiles is a wrap of StoreFilesRPC method.
// This function will be invoked in the underlying situation:
//
//  1. The node's successor[0] failed, the node needs to send the backup file list to its new successor.
//  2. A new node join the ring and becomes the node's new predecessor, the node needs to send the chosen file list to it. (file's identifier <= predecessor)
func (nodeInfo *NodeInfo) StoreFiles(fileList storage.FileList) (*StoreFileListReply, error) {
	args := &StoreFileListArgs{
		FileList: fileList,
	}
	reply := &StoreFileListReply{}
	err := nodeInfo.callRPC("StoreFilesRPC", args, reply)
	return reply, err
}

// StoreFilesRPC : Store the file list on the node's storage
func (handler *RPCHandler) StoreFilesRPC(args *StoreFileListArgs, reply *StoreFileListReply) error {
	defer log.LogFunction()()

	if err := localNode.StoreFiles(args.FileList); err != nil {
		log.Error("StoreFiles failed: %v", err)
		reply.Success = false
	} else {
		log.Info("Store files successfully")
		reply.Success = true
	}
	return nil
}

/*                             multiple files part                             */
