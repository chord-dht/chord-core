package node

import (
	"fmt"

	"github.com/chord-dht/chord-core/storage"
)

/*
 * The code in this file is used as a wrap of node's storage and backup storages.
 * It provides a more convenient way to interact with the storage and backup storages of the node.
 * You should not contract with the storage and backup storages directly.
 */

/*                             Used for storage                             */

// GetFilesName gets all files' name from the node.
func (node *Node) GetFilesName() []string {
	return node.localStorage.GetFilesName()
}

// StoreFile stores the given data associated with the filename in the node.
func (node *Node) StoreFile(filename string, data []byte) error {
	return node.localStorage.Put(filename, data)
}

// GetFile gets the data associated with the filename from the node.
func (node *Node) GetFile(filename string) ([]byte, error) {
	return node.localStorage.Get(filename)
}

// DeleteFile removes the data associated with the filename from the node.
func (node *Node) DeleteFile(filename string) error {
	return node.localStorage.Delete(filename)
}

// UpdateFile updates the data associated with the filename in the node.
func (node *Node) UpdateFile(filename string, data []byte) error {
	return node.localStorage.Update(filename, data)
}

// StoreFiles stores the given files in the node.
func (node *Node) StoreFiles(files storage.FileList) error {
	return node.localStorage.PutFiles(files)
}

// GetAllFiles gets all files from the node.
func (node *Node) GetAllFiles() (storage.FileList, error) {
	return node.localStorage.GetAllFiles()
}

// GetFilesByFilter gets files from the node that satisfy the filter.
func (node *Node) GetFilesByFilter(filter func(string) bool) (storage.FileList, error) {
	return node.localStorage.GetFilesByFilter(filter)
}

// ExtractFilesByFilter gets the files from the node that satisfy the filter and removes them from the node.
func (node *Node) ExtractFilesByFilter(filter func(string) bool) (storage.FileList, error) {
	return node.localStorage.ExtractFilesByFilter(filter)
}

/*                             Used for storage                             */

/*                             Used for backupStorages                             */

// GetAllBackupFilesName gets backup files' name from all backup storages.
func (node *Node) GetAllBackupFilesName() [][]string {
	filesNames := make([][]string, node.successorsLength)
	for i := 0; i < node.successorsLength; i++ {
		filesNames[i] = node.GetBackupFilesName(i)
	}
	return filesNames
}

// GetBackupFilesName gets backup files' name from one of the backup storages.
func (node *Node) GetBackupFilesName(index int) []string {
	return node.backupStorages[index].GetFilesName()
}

// GetAllBackupFiles gets all backup files from the node.
func (node *Node) GetAllBackupFiles() ([]storage.FileList, error) {
	fileLists := make([]storage.FileList, node.successorsLength)
	for i := 0; i < node.successorsLength; i++ {
		fileList, err := node.backupStorages[i].GetAllFiles()
		if err != nil {
			return nil, err
		}
		fileLists[i] = fileList
	}
	return fileLists, nil
}

// GetBackupFilesUpToIndex gets backup files from the node up to the specified index (exclusive) and flattens them into a single fileList.
func (node *Node) GetBackupFilesUpToIndex(endIndex int) (storage.FileList, error) {
	if endIndex >= node.successorsLength || endIndex < 0 {
		return nil, fmt.Errorf("endIndex out of range: %d", endIndex)
	}

	var fileList storage.FileList
	for i := 0; i < endIndex; i++ {
		files, err := node.backupStorages[i].GetAllFiles()
		if err != nil {
			// if it fails, then we clear this storage and continue
			if err := node.backupStorages[i].Clear(); err != nil {
				return nil, err // but if clear fails, then we return the error
			}
			continue
		}
		fileList = append(fileList, files...)
	}
	return fileList, nil
}

// StoreBackupFiles stores the given files in the backup storages.
func (node *Node) StoreBackupFiles(fileLists []storage.FileList) error {
	if len(fileLists) != node.successorsLength {
		return fmt.Errorf("number of fileLists does not match number of backup storages")
	}

	for i := 0; i < node.successorsLength; i++ {
		if err := node.backupStorages[i].PutFiles(fileLists[i]); err != nil {
			// if it fails, then we clear this storage and continue
			if err := node.backupStorages[i].Clear(); err != nil {
				return err // but if clear fails, then we return the error
			}
			continue
		}
	}
	return nil
}

/*                             Used for backupStorages                             */
