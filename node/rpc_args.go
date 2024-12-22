package node

import (
	"chord/storage"
)

/*                             basic part                             */

type Empty struct {
}

type BoolReply struct {
	Success bool
}

/*                             basic part                             */

/*                             find part                             */

type FindSuccessorReply struct {
	Found    bool
	NodeInfo NodeInfo
}

/*                             find part                             */

/*                             store part                             */

type StoreFileArgs struct {
	File storage.File
}

type StoreFileReply = BoolReply

type StoreFileListArgs struct {
	FileList storage.FileList
}

type StoreFileListReply = BoolReply

/*                             store part                             */

/*                             get part                             */

type GetFileArgs struct {
	Filename string
}

type GetFileReply struct {
	Success     bool
	FileContent []byte
}

type GetFileListReply struct {
	Success  bool
	FileList storage.FileList
}

type GetFileListsReply struct {
	Success   bool
	FileLists []storage.FileList
}

/*                             get part                             */

/*                             other                             */

type GetLengthReply struct {
	IdentifierLength int
	SuccessorsLength int
}

/*                             other                             */
