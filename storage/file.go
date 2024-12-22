package storage

// File represents a file with its key and content.
type File struct {
	Key   string
	Value []byte
}

// FileList represents a list of files.
type FileList []*File
