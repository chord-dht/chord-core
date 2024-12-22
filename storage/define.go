package storage

type Storage interface {
	CheckFiles()
	GetFilesName() []string
	Get(fileKey string) ([]byte, error)
	Put(fileKey string, value []byte) error
	Update(fileKey string, newValue []byte) error
	Delete(fileKey string) error
	GetFilesByFilter(filter func(string) bool) (FileList, error)
	PutFiles(files FileList) error
	GetAllFiles() (FileList, error)
	Clear() error
	ExtractFilesByFilter(filter func(string) bool) (FileList, error)
}
