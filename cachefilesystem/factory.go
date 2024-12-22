package storage

import (
	"fmt"

	"github.com/chord-dht/chord-core/storage"
)

// CacheStorageFactory is the default implementation of StorageFactory using NewStorage.
func CacheStorageFactory(path string) (storage.Storage, error) {
	storage, err := NewStorage(path)
	if err != nil {
		return nil, fmt.Errorf("error creating storage at %s: %w", path, err)
	}
	return storage, nil
}
