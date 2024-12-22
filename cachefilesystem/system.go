package storage

import (
	"chord/storage"
	"container/list"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// CacheStorageSystem represents a storage system with caching and disk persistence.
type CacheStorageSystem struct {
	storagePath string              // Path to store files on disk
	filesname   map[string]struct{} // Map to track stored files

	cache       map[string]*list.Element // In-memory cache
	cacheList   *list.List               // List to maintain LRU order
	cacheSize   int                      // Maximum size of the cache
	maxFileSize int64                    // Maximum file size, files larger than this will be stored directly on disk

	mu sync.Mutex // Mutex to ensure thread safety
}

// NewStorage creates a new StorageSystem instance with default settings.
func NewStorage(storagePath string) (*CacheStorageSystem, error) {
	// Create the storage directory if it does not exist
	if _, err := os.Stat(storagePath); os.IsNotExist(err) {
		err := os.MkdirAll(storagePath, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("error creating directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("error checking directory: %w", err)
	}

	defaultCacheSize := 100
	defaultMaxFileSize := int64(1024 * 1024) // 1MB

	return NewStorageWithSetting(
		storagePath,
		defaultCacheSize,
		defaultMaxFileSize,
	), nil
}

// cacheItem is an alias for File.
type cacheItem = storage.File

// NewStorageWithSetting creates a new StorageSystem instance with a cache size and max file size.
func NewStorageWithSetting(
	storagePath string,
	cacheSize int,
	maxFileSize int64,
) *CacheStorageSystem {
	return &CacheStorageSystem{
		storagePath: storagePath,
		filesname:   make(map[string]struct{}),
		cache:       make(map[string]*list.Element),
		cacheList:   list.New(),
		cacheSize:   cacheSize,
		maxFileSize: maxFileSize,
	}
}

// persistToDisk saves the given value to a file on disk.
func (s *CacheStorageSystem) persistToDisk(fileKey string, Value []byte) error {
	filePath := filepath.Join(s.storagePath, fileKey)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(Value)
	if err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}

	err = file.Sync()
	if err != nil {
		os.Remove(filePath)
		return fmt.Errorf("error syncing file: %w", err)
	}

	s.filesname[fileKey] = struct{}{}
	return nil
}

// loadFromDisk loads the value from a file on disk.
func (s *CacheStorageSystem) loadFromDisk(fileKey string) ([]byte, error) {
	filePath := filepath.Join(s.storagePath, fileKey)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("error getting file info: %w", err)
	}

	data := make([]byte, stat.Size())
	_, err = io.ReadFull(file, data)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}
	return data, nil
}

// addToCache adds the given file to the cache.
func (s *CacheStorageSystem) addToCache(fileKey string, value []byte) {
	// If the cache is full, remove the least recently used item
	if s.cacheList.Len() >= s.cacheSize {
		backElement := s.cacheList.Back()
		if backElement != nil {
			s.cacheList.Remove(backElement)
			delete(s.cache, backElement.Value.(*cacheItem).Key)
			backElement.Value = nil // Explicitly set to nil to avoid memory leak
		}
	}

	// Add the value to the cache
	item := &cacheItem{Key: fileKey, Value: value}
	element := s.cacheList.PushFront(item)
	s.cache[fileKey] = element
}

// persistAndCache persists the value to disk and caches it if it is small enough.
func (s *CacheStorageSystem) persistAndCache(fileKey string, value []byte) error {
	// Persist the value to disk
	if err := s.persistToDisk(fileKey, value); err != nil {
		return err
	}

	// Add the fileKey to the filesname map
	s.filesname[fileKey] = struct{}{}

	// Check the file size
	fileSize := int64(len(value))

	// If the file size is larger than maxFileSize, do not cache it
	if fileSize > s.maxFileSize {
		return nil
	}

	// Add the value to the cache
	s.addToCache(fileKey, value)

	return nil
}

// CheckFiles checks if the files still exist on disk.
func (s *CacheStorageSystem) CheckFiles() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var keysToDelete []string

	for fileKey := range s.filesname {
		filePath := filepath.Join(s.storagePath, fileKey)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			keysToDelete = append(keysToDelete, fileKey)
		}
	}

	for _, key := range keysToDelete {
		delete(s.filesname, key)
	}
}

func (s *CacheStorageSystem) GetFilesName() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, 0, len(s.filesname))
	for key := range s.filesname {
		keys = append(keys, key)
	}

	return keys
}

// Get retrieves the value associated with the given fileKey.
// It first checks the filesname, then the cache, and if not found, loads it from disk.
func (s *CacheStorageSystem) Get(fileKey string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the fileKey is in the filesname
	if _, found := s.filesname[fileKey]; !found {
		return nil, fmt.Errorf("fileKey not found: %s", fileKey)
	}

	// Check if the value is in the cache
	if element, found := s.cache[fileKey]; found {
		s.cacheList.MoveToFront(element)
		return element.Value.(*cacheItem).Value, nil
	}

	// Load the value from disk
	value, err := s.loadFromDisk(fileKey)
	if err != nil {
		return nil, err
	}

	// Add the value to the cache
	s.addToCache(fileKey, value)

	return value, nil
}

// Put stores the value associated with the given fileKey.
func (s *CacheStorageSystem) Put(fileKey string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.persistAndCache(fileKey, value)
}

// Update modifies the value associated with the given fileKey.
func (s *CacheStorageSystem) Update(fileKey string, newValue []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the fileKey exists in the filesname
	if _, found := s.filesname[fileKey]; !found {
		return fmt.Errorf("fileKey not found: %s", fileKey)
	}

	// Persist the new value to disk
	if err := s.persistToDisk(fileKey, newValue); err != nil {
		return err
	}

	// Check the file size
	fileSize := int64(len(newValue))

	// If the file size is larger than maxFileSize, remove it from the cache
	if fileSize > s.maxFileSize {
		if element, found := s.cache[fileKey]; found {
			s.cacheList.Remove(element)
			delete(s.cache, fileKey)
		}
		return nil
	}

	// Update the cache with the new value
	if element, found := s.cache[fileKey]; found {
		s.cacheList.MoveToFront(element)
		element.Value.(*cacheItem).Value = newValue
	} else {
		s.addToCache(fileKey, newValue)
	}

	return nil
}

// Delete removes the value associated with the given fileKey from both the cache and disk.
func (s *CacheStorageSystem) Delete(fileKey string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the fileKey exists in the filesname
	if _, found := s.filesname[fileKey]; !found {
		return fmt.Errorf("fileKey not found: %s", fileKey)
	}

	// defer ensures the fileKey is removed from the filesname regardless of os.Remove result
	defer delete(s.filesname, fileKey)

	// Remove from cache if present
	if element, found := s.cache[fileKey]; found {
		s.cacheList.Remove(element)
		delete(s.cache, fileKey)
	}

	// Remove from disk
	filePath := filepath.Join(s.storagePath, fileKey)
	err := os.Remove(filePath)
	if err != nil {
		return fmt.Errorf("error removing file: %w", err)
	} else {
		return nil
	}
}

func (s *CacheStorageSystem) GetFilesByFilter(filter func(string) bool) (storage.FileList, error) {
	var files storage.FileList
	s.mu.Lock()
	defer s.mu.Unlock()

	for fileKey := range s.filesname {
		if filter(fileKey) {
			// Load the value from disk
			value, err := s.loadFromDisk(fileKey)
			if err != nil {
				return nil, err
			}

			// Add the value to the files list
			files = append(files, &storage.File{Key: fileKey, Value: value})
		}
	}
	return files, nil
}

// PutFiles stores the given files.
func (s *CacheStorageSystem) PutFiles(files storage.FileList) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, file := range files {
		if err := s.persistAndCache(file.Key, file.Value); err != nil {
			return err
		}
	}
	return nil
}

// GetAllFiles retrieves all files from the storage system.
func (s *CacheStorageSystem) GetAllFiles() (storage.FileList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var files storage.FileList

	// Iterate over all fileKeys in the filesname
	for fileKey := range s.filesname {
		// Load the value from disk directly
		value, err := s.loadFromDisk(fileKey)
		if err != nil {
			return nil, err
		}

		// Add the value to the files list
		files = append(files, &storage.File{Key: fileKey, Value: value})
	}

	return files, nil
}

// Clear removes all files from both the cache and disk.
func (s *CacheStorageSystem) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear the cache
	s.cache = make(map[string]*list.Element)
	s.cacheList.Init()

	// Clear the filesname map
	s.filesname = make(map[string]struct{})

	// Remove all files from the disk
	err := os.RemoveAll(s.storagePath)
	if err != nil {
		return fmt.Errorf("error clearing storage directory: %w", err)
	}

	// Recreate the storage directory
	err = os.MkdirAll(s.storagePath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error recreating storage directory: %w", err)
	}

	return nil
}

// ExtractFilesByFilter extracts the files that match the filter from the file system and returns them as a FileList.
// It also removes the files from the file system.
// If an error occurs, the process continues to the next file.
//  1. Load the value from disk failed -> continue to the next file, but delete the key from the filesname later
//  2. Remove from disk failed -> continue to the next file, but delete the key from the filesname later
//
// This function is special, as even if an error occurs, we still believe the FileList result is valid.
func (s *CacheStorageSystem) ExtractFilesByFilter(filter func(string) bool) (storage.FileList, error) {
	var files storage.FileList
	var keysToDelete []string
	var errs []error

	s.mu.Lock()
	defer s.mu.Unlock()

	defer func() {
		if len(keysToDelete) > 0 {
			// Delete keys from filesname map
			for _, key := range keysToDelete {
				delete(s.filesname, key)
			}
		}
	}()

	for fileKey := range s.filesname {
		if filter(fileKey) {
			// Load the value from disk directly
			value, err := s.loadFromDisk(fileKey)
			if err != nil {
				errs = append(errs, fmt.Errorf("error loading file %s: %w", fileKey, err))
				keysToDelete = append(keysToDelete, fileKey)
				// error won't stop the process, but continue to the next file
				continue
			}

			// Add the value to the files list
			files = append(files, &storage.File{Key: fileKey, Value: value})

			// Remove from disk
			filePath := filepath.Join(s.storagePath, fileKey)
			err = os.Remove(filePath)
			if err != nil {
				errs = append(errs, fmt.Errorf("error removing file %s: %w", fileKey, err))
				keysToDelete = append(keysToDelete, fileKey)
				// error won't stop the process, but continue to the next file
				continue
			}

			// Add the key to the keysToDelete list
			keysToDelete = append(keysToDelete, fileKey)
		}
	}

	if len(errs) > 0 {
		return files, fmt.Errorf("encountered errors: %v", len(errs))
	}

	return files, nil
}
