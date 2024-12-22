package storage

import (
	"bytes"
	"chord/storage"
	"os"
	"path/filepath"
	"testing"
)

func setupTestStorageSystem(t *testing.T) *CacheStorageSystem {
	storagePath := "./test_storage"

	// Clean up before starting the test
	os.RemoveAll(storagePath)

	ss, err := NewStorage(storagePath)
	if err != nil {
		t.Fatalf("Failed to create storage system: %v", err)
	}

	return ss
}

func TestNewStorageSystem(t *testing.T) {
	ss := setupTestStorageSystem(t)
	if ss != nil {
		defer os.RemoveAll(ss.storagePath)
	}

	if ss == nil {
		t.Fatal("Expected non-nil StorageSystem")
	}
}

func TestPersistToDisk(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey := "testfile"
	value := []byte("testdata")

	err := ss.persistToDisk(fileKey, value)
	if err != nil {
		t.Fatalf("Failed to persist to disk: %v", err)
	}

	filePath := filepath.Join(ss.storagePath, fileKey)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("Expected file to exist: %s", filePath)
	}
}

func TestLoadFromDisk(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey := "testfile"
	expectedValue := []byte("testdata")

	err := ss.persistToDisk(fileKey, expectedValue)
	if err != nil {
		t.Fatalf("Failed to persist to disk: %v", err)
	}

	value, err := ss.loadFromDisk(fileKey)
	if err != nil {
		t.Fatalf("Failed to load from disk: %v", err)
	}

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %s, got %s", expectedValue, value)
	}
}

func TestAddToCache(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey := "testfile"
	value := []byte("testdata")

	ss.addToCache(fileKey, value)

	if _, found := ss.cache[fileKey]; !found {
		t.Fatal("Expected file to be in cache")
	}
}

func TestGet(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey := "testfile"
	expectedValue := []byte("testdata")

	err := ss.Put(fileKey, expectedValue)
	if err != nil {
		t.Fatalf("Failed to put file: %v", err)
	}

	value, err := ss.Get(fileKey)
	if err != nil {
		t.Fatalf("Failed to get file: %v", err)
	}

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %s, got %s", expectedValue, value)
	}
}

func TestPut(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey := "testfile"
	value := []byte("testdata")

	err := ss.Put(fileKey, value)
	if err != nil {
		t.Fatalf("Failed to put file: %v", err)
	}

	if _, found := ss.filesname[fileKey]; !found {
		t.Fatal("Expected file to be in filesname")
	}
}

func TestUpdate(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey := "testfile"
	initialValue := []byte("initialdata")
	updatedValue := []byte("updateddata")

	// Put initial value
	err := ss.Put(fileKey, initialValue)
	if err != nil {
		t.Fatalf("Failed to put initial file: %v", err)
	}

	// Update the value
	err = ss.Update(fileKey, updatedValue)
	if err != nil {
		t.Fatalf("Failed to update file: %v", err)
	}

	// Get the updated value
	value, err := ss.Get(fileKey)
	if err != nil {
		t.Fatalf("Failed to get updated file: %v", err)
	}

	if !bytes.Equal(value, updatedValue) {
		t.Fatalf("Expected %s, got %s", updatedValue, value)
	}

	// Check if the updated value is persisted to disk
	filePath := filepath.Join(ss.storagePath, fileKey)
	diskValue, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read file from disk: %v", err)
	}

	if !bytes.Equal(diskValue, updatedValue) {
		t.Fatalf("Expected %s on disk, got %s", updatedValue, diskValue)
	}
}

func TestDelete(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey := "testfile"
	value := []byte("testdata")

	err := ss.Put(fileKey, value)
	if err != nil {
		t.Fatalf("Failed to put file: %v", err)
	}

	err = ss.Delete(fileKey)
	if err != nil {
		t.Fatalf("Failed to delete file: %v", err)
	}

	if _, found := ss.filesname[fileKey]; found {
		t.Fatal("Expected file to be removed from filesname")
	}

	filePath := filepath.Join(ss.storagePath, fileKey)
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("Expected file to be removed from disk: %s", filePath)
	}
}

func TestGetFilesByFilter(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey1 := "testfile1"
	value1 := []byte("testdata1")
	fileKey2 := "testfile2"
	value2 := []byte("testdata2")

	ss.Put(fileKey1, value1)
	ss.Put(fileKey2, value2)

	filter := func(key string) bool {
		return key == fileKey1
	}

	files, err := ss.GetFilesByFilter(filter)
	if err != nil {
		t.Fatalf("Failed to get files by filter: %v", err)
	}

	if len(files) != 1 || files[0].Key != fileKey1 {
		t.Fatalf("Expected to get file %s, got %v", fileKey1, files)
	}
}

func TestPutFiles(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	files := storage.FileList{
		{Key: "testfile1", Value: []byte("testdata1")},
		{Key: "testfile2", Value: []byte("testdata2")},
	}

	err := ss.PutFiles(files)
	if err != nil {
		t.Fatalf("Failed to put files: %v", err)
	}

	for _, file := range files {
		if _, found := ss.filesname[file.Key]; !found {
			t.Fatalf("Expected file %s to be in filesname", file.Key)
		}
	}
}

func TestGetAllFiles(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	files := storage.FileList{
		{Key: "testfile1", Value: []byte("testdata1")},
		{Key: "testfile2", Value: []byte("testdata2")},
	}

	ss.PutFiles(files)

	allFiles, err := ss.GetAllFiles()
	if err != nil {
		t.Fatalf("Failed to get all files: %v", err)
	}

	if len(allFiles) != len(files) {
		t.Fatalf("Expected %d files, got %d", len(files), len(allFiles))
	}
}

func TestClear(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	files := storage.FileList{
		{Key: "testfile1", Value: []byte("testdata1")},
		{Key: "testfile2", Value: []byte("testdata2")},
	}

	ss.PutFiles(files)

	err := ss.Clear()
	if err != nil {
		t.Fatalf("Failed to clear storage system: %v", err)
	}

	if len(ss.filesname) != 0 {
		t.Fatal("Expected filesname to be empty")
	}

	if len(ss.cache) != 0 {
		t.Fatal("Expected cache to be empty")
	}

	if _, err := os.Stat(ss.storagePath); os.IsNotExist(err) {
		t.Fatalf("Expected storage path to exist: %s", ss.storagePath)
	}
}

func TestExtractFilesByFilter(t *testing.T) {
	ss := setupTestStorageSystem(t)
	defer os.RemoveAll(ss.storagePath)

	fileKey1 := "testfile1"
	value1 := []byte("testdata1")
	fileKey2 := "testfile2"
	value2 := []byte("testdata2")

	ss.Put(fileKey1, value1)
	ss.Put(fileKey2, value2)

	filter := func(key string) bool {
		return key == fileKey1
	}

	files, err := ss.ExtractFilesByFilter(filter)
	if err != nil {
		t.Fatalf("Failed to extract files by filter: %v", err)
	}

	if len(files) != 1 || files[0].Key != fileKey1 {
		t.Fatalf("Expected to extract file %s, got %v", fileKey1, files)
	}

	if _, found := ss.filesname[fileKey1]; found {
		t.Fatalf("Expected file %s to be removed from filesname", fileKey1)
	}

	filePath := filepath.Join(ss.storagePath, fileKey1)
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("Expected file %s to be removed from disk", filePath)
	}
}
