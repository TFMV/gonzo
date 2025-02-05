package storage

import (
	"os"

	"github.com/TFMV/gonzo/db"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type Storage struct {
	db *db.DB
}

func NewStorage(db *db.DB) *Storage {
	return &Storage{
		db: db,
	}
}

func (s *Storage) SaveToDisk(filepath string) error {
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create Arrow IPC writer with correct options
	writer, err := ipc.NewFileWriter(file, ipc.WithSchema(s.db.GetSchema()), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return err
	}
	defer writer.Close()

	return nil
}

func (s *Storage) LoadFromDisk(filepath string) error {
	// Open file for reading
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create Arrow IPC reader
	reader, err := ipc.NewFileReader(file, ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		return err
	}
	defer reader.Close()

	return nil
}
