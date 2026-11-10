package archive

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Archiver interface {
	Archive(payload []byte) error
	Close() error
}

type FileArchiver struct {
	baseDir string
	mu      sync.Mutex
	file    *os.File
	gz      *gzip.Writer
	curDay  string
}

// TODO: integrate with Object Store 

func NewFileArchiver(baseDir string) (*FileArchiver, error) {
	if baseDir == "" {
		baseDir = "./archives"
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("create archive dir: %w", err)
	}

	return &FileArchiver{baseDir: baseDir}, nil
}

func (a *FileArchiver) Archive(payload []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	today := time.Now().UTC().Format("2006-01-02")
	if a.curDay != today || a.file == nil {
		if err := a.rotate(today); err != nil {
			return err
		}
	}

	line := append(payload, '\n')
	if _, err := a.gz.Write(line); err != nil {
		return fmt.Errorf("write to archive: %w", err)
	}
	return a.gz.Flush()
}

func (a *FileArchiver) rotate(day string) error {
	if a.gz != nil {
		_ = a.gz.Close()
	}
	if a.file != nil {
		_ = a.file.Close()
	}

	filePath := filepath.Join(a.baseDir, fmt.Sprintf("raw_events_%s.json.gz", day))
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open archive file: %w", err)
	}

	a.file = f
	a.gz = gzip.NewWriter(f)
	a.curDay = day
	return nil
}

func (a *FileArchiver) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.gz != nil {
		_ = a.gz.Close()
	}
	if a.file != nil {
		_ = a.file.Close()
	}
	return nil
}

// ReadArchiveFile streams raw event lines from a .json or .json.gz archive file.
func ReadArchiveFile(filePath string, onEvent func(data []byte) error) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open archive: %w", err)
	}
	defer f.Close()

	var scanner *bufio.Scanner
	if filepath.Ext(filePath) == ".gz" {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip reader: %w", err)
		}
		defer gz.Close()
		scanner = bufio.NewScanner(gz)
	} else {
		scanner = bufio.NewScanner(f)
	}

	const maxScanCapacity = 4 * 1024 * 1024
	buf := make([]byte, maxScanCapacity)
	scanner.Buffer(buf, maxScanCapacity)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		// Validate JSON
		if !json.Valid(line) {
			continue
		}
		if err := onEvent(line); err != nil {
			return err
		}
	}

	return scanner.Err()
}
