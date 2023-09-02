package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	CrcSize       = 4
	TimestampSize = 8
	TombstoneSize = 1
	KeySizeSize   = 8
	ValueSizeSize = 8

	CrcStart       = 0
	TimestampStart = CrcStart + CrcSize
	TombstoneStart = TimestampStart + TimestampSize
	KeySizeStart   = TombstoneStart + TombstoneSize
	ValueSizeStart = KeySizeStart + KeySizeSize
	KeyStart       = ValueSizeStart + ValueSizeSize
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

const (
	LogDirectory   = "wal"
	MaxSegmentSize = 1024 * 1024 // 1 MB
	LowWaterMark   = 10
)

func findHighestIndex(files []string) int {
	highestIndex := -1 // Initialize to -1 so that any positive index is considered
	for _, file := range files {
		var index int
		_, err := fmt.Sscanf(filepath.Base(file), "wal_%d", &index)
		if err == nil && index > highestIndex {
			highestIndex = index
		}
	}

	if highestIndex == -1 {
		highestIndex = 0
	}

	return highestIndex
}

type WAL struct {
	dir   string
	index int
	lwm   int
	file  *os.File
}

func NewWAL() (*WAL, error) {
	err := os.MkdirAll(LogDirectory, 0777)
	if err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(LogDirectory, "wal_"))
	if err != nil {
		return nil, err
	}

	highestIndex := findHighestIndex(files)

	// Create a new WAL with the next index
	filename := filepath.Join(LogDirectory, fmt.Sprintf("wal_%d", highestIndex))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
	if err != nil {
		return nil, err
	}

	return &WAL{
		dir:   LogDirectory,
		index: highestIndex,
		lwm:   LowWaterMark,
		file:  file,
	}, nil
}

func (w *WAL) WriteRecord(record Record) error {
	buf := new(bytes.Buffer)

	// Compute CRC
	forCRC := append(record.Key, record.Value...)
	crc := CRC32(forCRC)

	// Write CRC
	err := binary.Write(buf, binary.BigEndian, crc)
	if err != nil {
		return err
	}

	// Write Timestamp
	err = binary.Write(buf, binary.BigEndian, record.Timestamp.Unix())
	if err != nil {
		return err
	}

	// Write Tombstone
	if record.Tombstone {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	// Write Key Size
	err = binary.Write(buf, binary.BigEndian, int64(len(record.Key)))
	if err != nil {
		return err
	}

	// Write Value Size
	err = binary.Write(buf, binary.BigEndian, int64(len(record.Value)))
	if err != nil {
		return err
	}

	// Write Key
	buf.Write(record.Key)

	// Write Value
	buf.Write(record.Value)

	// Append to the log file
	_, err = w.file.Write(buf.Bytes())
	if err != nil {
		return err
	}

	// Need to check if the segment is now full
	fileInfo, err := w.file.Stat()
	if err != nil {
		return err
	}
	if fileInfo.Size() >= MaxSegmentSize {
		err := w.file.Close()
		if err != nil {
			return err
		}
		w.index++
		filename := filepath.Join(w.dir, fmt.Sprintf("wal_%d", w.index))
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
		if err != nil {
			return err
		}
		w.file = file
	}

	return nil
}

// pitaj ih jel bi radije da vraca listu recordsa
func (w *WAL) ReadWAL() error {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal_*"))
	if err != nil {
		return err
	}

	sort.Slice(files, func(i, j int) bool {
		var index1, index2 int
		fmt.Sscanf(filepath.Base(files[i]), "wal_%d", &index1)
		fmt.Sscanf(filepath.Base(files[j]), "wal_%d", &index2)
		return index1 < index2
	})

	var records []Record

	for _, file := range files {
		file, err := os.Open(file)
		if err != nil {
			return err
		}

		for {
			var crc uint32
			err := binary.Read(file, binary.BigEndian, &crc)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("Greška prilikom čitanja crc:", err)
				return err
			}

			var timestampUnix int64
			err = binary.Read(file, binary.BigEndian, &timestampUnix)
			if err != nil {
				fmt.Println("Greška prilikom čitanja vremenske oznake:", err)
				return err
			}
			timestamp := time.Unix(timestampUnix, 0)

			var tombstoneByte byte
			err = binary.Read(file, binary.BigEndian, &tombstoneByte)
			if err != nil {
				fmt.Println("Greška prilikom čitanja Tombstone:", err)
				return err
			}
			tombstone := tombstoneByte == 1

			var keySize int64
			err = binary.Read(file, binary.BigEndian, &keySize)
			if err != nil {
				fmt.Println("Greška prilikom čitanja veličine ključa:", err)
				return err
			}

			var valueSize int64
			err = binary.Read(file, binary.BigEndian, &valueSize)
			if err != nil {
				fmt.Println("Greška prilikom čitanja veličine vrednosti:", err)
				return err
			}

			key := make([]byte, keySize)
			_, err = file.Read(key)
			if err != nil {
				fmt.Println("Greška prilikom čitanja ključa:", err)
				return err
			}

			value := make([]byte, valueSize)
			_, err = file.Read(value)
			if err != nil {
				fmt.Println("Greška prilikom čitanja vrednosti:", err)
				return err
			}

			record := Record{
				Timestamp: timestamp,
				Tombstone: tombstone,
				Key:       key,
				Value:     value,
			}

			records = append(records, record)
		}
		file.Close()
	}

	for _, entry := range records {
		fmt.Printf("Timestamp: %s, Tombstone: %v, Key: %s, Value: %s\n",
			entry.Timestamp.Format(time.RFC3339), entry.Tombstone, entry.Key, entry.Value)
	}
	return nil
}

func (w *WAL) cleanUpWal() bool {
	files, err := filepath.Glob(filepath.Join(w.dir, "wal_"))
	if err != nil {
		fmt.Println(err)
		return false
	}

	//If there is less than 20 wal segments we won't remove any
	if len(files) < 20 {
		fmt.Println("There is not enough segments for the clean up.")
		return false
	}

	// Remove files with index lower than the low watermark
	for _, file := range files {
		var index int
		_, err := fmt.Sscanf(filepath.Base(file), "wal_%d", &index)
		if err == nil && index < w.lwm {
			err := os.Remove(file)
			if err != nil {
				fmt.Println(err)
				return false
			}
		}
	}

	// Update indexes to start with 0 again
	for i, file := range files {
		newIndex := i
		newFilename := filepath.Join(w.dir, fmt.Sprintf("wal_%d", newIndex))
		err := os.Rename(file, newFilename)
		if err != nil {
			fmt.Println(err)
			return false
		}
	}

	return true
}

func main() {
	mojWal, err := NewWAL()
	if err != nil {
		fmt.Println(err)
	}

	var records []Record

	records = append(records, *NewRecord(time.Now(), false, []byte("milica"), []byte("123")))
	records = append(records, *NewRecord(time.Now(), false, []byte("poop"), []byte("shitttt")))
	records = append(records, *NewRecord(time.Now(), true, []byte("milica"), []byte("123")))
	records = append(records, *NewRecord(time.Now(), false, []byte("milica"), []byte("betterpass!")))

	for _, record := range records {
		err = mojWal.WriteRecord(record)
		if err != nil {
			fmt.Println(err)
		}
	}

	err = mojWal.ReadWAL()
	if err != nil {
		fmt.Println(err)
	}

	cleaned := mojWal.cleanUpWal()
	fmt.Println(cleaned)
}
