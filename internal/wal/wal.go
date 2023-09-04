package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go-touch-grass/config"
	"hash/crc32"
	"io"
	"os"
	fp "path/filepath"
	"sort"
	"time"

	"golang.org/x/exp/slices"
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

func findHighestIndex(files []string) int {
	highestIndex := -1 // Initialize to -1 so that any positive index is considered
	for _, file := range files {
		var index int
		_, err := fmt.Sscanf(fp.Base(file), "wal_%03d", &index)
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
	dir      string
	index    int
	lwm      int
	sgmtsize int64
	file     *os.File
}

func New(logPath string, config *config.Config) *WAL {
	os.MkdirAll(logPath, 0777)

	files, _ := fp.Glob(fp.Join(logPath, "wal_*"))
	highestIndex := findHighestIndex(files)

	// Create a new WAL with the next index
	filename := fp.Join(logPath, fmt.Sprintf("wal_%03d", highestIndex))
	file, _ := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)

	return &WAL{
		dir:      logPath,
		index:    highestIndex,
		lwm:      config.WalLowWaterMark,
		sgmtsize: config.WalSegmentSize,
		file:     file,
	}
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

	if record.FlushFlag {
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
	if fileInfo.Size() >= w.sgmtsize {
		err := w.file.Close()
		if err != nil {
			return err
		}
		w.index++
		filename := fp.Join(w.dir, fmt.Sprintf("wal_%03d", w.index))
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0777)
		if err != nil {
			return err
		}
		w.file = file
	}

	return nil
}

// pitaj ih jel bi radije da vraca listu recordsa
func (w *WAL) ReadWAL() ([]Record, error) {
	files, err := fp.Glob(fp.Join(w.dir, "wal_*"))
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		var index1, index2 int
		fmt.Sscanf(fp.Base(files[i]), "wal_%03d", &index1)
		fmt.Sscanf(fp.Base(files[j]), "wal_%03d", &index2)
		return index1 < index2
	})

	var records []Record

	for _, file := range files {
		file, err := os.Open(file)
		if err != nil {
			return nil, err
		}

		for {
			var crc uint32
			err := binary.Read(file, binary.BigEndian, &crc)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Println("Greška prilikom čitanja crc:", err)
				return nil, err
			}

			var timestampUnix int64
			err = binary.Read(file, binary.BigEndian, &timestampUnix)
			if err != nil {
				fmt.Println("Greška prilikom čitanja vremenske oznake:", err)
				return nil, err
			}
			timestamp := time.Unix(timestampUnix, 0)

			var tombstoneByte byte
			err = binary.Read(file, binary.BigEndian, &tombstoneByte)
			if err != nil {
				fmt.Println("Greška prilikom čitanja Tombstone:", err)
				return nil, err
			}
			tombstone := tombstoneByte == 1

			var flushFlagByte byte
			err = binary.Read(file, binary.BigEndian, &flushFlagByte)
			if err != nil {
				fmt.Println("Greska pri citanju flush flag-a", err)
				return nil, err
			}
			flushFlag := flushFlagByte == 1

			var keySize int64
			err = binary.Read(file, binary.BigEndian, &keySize)
			if err != nil {
				fmt.Println("Greška prilikom čitanja veličine ključa:", err)
				return nil, err
			}

			var valueSize int64
			err = binary.Read(file, binary.BigEndian, &valueSize)
			if err != nil {
				fmt.Println("Greška prilikom čitanja veličine vrednosti:", err)
				return nil, err
			}

			key := make([]byte, keySize)
			_, err = file.Read(key)
			if err != nil {
				fmt.Println("Greška prilikom čitanja ključa:", err)
				return nil, err
			}

			value := make([]byte, valueSize)
			_, err = file.Read(value)
			if err != nil {
				fmt.Println("Greška prilikom čitanja vrednosti:", err)
				return nil, err
			}

			record := Record{
				FlushFlag: flushFlag,
				Timestamp: timestamp,
				Tombstone: tombstone,
				Key:       key,
				Value:     value,
			}

			records = append(records, record)
		}
		file.Close()
	}

	// for _, entry := range records {
	// 	fmt.Printf("Timestamp: %s, Tombstone: %v, Key: %s, Value: %s\n",
	// 		entry.Timestamp.Format(time.RFC3339), entry.Tombstone, entry.Key, entry.Value)
	// }
	return records, nil
}

func (w *WAL) CleanUpWal() {
	files, err := fp.Glob(fp.Join(w.dir, "wal_*"))
	if err != nil {
		fmt.Println(err)
		return
	}

	//If there is less than w.lwm wal segments we won't remove any
	if len(files) <= w.lwm {
		fmt.Println("Nema dovoljno segmenata za ciscenje.")
		return
	}

	// slices.Reverse(files)
	// Remove files with index lower than the low watermark
	for _, file := range files {
		var index int
		_, err := fmt.Sscanf(fp.Base(file), "wal_%03d", &index)
		if err == nil && index <= w.lwm {
			err := os.Remove(file)
			if err != nil {
				fmt.Println(err)
				return
			}
		}
	}

	files, _ = fp.Glob(fp.Join(w.dir, "wal_*"))

	// Update indexes to start with 0 again
	for i, file := range files {
		newIndex := i
		newFilename := fp.Join(w.dir, fmt.Sprintf("wal_%03d", newIndex))
		err := os.Rename(file, newFilename)
		if err != nil {
			fmt.Println(err)
			return
		}
		w.index = newIndex
	}
}

func (w *WAL) ReadSegment(path string) ([]Record, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 066)
	var records []Record
	if err != nil {
		return nil, err
	}

	for {
		var crc uint32
		err := binary.Read(file, binary.BigEndian, &crc)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Greška prilikom čitanja crc:", err)
			return nil, err
		}

		var timestampUnix int64
		err = binary.Read(file, binary.BigEndian, &timestampUnix)
		if err != nil {
			fmt.Println("Greška prilikom čitanja vremenske oznake:", err)
			return nil, err
		}
		timestamp := time.Unix(timestampUnix, 0)

		var tombstoneByte byte
		err = binary.Read(file, binary.BigEndian, &tombstoneByte)
		if err != nil {
			fmt.Println("Greška prilikom čitanja Tombstone:", err)
			return nil, err
		}
		tombstone := tombstoneByte == 1

		var flushFlagByte byte
		err = binary.Read(file, binary.BigEndian, &flushFlagByte)
		if err != nil {
			fmt.Println("Greska pri citanju flush flag-a", err)
			return nil, err
		}
		flushFlag := flushFlagByte == 1

		var keySize int64
		err = binary.Read(file, binary.BigEndian, &keySize)
		if err != nil {
			fmt.Println("Greška prilikom čitanja veličine ključa:", err)
			return nil, err
		}

		var valueSize int64
		err = binary.Read(file, binary.BigEndian, &valueSize)
		if err != nil {
			fmt.Println("Greška prilikom čitanja veličine vrednosti:", err)
			return nil, err
		}

		key := make([]byte, keySize)
		_, err = file.Read(key)
		if err != nil {
			fmt.Println("Greška prilikom čitanja ključa:", err)
			return nil, err
		}

		value := make([]byte, valueSize)
		_, err = file.Read(value)
		if err != nil {
			fmt.Println("Greška prilikom čitanja vrednosti:", err)
			return nil, err
		}

		record := Record{
			FlushFlag: flushFlag,
			Timestamp: timestamp,
			Tombstone: tombstone,
			Key:       key,
			Value:     value,
		}

		records = append(records, record)
	}
	file.Close()

	return records, nil

}

func (w *WAL) Recover() ([]Record, error) {
	recovery_log := make([]Record, 0)
	wal_dir, err := os.Open(w.dir)
	if err != nil {
		return nil, err
	}
	segments, err := wal_dir.ReadDir(0)
	if err != nil {
		return nil, err
	}
	segment_paths := make([]string, 0)
	for _, v := range segments {
		segment_paths = append(segment_paths, fp.Join(w.dir, v.Name()))
	}
	sort.StringSlice.Sort(segment_paths)

	for i := len(segment_paths) - 1; i >= 0; i-- {
		segment, err := w.ReadSegment(segment_paths[i])
		if err != nil {
			return nil, err
		}
		for j := len(segment) - 1; j >= 0; j-- {
			if segment[j].FlushFlag {
				slices.Reverse(recovery_log)
				return recovery_log, nil
			}
			recovery_log = append(recovery_log, segment[j])
		}
	}

	slices.Reverse(recovery_log)
	return recovery_log, nil
}
