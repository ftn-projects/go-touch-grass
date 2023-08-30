package sstable

import "time"

type DataElement struct {
	CRC       uint64
	Timestamp time.Time
	Tombstone bool
	KeySize   uint64
	Key       string
	ValueSize uint64
	Value     []byte
}
