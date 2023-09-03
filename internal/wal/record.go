package wal

import "time"

type Record struct {
	FlushFlag bool
	Timestamp time.Time
	Tombstone bool
	Key       []byte
	Value     []byte
}

func NewRecord(timestamp time.Time, tombstone bool, key []byte, value []byte) *Record {
	return &Record{Timestamp: timestamp, Tombstone: tombstone, Key: key, Value: value}
}
