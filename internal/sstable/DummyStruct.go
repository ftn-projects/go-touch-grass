package sstable

type dummy struct {
	key       string
	value     []byte
	tombstone byte
}
