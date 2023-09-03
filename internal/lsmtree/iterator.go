package lsmtree

import (
	"go-touch-grass/internal/sstable"
	"os"
)

type ssTableIterator struct {
	table    *sstable.SSTable
	file     *os.File
	position uint64
}

func newIterator(toc *sstable.TOC) *ssTableIterator {
	table := sstable.GetSSTable(toc)
	file, _ := os.OpenFile(table.Toc.DataPath, os.O_RDONLY, 0666)
	return &ssTableIterator{
		table: table,
		file:  file,
	}
}

func (it *ssTableIterator) Read() *sstable.DataElement {
	if it.file == nil {
		return nil
	}

	it.file.Seek(int64(it.position), 0)
	record, b := sstable.ReadNextDataRecord(it.file)
	it.position += b

	if it.position >= it.table.Toc.DataSize {
		it.file.Close()
		it.file = nil
	}
	return &record
}
