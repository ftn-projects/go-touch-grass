package lsmtree

import (
	"go-touch-grass/internal/sstable"
	"os"
)

type ssTableIterator struct {
	table *sstable.SSTable
	file  *os.File
}

func newIterator(toc *sstable.TOC) *ssTableIterator {
	table := sstable.GetSSTable(toc)
	file, _ := os.OpenFile(table.Toc.DataPath, os.O_RDONLY, 0666)
	return &ssTableIterator{
		table: table,
		file:  file,
	}
}

func (it *ssTableIterator) End() bool {
	return it.file == nil
}

func (it *ssTableIterator) Read() sstable.DataElement {
	record := sstable.ReadNextDataRecord(it.file)
	offset, _ := it.file.Seek(0, 1)

	if offset >= int64(it.table.Toc.DataSize) {
		it.file.Close()
		it.file = nil
	}
	return record
}
