package main

import (
	"bufio"
	"go-touch-grass/internal/sstable"
	"os"
)

func main() {
	// conf := config.New()
	// mem := memtable.GetExample()
	// sstable.NewSSTable(conf).WriteNewSSTable(mem.GetAll(), conf.SSTableAllInOne)
	toc := sstable.GetTOC(0)
	table := sstable.GetSSTable(toc)
	table.Index.Find("aaa")

	scaner := bufio.NewScanner(os.Stdin)
	for scaner.Scan() {
		offset := table.Index.Find(scaner.Text())
		if offset != -1 {
			table.Read(offset)
		}
	}

}
