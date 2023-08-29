package main

import (
	"bufio"
	"go-touch-grass/config"
	"go-touch-grass/internal/sstable"
	"os"
)

func main() {
	conf := config.New()
	sstable.NewSSTable(conf).WriteNewSSTable()
	table := sstable.GetSSTable(0)
	scaner := bufio.NewScanner(os.Stdin)
	for scaner.Scan() {
		offset := table.Index.Find(scaner.Text())
		table.Read(offset)
	}
}
