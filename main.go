package main

import (
	"bufio"
	"go-touch-grass/cli"
	"go-touch-grass/config"
	"go-touch-grass/internal/sstable"
	"os"
)

var global int = 0

func main() {
	cli.MainMenu()
	conf := config.New()
	sstable.NewSSTable(conf).WriteNewSSTable()
	table := sstable.GetSSTable(0)
	scaner := bufio.NewScanner(os.Stdin)
	for scaner.Scan() {
		offset := table.Index.Find(scaner.Text())
		table.Read(offset)
	}
}
