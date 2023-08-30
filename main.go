package main

import (
	"bufio"
	"go-touch-grass/cli"
	"go-touch-grass/config"
	"go-touch-grass/internal/sstable"
	"os"
)

func main() {
	cli.MainMenu()
	conf := config.New()
	sstable.NewSSTable(conf).WriteNewSSTable()
	table := sstable.GetSSTable(0)
	scaner := bufio.NewScanner(os.Stdin)
	for scaner.Scan() {
		offset := table.Index.Find(scaner.Text())
		if offset != -1 {
			table.Read(offset)
		}
	}

	// data := [][]byte{
	// 	[]byte("data1"),
	// 	[]byte("data2"),
	// 	[]byte("data3"),
	// 	[]byte("data4"),
	// 	[]byte("data5"),
	// }
	// m1 := merkle.NewMerkleTree(&data)
	// m1.Save("./data/SSTables/metadata.txt")
	// m2 := merkle.TryLoad("./data/SSTables/metadata.txt")
	// m2.Save("./data/SSTables/metadata2.txt")
	// fmt.Println(m2)
}
