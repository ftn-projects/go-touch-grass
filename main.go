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
	table := sstable.GetSSTable(1)
	table.Index.ReadIndex()
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
