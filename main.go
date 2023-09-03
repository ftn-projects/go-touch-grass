package main

import (
	"fmt"
	"go-touch-grass/config"
	"go-touch-grass/internal/app"
	"go-touch-grass/internal/lsmtree"
)

func main() {
	conf := config.New(app.GetConfigPath())
	lsm := lsmtree.New(*conf, app.GetDataPath())
	lsm.Put("r", []byte("a"))
	lsm.Put("s", []byte("b"))
	lsm.Put("s", []byte("g"))
	lsm.Put("a", []byte("c"))

	lsm.Delete("a")
	lsm.Delete("r")
	lsm.Put("a", []byte("//"))
	lsm.Delete("r")
	fmt.Println(lsm.GetFromMemtable("a"))
	fmt.Println(lsm.GetFromDisc("a"))

	// mem := memtable.GetExample()
	// sstable.NewSSTable(conf).WriteNewSSTable(mem.GetAll(), conf.SSTableAllInOne)
	// toc := sstable.GetTOC(0)
	// table := sstable.GetSSTable(toc)
	// table.Index.Find("aaa")
	// fmt.Println(table.QueryBloomFilter("aaa"))
	// scaner := bufio.NewScanner(os.Stdin)
	// for scaner.Scan() {
	// 	v := scaner.Text()
	// 	if table.QueryBloomFilter(v) {
	// 		start, end := table.QuerySummary(v)
	// 		if start >= 0 && end >= 0 {
	// 			keyelm := table.Index.FindBetweenRange(v, start, end)
	// 			if keyelm != nil {
	// 				table.Read(keyelm.Offset)
	// 			}
	// 		}
	// 	}
	// }

	//IGNORE OVO -Arezina
	// myCache := cache.NewCache(5)
	// myCache.Add("key1", []byte("value1"))
	// myCache.Add("key2", []byte("value2"))
	// myCache.Add("key3", []byte("value3"))

	// //I have become death, destroyer of key3
	// myCache.Remove("key3")

	// myCache.Add("key4", []byte("value4"))
	// myCache.Add("key5", []byte("value5"))
	// myCache.Add("key6", []byte("value6"))
	// myCache.Add("key7", []byte("value7"))

	// myCache.PrintCache()

	// myCache.Clear()

	// myCache.PrintCache()
}
