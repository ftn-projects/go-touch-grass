package main

func main() {
	// conf := config.New()
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
}
