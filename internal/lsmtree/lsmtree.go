package lsmtree

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"go-touch-grass/config"
	"go-touch-grass/internal/bloom"
	"go-touch-grass/internal/memtable"
	"go-touch-grass/internal/sstable"
	"go-touch-grass/internal/summary"
	"io"
	"os"
	fp "path/filepath"
	"sort"
	"strconv"
	"strings"
)

type LSMTree struct {
	memtable   *memtable.Memtable
	max_level  uint
	level_size uint
	levels     []string
	conf       config.Config
	dataPath   string
}

func formatLevel(level int) string {
	return fmt.Sprintf("%03d", level)
}

func (lsm *LSMTree) LoadTocPaths(level int) []string {
	tocs := make([]string, 0)
	folder, _ := os.Open(lsm.levels[level-1])
	content, err := folder.ReadDir(0)
	if err != nil {
		return []string{}
	}
	for _, v := range content {
		temp := strings.Split(v.Name(), "-")
		if temp[len(temp)-1] == "TOC.yaml" {
			tocs = append(tocs, "./data/level-"+formatLevel(level)+"/"+v.Name())
		}
	}

	sort.StringSlice.Sort(tocs)
	return tocs
}

func (lsm *LSMTree) LevelFull(level int) bool {
	return len(lsm.LoadTocPaths(level)) >= int(lsm.level_size)
}

func (lsm *LSMTree) CreateNewLevel() {
	max_num_lvl := lsm.getMaxLevelNumber() + 1
	if max_num_lvl >= lsm.conf.LsmMaxLevel {
		fmt.Println("Dostignuta maksimalna visina LSM stabla.")
	}

	newDir := fp.Join(lsm.dataPath, "level-"+formatLevel(max_num_lvl))
	err := os.Mkdir(newDir, 0755)
	if err != nil {
		panic(err)
	}
	lsm.levels = append(lsm.levels, newDir)
}

func (lsm *LSMTree) getMaxLevelNumber() int {
	max_num_lvl := -1
	for _, v := range lsm.levels {
		t := strings.Split(v, "-")
		num, _ := strconv.Atoi(t[len(t)-1])
		if num > max_num_lvl {
			max_num_lvl = num
		}
	}
	return max_num_lvl
}

func New(conf config.Config, dataPath string) *LSMTree {
	lsm := &LSMTree{}
	lsm.conf = conf
	lsm.dataPath = dataPath
	lsm.memtable = memtable.New(&conf)
	_, err := os.Open(dataPath)
	if err != nil {
		err = os.Mkdir(dataPath, 0755)
		if err != nil {
			panic("Check path in config file")
		}
	}
	data_folder, _ := os.Open(dataPath)

	level, _ := data_folder.ReadDir(0)
	lsm.levels = make([]string, 0)
	if len(level) == 0 {
		first_lvl := fp.Join(dataPath, "level-001")
		_, err := os.Stat(first_lvl)
		if err != nil {
			os.Mkdir(first_lvl, 0755)
		}
		lsm.levels = append(lsm.levels, first_lvl)
	} else {
		for i := 0; i < len(level); i++ {
			lsm.levels = append(lsm.levels, fp.Join(dataPath, level[i].Name()))
		}
	}
	lsm.max_level = uint(conf.LsmMaxLevel)
	lsm.level_size = uint(conf.LsmLevelSize)
	sort.StringSlice.Sort(lsm.levels)
	return lsm
}

func writeRecord(w io.Writer, rec *sstable.DataElement) uint64 {
	n := 37
	tsBytes := make([]byte, 16)
	binary.BigEndian.PutUint64(tsBytes[:8], uint64(rec.Timestamp.Unix()))
	binary.BigEndian.PutUint64(tsBytes[8:], uint64(rec.Timestamp.Nanosecond()))
	binary.Write(w, binary.BigEndian, rec.CRC)
	binary.Write(w, binary.BigEndian, tsBytes)
	binary.Write(w, binary.BigEndian, rec.Tombstone)
	binary.Write(w, binary.BigEndian, rec.KeySize)
	binary.Write(w, binary.BigEndian, rec.ValueSize)
	k, _ := w.Write([]byte(rec.Key))
	v, _ := w.Write(rec.Value)
	return uint64(n + k + v)
}

func getMinRecord(records []*sstable.DataElement) (int, []int) {
	min := -1
	for i, rec := range records {
		if rec == nil {
			continue
		} else if min == -1 {
			min = i
		} else if rec.Key < records[min].Key ||
			(rec.Key == records[min].Key && rec.Timestamp.Unix() >
				records[min].Timestamp.Unix()) {
			min = i
		}
	}

	var toRefresh []int
	for i, rec := range records {
		if rec != nil && rec.Key == records[min].Key {
			toRefresh = append(toRefresh, i)
		}
	}
	return min, toRefresh
}

func (lsm *LSMTree) CompactLevel(level int) {
	if level >= int(lsm.max_level) {
		return
	}

	if len(lsm.levels) == level {
		lsm.CreateNewLevel()
	}
	table := sstable.NewSSTable(&lsm.conf, lsm.dataPath, "level-"+formatLevel(level+1))

	toc_paths := lsm.LoadTocPaths(level)
	iterators := make([]*ssTableIterator, len(toc_paths))
	records := make([]*sstable.DataElement, len(toc_paths))
	for i, toc_path := range toc_paths {
		toc := sstable.GetTOC(toc_path)
		iterators[i] = newIterator(toc)
	}

	record_count := uint64(lsm.conf.MemtableCap * len(iterators))
	bf := bloom.New(record_count, lsm.conf.FilterPrecision)

	data_file, _ := os.Create(table.Toc.DataPath)
	w := bufio.NewWriter(data_file)

	var keys []string
	var offsets []uint64
	position := uint64(0)

	for i, iterator := range iterators {
		records[i] = iterator.Read()
	}
	for {
		min, toRead := getMinRecord(records)
		if len(toRead) == 0 {
			break
		}

		rec := records[min]
		// if !rec.Tombstone {
		bf.Add(rec.Key)
		keys = append(keys, rec.Key)
		offsets = append(offsets, position)
		position += writeRecord(w, rec)
		w.Flush()
		w.Reset(data_file)
		// }

		for _, i := range toRead {
			records[i] = iterators[i].Read()
		}
	}
	data_file.Close()
	table.Toc.DataSize = position

	// Creating index segment
	var ioffsets []uint64
	if lsm.conf.SSTableAllInOne {
		table.Index.Offset = int64(position)
		ioffsets = table.Index.CreateIndexSegment(keys, offsets)
		position += table.Index.Size
		data_file.Seek(int64(position), 0)
	} else {
		table.Index.Offset = 0
		ioffsets = table.Index.CreateIndexSegment(keys, offsets)
	}

	// Creating Summary
	s := summary.New(lsm.conf.SummaryStep, keys, ioffsets)
	table.Toc.SummaryOffset = 0
	if lsm.conf.SSTableAllInOne {
		table.Toc.SummaryOffset = int64(position)
		table.Toc.SummarySize = uint64(s.Serialize(data_file))
		position += table.Toc.SummarySize
		data_file.Seek(int64(position), 0)
	} else {
		sfile, _ := os.Create(table.Toc.SummaryPath)
		table.Toc.SummarySize = uint64(s.Serialize(sfile))
		sfile.Close()
	}

	// Creating filter segment
	table.Toc.FilterOffset = 0
	if lsm.conf.SSTableAllInOne {
		table.Toc.FilterOffset = int64(position)
		table.Toc.FilterSize = uint64(bf.Serialize(data_file))
		position += table.Toc.FilterSize
		data_file.Seek(int64(position), 0)
	} else {
		bffile, _ := os.Create(table.Toc.FilterPath)
		table.Toc.FilterSize = uint64(bf.Serialize(bffile))
		bffile.Close()
	}
	table.CreateTOC()
	table.CreateMerkle()
	DeleteDirContent(lsm.levels[level-1])

	if lsm.LevelFull(level + 1) {
		lsm.CompactLevel(level + 1)
	}
}

func (lsm *LSMTree) GetFromMemtable(key string) ([]byte, bool) {
	// Povratna vrdnost podaci i da li je obrisan
	record, found := lsm.memtable.Get(key)
	if found {
		if !record.Tombstone {
			return record.Data, false
		}
		return nil, true
	}
	return nil, false
}

func (lsm *LSMTree) GetFromDisc(key string) (data []byte, tombstone bool) {
	for i := 1; i <= len(lsm.levels); i++ {
		level := lsm.LoadTocPaths(i)
		for j := len(level) - 1; j >= 0; j-- {
			TOC := sstable.GetTOC(level[j])
			table := sstable.GetSSTable(TOC)
			if table.QueryBloomFilter(key) {
				start, end := table.QuerySummary(key)
				if start >= 0 && end >= 0 {
					keyelm := table.Index.FindBetweenRange(key, start, end)
					if keyelm != nil {
						return table.Read(keyelm.Offset)

					}
				}
			}
		}
	}
	return nil, false
}

func (lsm *LSMTree) Put(key string, data []byte) {
	// Funkcija za stavljanje u memtable
	lsm.memtable.Put(key, data)
	if lsm.memtable.IsFull() {
		lsm.FlushMemtable()
	}
}
func (lsm *LSMTree) Delete(key string) {
	lsm.memtable.Delete(key)
	if lsm.memtable.IsFull() {
		lsm.FlushMemtable()
	}
}

func (lsm *LSMTree) FlushMemtable() {
	// Treba proveriti svaki nivo da li je slucajno dosao do prekoracenja
	table := sstable.NewSSTable(&lsm.conf, lsm.dataPath, "level-001")
	table.WriteNewSSTable(lsm.memtable.GetAll(), lsm.conf)
	lsm.memtable.Clear()
	if lsm.LevelFull(1) {
		lsm.CompactLevel(1)
	}
}

func DeleteDirContent(path string) {
	d, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	files, err := d.ReadDir(0)
	if err != nil {
		panic(err)
	}
	for _, v := range files {
		file_path := fp.Join(path, v.Name())
		err := os.Remove(file_path)
		if err != nil {
			panic(err)
		}
	}

}
