package lsmtree

import (
	"errors"
	"fmt"
	"go-touch-grass/config"
	"go-touch-grass/internal/memtable"
	"go-touch-grass/internal/sstable"
	"os"
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
}

func (lsm *LSMTree) LoadLevel(level int) []string {
	result := make([]string, 0)
	folder, err := os.Open(lsm.levels[level-1])
	content, err := folder.ReadDir(0)
	if err != nil {
		panic(err)
	}
	for _, v := range content {
		temp := strings.Split(v.Name(), "-")
		if temp[len(temp)-1] == "TOC.yaml" {
			result = append(result, "./data/level-"+fmt.Sprintf("%03d", level)+"/"+v.Name())
		}
	}

	sort.StringSlice.Sort(result)
	return result

}
func (lsm *LSMTree) CheckLevelMaxSize(level int) {
	if len(lsm.LoadLevel(level)) >= int(lsm.level_size) {
		lsm.Compaction(level)
	}

}
func (lsm *LSMTree) CreateNewLevel() {
	max_num_lvl := lsm.getMaxLevelNumber() + 1
	newDir := "./data/level-" + fmt.Sprintf("%03d", max_num_lvl)
	err := os.Mkdir(newDir, 0755)
	lsm.levels = append(lsm.levels, newDir)
	if err != nil {
		panic(err)
	}

}

func (lsm *LSMTree) getMaxLevelNumber() int {
	max_num_lvl := -1
	for _, v := range lsm.levels {
		t := strings.Split(v, "-")
		num, _ := strconv.Atoi(t[1])
		if num > max_num_lvl {
			max_num_lvl = num
		}
	}
	return max_num_lvl
}

func New(conf config.Config) *LSMTree {
	lsm := &LSMTree{}
	lsm.conf = conf
	lsm.memtable = memtable.New(&conf)
	data_folder, err := os.Open("./data")
	if err != nil {
		err = os.Mkdir("./data", 0755)
		if err != nil {
			panic(errors.New("Check path in config file"))
		}
	}
	data_folder, err = os.Open("./data")

	level, err := data_folder.ReadDir(0)
	lsm.levels = make([]string, 0)
	if len(level) == 0 {
		first_lvl := "./data/level-001"
		lsm.levels = append(lsm.levels, first_lvl)
		os.Mkdir(first_lvl, 0755)
	} else {
		for i := 0; i < len(level); i++ {
			lsm.levels = append(lsm.levels, "./data/"+level[i].Name())
		}
	}
	sort.StringSlice.Sort(lsm.levels)
	return lsm
}

func (lsm *LSMTree) Compaction(level int) {
	// TO-DO kreirati funkciju za pravljenje nove SSTabele sa drugim SSTabelama, potrebno je ucitati svaki
	// TOC i zatim svaku SSTable strukturu, nakon toga se cita sekvencijalno upisuju podaci i cuva index u memoriji
	// nazalost ne moze unapred znati velicinu potrebnu za data segment :(
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

func (lsm *LSMTree) GetFromDisc(key string) ([]byte, bool) {
	for i := 1; i <= len(lsm.levels); i++ {
		level := lsm.LoadLevel(i)
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
	table := sstable.NewSSTable(&lsm.conf, "level-001")
	table.WriteNewSSTable(lsm.memtable.GetAll(), lsm.conf)
	lsm.CheckLevelMaxSize(1)
	lsm.memtable.Clear()
}
