package memtable

import (
	"fmt"
	conf "go-touch-grass/config"
	"go-touch-grass/pkg/btree"
	"go-touch-grass/pkg/skiplist"
)

type Container interface {
	Put(key string, data interface{})
	Get(key string) (data interface{}, found bool)
	GetAll() []interface{}
	Size() int
	Clear()
}

type Record struct {
	Key       string
	Tombstone bool
	Data      []byte
}

type Memtable struct {
	table Container
	cap   int
}

func New(c *conf.Config) *Memtable {
	var table Container

	switch c.MemtableContainer {
	case "skiplist":
		table = skiplist.New(c.SkiplistMaxHeight)
	case "btree":
		table = btree.New(c.BtreeDegree)
	default:
		panic("error in config file (MemtableContainer field)")
	}
	return &Memtable{table, c.MemtableCap}
}

func (mt *Memtable) Put(key string, data []byte) {
	_, contains := mt.table.Get(key)
	if !contains && mt.IsFull() {
		panic("adding new record to a full memtable")
	}

	record := Record{key, false, data}
	mt.table.Put(key, record)
}

func (mt *Memtable) Delete(key string) {
	record := Record{key, true, nil}
	mt.table.Put(key, record)
}

func (mt *Memtable) Get(key string) []byte {
	data, found := mt.table.Get(key)
	if !found {
		panic("memtable does not contain provided key")
	}
	return data.(Record).Data
}

func (mt *Memtable) IsFull() bool {
	return mt.table.Size() >= mt.cap
}

func (mt *Memtable) Clear() {
	mt.table.Clear()
}

func (mt *Memtable) GetAll() []Record {
	records := make([]Record, mt.table.Size())
	for i, rec := range mt.table.GetAll() {
		records[i] = rec.(Record)
	}
	return records
}

func (mt *Memtable) Print() {
	fmt.Println("[")
	for _, rec := range mt.GetAll() {
		fmt.Println("    ", rec)
	}
	fmt.Println("]")
}

func GetExample() *Memtable {
	mem := New(conf.New())
	mem.Put("aaa", []byte("aaa"))
	mem.Put("bbb", []byte("bbb"))
	mem.Put("ccc", []byte("ccc"))
	mem.Put("ddd", []byte("ddd"))
	mem.Put("eee", []byte("eee"))
	mem.Put("fff", []byte("fff"))
	mem.Put("ggg", []byte("ggg"))
	mem.Put("hhh", []byte("hhh"))
	mem.Put("iii", []byte("iii"))
	mem.Put("jjj", []byte("jjj"))

	mem.Put("ccc", []byte("yyy"))
	mem.Put("ddd", []byte("zzz"))

	mem.Delete("aaa")
	mem.Delete("bbb")
	return mem
}
