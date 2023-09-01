package memtable

import (
	"fmt"
	conf "go-touch-grass/config"
	"go-touch-grass/pkg/btree"
	"go-touch-grass/pkg/skiplist"
	"hash"
	"hash/crc32"
	"time"
)

type Container interface {
	Put(key string, data interface{})
	Get(key string) (data interface{}, found bool)
	GetAll() []interface{}
	Size() int
	Clear()
}

type Record struct {
	Crc       uint32
	Timestamp time.Time
	Tombstone bool
	Key       string
	Data      []byte
}

type Memtable struct {
	table Container
	cap   int
	h     hash.Hash32
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
	return &Memtable{table, c.MemtableCap, crc32.NewIEEE()}
}

func (mt *Memtable) getCrc(key string, data []byte) uint32 {
	mt.h.Write([]byte(key))
	mt.h.Write(data)
	sum := mt.h.Sum32()
	mt.h.Reset()
	fmt.Println(sum)
	return sum
}

func (mt *Memtable) putRecord(key string, data []byte, tombstone bool) {
	record := Record{
		Crc:       mt.getCrc(key, data),
		Timestamp: time.Now(),
		Tombstone: tombstone,
		Key:       key,
		Data:      data,
	}
	mt.table.Put(key, record)
}

func (mt *Memtable) Put(key string, data []byte) {
	_, contains := mt.table.Get(key)
	if !contains && mt.IsFull() {
		panic("adding new record to a full memtable")
	}
	mt.putRecord(key, data, false)
}

func (mt *Memtable) Delete(key string) {
	mt.putRecord(key, nil, true)
}

func (mt *Memtable) Get(key string) (Record, bool) {
	data, found := mt.table.Get(key)
	if !found {
		return Record{}, false
	}
	return data.(Record), true
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
