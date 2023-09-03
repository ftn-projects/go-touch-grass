package memtable

import (
	"errors"
	"fmt"
	conf "go-touch-grass/config"
	"go-touch-grass/internal/hash"
	"go-touch-grass/pkg/btree"
	"go-touch-grass/pkg/skiplist"
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

func (mt *Memtable) putRecord(key string, data []byte, tombstone bool) error {
	_, contains := mt.table.Get(key)
	if !contains && mt.IsFull() {
		return errors.New("pokusaj dodavanja u punu memoriju")
	}
	record := Record{
		Crc:       hash.GetCrc(key, data),
		Timestamp: time.Now(),
		Tombstone: tombstone,
		Key:       key,
		Data:      data,
	}
	mt.table.Put(key, record)
	return nil
}

func (mt *Memtable) Put(key string, data []byte) error {
	return mt.putRecord(key, data, false)
}

func (mt *Memtable) Delete(key string) error {
	return mt.putRecord(key, nil, true)
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

func GetExample(config *conf.Config) *Memtable {
	mem := New(config)
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
