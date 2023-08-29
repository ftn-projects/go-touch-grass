package memtable

import (
	conf "go-touch-grass/config"
	"go-touch-grass/pkg/btree"
	"go-touch-grass/pkg/skiplist"
)

type Container interface {
	Put(key string, data interface{})
	Find(key string) (data interface{}, found bool)
	GetAll() []interface{}
}

type Record struct {
	Key       string
	Tombstone bool
	Data      []byte
}

type Memtable struct {
	table Container
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

	return &Memtable{table}
}
