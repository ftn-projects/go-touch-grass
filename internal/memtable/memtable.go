package memtable

import (
	conf "go-touch-grass/config"
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

func New(config conf.Config) *Memtable {
	aa := skiplist.New(5)
	return &Memtable{aa}
}
