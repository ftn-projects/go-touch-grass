package main

import (
	"fmt"
	conf "go-touch-grass/config"
	"go-touch-grass/internal/memtable"
)

func main() {
	mt := memtable.New(conf.Config{})
	fmt.Println(mt)
	conf.New()
}
