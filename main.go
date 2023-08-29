package main

import (
	conf "go-touch-grass/config"
	"go-touch-grass/pkg/btree"
)

func main() {
	btree.New(2)
	conf.New()
}
