package main

import (
	"fmt"
	"go-touch-grass/internal/memtable"
)

func main() {
	mem := memtable.GetExample()
	fmt.Println("full: ", mem.IsFull())
	mem.Print()
}
