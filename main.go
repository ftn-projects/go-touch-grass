package main

import (
	"go-touch-grass/cli"
	"go-touch-grass/config"
)

func main() {
	config.New()
	cli.MainMenu()
}
