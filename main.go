package main

import (
	"go-touch-grass/cli"
	"go-touch-grass/internal/app"
)

func main() {
	application := app.New()
	cli.NewMenu(application).Show()
}
