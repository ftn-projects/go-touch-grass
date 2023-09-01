package app

import (
	conf "go-touch-grass/config"
	"go-touch-grass/internal/memtable"
	"os"
	fp "path/filepath"
	"runtime"
)

type App struct {
	datapath string
	config   *conf.Config
	table    *memtable.Memtable
	// cache    *cache.Cache
	// wal      *wal.Wal
}

func New() *App {
	config := conf.New(getConfigPath())
	return &App{
		datapath: getDataPath(),
		config:   config,
		table:    memtable.New(config),
	}
}

func (app *App) Put(key string, data []byte) {
	app.table.Put(key, data)
	// wal.Put()
	// check if table full -> flush to sstable
	//                        table.Clear()
	//                        cache.Clear()
}

func (app *App) Get(key string) ([]byte, bool) {
	// check if in memtable
	// in cache
	// access level 1 sstables, level 2 sstables etc
	return nil, false
}

func (app *App) Delete(key string) {
	// app.table.Delete(key) ?
}

// kasnije kad smislimo da li ide drvo po sstabeli ili po nivou
func (app *App) FormMerkleTrees() {

}

func (app *App) InitiateCompaction(level int) {
	// if level full -> compact level to higher (Milicin deo)
}

func getRootPath() string {
	_, path, _, _ := runtime.Caller(0)
	return fp.Dir(fp.Dir(fp.Dir(path)))
}

func getConfigPath() string {
	return fp.Join(getRootPath(), "config", "config.yaml")
}

func getDataPath() string {
	path := fp.Join(getRootPath(), "data")
	_, err := os.Stat(path)
	if err != nil {
		os.Mkdir(path, 0666)
	}
	return path
}
