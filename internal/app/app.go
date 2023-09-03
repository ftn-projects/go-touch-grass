package app

import (
	"fmt"
	conf "go-touch-grass/config"
	"go-touch-grass/internal/cache"
	"go-touch-grass/internal/tbucket"
	"go-touch-grass/internal/wal"
	"os"
	fp "path/filepath"
)

type App struct {
	datapath string
	config   *conf.Config
	cache    *cache.Cache
	wal      *wal.WAL
	tbucket  *tbucket.TBucket
	// lsm *lsm.LSMTree
}

func New() *App {
	config := conf.New(GetConfigPath())
	return &App{
		datapath: GetDataPath(),
		config:   config,
		cache:    cache.New(config.CacheSize),
		wal:      wal.New(fp.Join(GetDataPath(), "wal"), config),
		tbucket:  tbucket.New(config),
		// lsm: lsm.New()
	}
}

func (app *App) CanMakeQuery() bool {
	success := app.tbucket.MakeQuery()
	if !success {
		fmt.Println("Previse zahteva molimo sacekajte.")
		return false
	}
	return true
}

func (app *App) Put(key string, data []byte) {
	if !app.CanMakeQuery() {
		return
	}
	// wal.Put()
	// LSM.Put(asdsds)
}

func (app *App) Get(key string) ([]byte, bool) {
	if !app.CanMakeQuery() {
		return nil, false
	}
	// LSM.MemtableGet(key)

	// Check if in cache
	valueCache, foundCache := app.cache.Get(key)
	if foundCache {
		return valueCache, true
	}

	// LSM.SstableGet(key)
	// cache.Put(key, data)

	return nil, false
}

func (app *App) Delete(key string) {
	if !app.CanMakeQuery() {
		return
	}
	// LSM.Delete(key)
}

func GetConfigPath() string {
	return fp.Join("./config", "config.yaml")
}

func GetDataPath() string {
	path := fp.Join("./data")
	_, err := os.Stat(path)
	if err != nil {
		os.Mkdir(path, 0755)
	}
	return path
}
