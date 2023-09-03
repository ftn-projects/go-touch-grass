package app

import (
	conf "go-touch-grass/config"
	"go-touch-grass/internal/cache"
	"go-touch-grass/internal/lsmtree"
	"go-touch-grass/internal/tbucket"
	"go-touch-grass/internal/wal"
	"os"
	fp "path/filepath"
	"time"
)

type App struct {
	datapath string
	config   *conf.Config
	cache    *cache.Cache
	wal      *wal.WAL
	lsm      *lsmtree.LSMTree
	tbucket  *tbucket.TBucket
}

func New() *App {
	config := conf.New(getConfigPath())
	return &App{
		datapath: getDataPath(),
		config:   config,
		cache:    cache.New(config.CacheSize),
		wal:      wal.New(getWalPath(), config),
		lsm:      lsmtree.New(config, getDataPath()),
		tbucket:  tbucket.New(config),
	}
}

func (app *App) CanMakeQuery() error {
	return app.tbucket.MakeQuery()
}

func (app *App) Put(key string, data []byte) (err error) {
	err = app.tbucket.MakeQuery()
	if err != nil {
		return
	}

	wal_record := wal.NewRecord(time.Now(), false, []byte(key), data)
	err = app.wal.WriteRecord(*wal_record)
	if err != nil {
		return
	}

	err, _ = app.lsm.Put(key, data)
	// if flushed {
	// 	app.wal.CleanUpWal()
	// }
	return
}

func (app *App) Get(key string) (data []byte, err error) {
	err = app.tbucket.MakeQuery()
	if err != nil {
		return
	}

	data, deleted := app.lsm.GetFromMemtable(key)
	if data != nil || deleted {
		return
	}

	// Check if in cache
	data = app.cache.Get(key)
	if data != nil {
		return
	}

	data, err = app.lsm.GetFromDisc(key)
	if err != nil {
		return
	}
	if data != nil {
		app.cache.Add(key, data)
	}
	return
}

func (app *App) Delete(key string) (err error) {
	err = app.tbucket.MakeQuery()
	if err != nil {
		return
	}

	err, _ = app.lsm.Delete(key)
	// if flushed {
	// 	app.wal.CleanUpWal()
	// }
	return
}

func (app *App) InitiateCompaction() error {
	return app.lsm.CompactLevel(1)
}

func getConfigPath() string {
	return fp.Join("./config", "config.yaml")
}

func getDataPath() string {
	path := fp.Join("./data")
	_, err := os.Stat(path)
	if err != nil {
		os.Mkdir(path, 0755)
	}
	return path
}

func getWalPath() string {
	path := fp.Join("./wal")
	_, err := os.Stat(path)
	if err != nil {
		os.Mkdir(path, 0755)
	}
	return path
}
