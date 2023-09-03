package app

import (
	"errors"
	conf "go-touch-grass/config"
	"go-touch-grass/internal/cache"
	"go-touch-grass/internal/lsmtree"
	"go-touch-grass/internal/tbucket"
	"go-touch-grass/internal/wal"
	"math"
	"os"
	fp "path/filepath"
	"strconv"
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

func New() (*App, error) {
	config, err := conf.New(getConfigPath())
	if err != nil {
		return nil, err
	}

	app := &App{
		datapath: getDataPath(),
		config:   config,
		cache:    cache.New(config.CacheSize),
		wal:      wal.New(getWalPath(), config),
		lsm:      lsmtree.New(config, getDataPath()),
		tbucket:  tbucket.New(config),
	}
	app.StartRecovery()
	return app, nil
}

func (app *App) UnlockTokenBucket() {
	app.tbucket = tbucket.New(&conf.Config{
		TBucketResetDuration: math.MaxInt64,
		TBucketMaxTokens:     math.MaxInt,
	})
}

func (app *App) CanMakeQuery() error {
	return app.tbucket.MakeQuery()
}

func (app *App) StartRecovery() (err error) {
	recovery_log, err := app.wal.Recover()
	if err != nil {
		return
	}
	if recovery_log == nil {
		return nil
	}
	for _, v := range recovery_log {
		app.lsm.Put(string(v.Key), v.Value)
	}
	return nil
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

	err, flushed := app.lsm.Put(key, data)
	if flushed {
		app.wal.WriteRecord(wal.Record{
			Timestamp: time.Now(),
			FlushFlag: true,
		})
		app.cache.Clear()
	}
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

	err, flushed := app.lsm.Delete(key)
	if flushed {
		app.wal.WriteRecord(wal.Record{
			Timestamp: time.Now(),
			FlushFlag: true,
		})
		app.cache.Clear()
	}
	return
}

func (app *App) InitiateCompaction(level int) error {
	if app.lsm.LevelEmpty(level) {
		return errors.New("uneti nivo je prazan (nema SSTabeli za kompakciju)")
	} else if level > app.lsm.LevelCount() {
		max := strconv.FormatInt(int64(app.lsm.LevelCount()), 10)
		return errors.New("uneti nivo je veci od trenutno maksimalnog (" + max + ")")
	} else if level == app.config.LsmMaxLevel {
		return errors.New("kompakcija poslednjeg nivoa nije moguca")
	}
	return app.lsm.CompactLevel(level)
}

func (app *App) CleanupWal() {
	app.wal.CleanUpWal()
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
