package app

import (
	conf "go-touch-grass/config"
	"go-touch-grass/internal/cache"
	"go-touch-grass/internal/wal"
	"os"
	fp "path/filepath"
	"runtime"
)

type App struct {
	datapath string
	config   *conf.Config
	cache    *cache.Cache
	wal      *wal.WAL
	// tbucket  *tbucket.TBucket
	// lsm *lsm.LSMTree
}

func New() *App {
	config := conf.New(getConfigPath())
	return &App{
		datapath: getDataPath(),
		config:   config,
		cache:    cache.NewCache(config.CacheSize),
		wal:      wal.New(fp.Join(getDataPath(), "wal"), config),
		// tbucket:  tbucket.New(),
		// lsm: lsm.New()
	}
}

func (app *App) Put(key string, data []byte) {
	// wal.Put()
	// LSM.Put(asdsds)
}

func (app *App) Get(key string) ([]byte, bool) {
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
	// LSM.Delete(key)
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
