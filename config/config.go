package config

import (
	"os"
	"runtime"

	"gopkg.in/yaml.v2"
)

type Config struct {
	path              string
	SkiplistMaxHeight int
	BtreeDegree       int
	MemtableCap       int
	MemtableContainer string
	FilterPrecision   float64
	SummaryStep       int
}

func (c Config) Save() {
	data, _ := yaml.Marshal(c)
	os.WriteFile(c.path, data, 0644)
}

func getConfigPath() string {
	_, path, _, _ := runtime.Caller(0)
	return path[:len(path)-len("config.go")] + "config.yaml"
}

func tryLoad(path string) (*Config, bool) {
	c := Config{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	err = yaml.Unmarshal(data, &c)
	return &c, err == nil
}

func getDefault() *Config {
	return &Config{
		SkiplistMaxHeight: 10,
		BtreeDegree:       4,
		MemtableCap:       10,
		MemtableContainer: "skiplist",
		FilterPrecision:   0.01,
		SummaryStep:       5,
	}
}

func New() *Config {
	path := getConfigPath()
	conf, ok := tryLoad(path)
	if !ok {
		conf = getDefault()
	}

	conf.path = path
	conf.Save()
	return conf
}
