package config

import (
	"os"

	"gopkg.in/yaml.v2"
)

type Config struct {
	path                 string
	SkiplistMaxHeight    int
	BtreeDegree          int
	MemtableCap          int
	MemtableContainer    string
	SSTableAllInOne      bool
	FilterPrecision      float64
	SummaryStep          int
	CacheSize            int
	WalLowWaterMark      int
	WalSegmentSize       int64
	TBucketResetDuration int64
	TBucketMaxTokens     int
	LsmMaxLevel          int
	LsmLevelSize         int
	MerkleChunkSize      int64
}

func (c Config) Save() {
	data, _ := yaml.Marshal(c)
	os.WriteFile(c.path, data, 0644)
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
		SkiplistMaxHeight:    10,
		BtreeDegree:          4,
		MemtableCap:          2,
		MemtableContainer:    "skiplist",
		SSTableAllInOne:      false,
		FilterPrecision:      0.01,
		SummaryStep:          5,
		CacheSize:            10,
		WalLowWaterMark:      10,
		WalSegmentSize:       1024 * 1024,
		TBucketResetDuration: 2000,
		TBucketMaxTokens:     5,
		LsmMaxLevel:          4,
		LsmLevelSize:         2,
		MerkleChunkSize:      100,
	}
}

func New(path string) *Config {
	conf, ok := tryLoad(path)
	if !ok {
		conf = getDefault()
	}

	conf.path = path
	conf.Save()
	return conf
}
