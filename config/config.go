package config

import (
	"errors"
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
	MerkleChunkSize      int
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

func validConfig(c *Config) error {
	err_message := "neispravan config fajl "
	ints := []int64{
		int64(c.SkiplistMaxHeight),
		int64(c.BtreeDegree),
		int64(c.MemtableCap),
		int64(c.SummaryStep),
		int64(c.CacheSize),
		int64(c.WalLowWaterMark),
		c.WalSegmentSize,
		c.TBucketResetDuration,
		int64(c.TBucketMaxTokens),
		int64(c.LsmMaxLevel),
		int64(c.LsmLevelSize),
		int64(c.MerkleChunkSize),
	}
	for _, v := range ints {
		if v <= 0 {
			return errors.New(err_message + "(negativan broj)")
		}
	}
	if c.FilterPrecision <= 0 || c.FilterPrecision >= 1 {
		return errors.New(err_message + "(FilterPrecision)")
	}
	if c.MemtableContainer != "skiplist" && c.MemtableContainer != "btree" {
		return errors.New(err_message + "(MemtableContainer)")
	}
	return nil
}

func GetDefault() *Config {
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

func New(path string) (*Config, error) {
	conf, ok := tryLoad(path)
	if !ok {
		conf = GetDefault()
	}

	err := validConfig(conf)
	if err != nil {
		return nil, err
	}

	conf.path = path
	conf.Save()
	return conf, nil
}
