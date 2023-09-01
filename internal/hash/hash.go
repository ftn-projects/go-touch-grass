package hash

import (
	"crypto/md5"
	"encoding/binary"
	"hash/crc32"
	"time"
)

type SeededHash struct {
	Seed []byte
}

func (h SeededHash) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func NewHashes(count uint) []SeededHash {
	h := make([]SeededHash, count)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < count; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := SeededHash{Seed: seed}
		h[i] = hfn
	}
	return h
}

func GetCrc(key string, data []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte(key))
	h.Write(data)
	sum := h.Sum32()
	h.Reset()
	return sum
}
