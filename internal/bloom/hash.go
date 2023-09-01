package bloom

import (
	"crypto/md5"
	"encoding/binary"
	"time"
)

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashes(count uint64) []HashWithSeed {
	h := make([]HashWithSeed, count)
	ts := uint64(time.Now().Unix())
	for i := uint64(0); i < count; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}
