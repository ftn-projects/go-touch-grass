package bloom

import (
	"go-touch-grass/internal/hash"
	"go-touch-grass/internal/util"
	"io"
	"math"
)

type BloomFilter struct {
	m, k   uint32 // k - broj hash f-ija, m - velicina niza
	filter []uint16
	hashes []hash.SeededHash
}

// kreiranje novog bloom filtera
func New(n uint64, p float64) *BloomFilter {
	init := &BloomFilter{}
	init.m = -uint32(math.Ceil((float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2)))
	init.k = uint32(math.Ceil(float64(init.m) / float64(n) * math.Log(2)))
	init.filter = make([]uint16, init.m)
	init.hashes = hash.NewHashes(uint(init.k))
	return init
}

// dodavanje  novog podatka u bloom filter
func (bf *BloomFilter) Add(key string) {
	b := []byte(key)
	for _, h := range bf.hashes {
		i := uint32(h.Hash(b)) % bf.m
		bf.filter[i] = 1
	}
}

// provera da li se podatak mozda nalazi u bloom filteru
func (bf *BloomFilter) Has(key string) bool {
	b := []byte(key)
	for _, h := range bf.hashes {
		i := uint32(h.Hash(b)) % bf.m
		if bf.filter[i] == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Serialize(w io.Writer) int {
	util.WriteUint(bf.m, w)
	util.WriteUint(bf.k, w)
	for _, f := range bf.filter {
		util.WriteUint(f, w)
	}
	for _, h := range bf.hashes {
		util.WriteBytes(h.Seed, w)
	}
	return 8 + 2*int(bf.m+bf.k*16)
}

func Deserialize(r io.Reader) *BloomFilter {
	m, _ := util.ReadUint32(r)
	k, _ := util.ReadUint32(r)
	filter := make([]uint16, m)
	hashes := make([]hash.SeededHash, k)
	for i := uint32(0); i < m; i++ {
		filter[i], _ = util.ReadUint16(r)
	}
	for i := uint32(0); i < k; i++ {
		seed, _ := util.ReadBytes(32, r)
		hashes[i] = hash.SeededHash{Seed: seed}
	}
	return &BloomFilter{m, k, filter, hashes}
}
