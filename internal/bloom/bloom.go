package bloom

import (
	"go-touch-grass/internal/util"
	"io"
	"math"
)

type BloomFilter struct {
	m, k   uint64 // k - broj hash f-ija, m - velicina niza
	filter []uint16
	hashes []HashWithSeed
}

// kreiranje novog bloom filtera
func New(n uint64, p float64) *BloomFilter {
	init := &BloomFilter{}
	init.m = -uint64(math.Ceil((float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2)))
	init.k = uint64(math.Ceil(float64(init.m) / float64(n) * math.Log(2)))
	init.filter = make([]uint16, init.m)
	init.hashes = CreateHashes(init.k)
	return init
}

// dodavanje  novog podatka u bloom filter
func (bf *BloomFilter) Add(key string) {
	b := []byte(key)
	for _, h := range bf.hashes {
		i := h.hash(b) % bf.m
		bf.filter[i] = 1
	}
}

// provera da li se podatak mozda nalazi u bloom filteru
func (bf *BloomFilter) Has(key string) bool {
	b := []byte(key)
	for _, h := range bf.hashes {
		i := h.hash(b) % bf.m
		if bf.filter[i] == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Serialize(w io.Writer) int {
	util.WriteNumber(bf.m, w)
	util.WriteNumber(bf.k, w)
	for _, f := range bf.filter {
		util.WriteNumber(f, w)
	}
	for _, h := range bf.hashes {
		util.WriteBytes(h.Seed, w)
	}
	return 16 + 2*int(bf.m+bf.k*16)
}

func Deserialize(r io.Reader) *BloomFilter {
	m, _ := util.ReadUint64(r)
	k, _ := util.ReadUint64(r)
	filter := make([]uint16, m)
	hashes := make([]HashWithSeed, k)
	for i := uint64(0); i < m; i++ {
		filter[i], _ = util.ReadUint16(r)
	}
	for i := uint64(0); i < k; i++ {
		seed, _ := util.ReadBytes(32, r)
		hashes[i] = HashWithSeed{Seed: seed}
	}
	return &BloomFilter{m, k, filter, hashes}
}
