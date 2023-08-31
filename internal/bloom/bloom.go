package bloom

import (
	"bytes"
	"encoding/gob"
	"math"
	"os"
)

type BloomFilter struct {
	m, k   int // k - broj hash f-ija, m - velicina niza
	filter []int
	hashes []HashWithSeed
}

// kreiranje novog bloom filtera
func New(n int, p float64) *BloomFilter {
	init := &BloomFilter{}
	init.m = -int(math.Ceil((float64(n) * math.Log(p)) / math.Pow(math.Log(2), 2)))
	init.k = int(math.Ceil(float64(init.m) / float64(n) * math.Log(2)))
	init.filter = make([]int, init.m)
	init.hashes = createHashFunctions(uint(init.k))
	return init
}

// dodavanje  novog podatka u bloom filter
func (bf *BloomFilter) Add(x []byte) {
	for _, h := range bf.hashes {
		i := h.hash(x) % uint64(bf.m)
		bf.filter[i] = 1
	}
}

// provera da li se podatak mozda nalazi u bloom filteru
func (bf *BloomFilter) Has(x []byte) bool {
	for _, h := range bf.hashes {
		i := h.hash(x) % uint64(bf.m)
		if bf.filter[i] == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Serialize(f *os.File) int {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(bf)
	f.Write(buf.Bytes())
	return buf.Len()
}

func DeserializeFilter(f *os.File, len int) *BloomFilter {
	b := make([]byte, len)
	f.Read(b)
	var buf bytes.Buffer
	buf.Write(b)
	c := gob.NewDecoder(&buf)
	bf := &BloomFilter{}
	c.Decode(bf)
	return bf
}
