package summary

import (
	"go-touch-grass/internal/util"
	"io"
	"math"
)

type Summary struct {
	keys    []string
	offsets []uint64
}

// kreiranje novog summary
func New(step int, ikeys []string, ioffsets []uint64) *Summary {
	size := int(math.Ceil(float64(len(ikeys))/float64(step))) + 1
	keys := make([]string, size)
	offsets := make([]uint64, size)

	j := 0
	for i := 0; i < len(ikeys); i += step { //svaki i*step key ubacuje u keys
		keys[j] = ikeys[i]
		offsets[j] = ioffsets[i]
		j++
	}
	keys[j] = ikeys[len(ikeys)-1]
	offsets[j] = ioffsets[len(ioffsets)-1]

	return &Summary{keys, offsets}
}

// ucitavanje prvog i poslednjeg
func DeserializeHeader(r io.Reader) (string, string, int) {
	sz1, _ := util.ReadUint32(r)
	first, _ := util.ReadString(int(sz1), r)
	sz2, _ := util.ReadUint32(r)
	last, _ := util.ReadString(int(sz2), r)
	return first, last, int(sz1) + int(sz2) + 8
}

func IsBetweenKeys(first, last, key string) bool {
	return first <= key && last >= key
}

// ucitavanje ostatka summary
func Deserialize(r io.Reader, size int) *Summary {
	keys := make([]string, 0)
	offsets := make([]uint64, 0)

	read := 0
	for read < size {
		sz, _ := util.ReadUint32(r)
		key, _ := util.ReadString(int(sz), r)
		offset, _ := util.ReadUint64(r)

		keys = append(keys, key)
		offsets = append(offsets, offset)
		read += int(sz) + 12
	}
	return &Summary{keys, offsets}
}

func (s *Summary) Serialize(w io.Writer) int {
	sum := 0
	first := s.keys[0]
	last := s.keys[len(s.keys)-1]
	util.WriteUint(uint32(len(first)), w)
	util.WriteString(first, w)
	util.WriteUint(uint32(len(last)), w)
	util.WriteString(last, w)
	sum += len(first) + len(last)

	for i, k := range s.keys {
		util.WriteUint(uint32(len(k)), w)
		util.WriteString(k, w)
		util.WriteUint(s.offsets[i], w)
		sum += len(k)
	}
	return sum + len(s.keys)*12 + 8
}

// pronalazenje izmedju koja dva kljuca se nalazi kljuc koji trazimo i njegov offset
func (s *Summary) GetOffset(key string) (uint64, uint64) {
	for i := 0; i < len(s.keys)-1; i++ {
		if IsBetweenKeys(s.keys[i], s.keys[i+1], key) {
			return s.offsets[i], s.offsets[i+1]
		}
	}
	panic("Kljuc se ne nalazi u ovom summary")
}
