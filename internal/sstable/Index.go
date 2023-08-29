package sstable

// Izmeni za cuvanje offseta
import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

type Index struct {
	indexfile string
	offset    int64
	size      uint64
}

type IndexElement struct {
	KeySize uint64
	Key     string
	Offset  int64
}

func NewIndex(filename string, offset int64, size uint64) *Index {
	i := &Index{}
	i.indexfile = filename
	i.offset = offset
	i.size = size
	return i
}

func (index *Index) Find(key string) int64 {
	file, err := os.OpenFile(index.indexfile, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	file.Seek(index.offset, 0)

	for {
		el := &IndexElement{}
		temp := make([]byte, 8)

		_, err := reader.Read(temp)
		if err != nil {
			return -1
		}
		// reading key size
		el.KeySize = binary.BigEndian.Uint64(temp)
		temp = make([]byte, el.KeySize)
		_, err = reader.Read(temp)
		if err != nil {
			panic(err)
		}

		// reading key
		el.Key = string(temp)
		temp = make([]byte, 8)
		reader.Read(temp)

		// reading offset
		if key == el.Key {
			el.Offset = int64(binary.BigEndian.Uint64(temp))
			return el.Offset
		}
	}
}
func (index *Index) ReadIndex() {
	file, err := os.OpenFile(index.indexfile, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)
	i, _ := file.Seek(index.offset, 0)

	for i <= int64(index.size)+index.offset {
		keySizeB := make([]byte, 8)
		_, err := reader.Read(keySizeB)
		if err != nil {
			return
		}
		keySize := binary.BigEndian.Uint64(keySizeB)
		keyB := make([]byte, keySize)
		reader.Read(keyB)
		offsetB := make([]byte, 8)
		reader.Read(offsetB)
		offset := binary.BigEndian.Uint64(offsetB)
		fmt.Println(keySize, "    ", string(keyB), "    ", offset)
		i += int64(16 + keySize)
	}
}
func (index *Index) CreateIndexSegment(indexes map[string]uint) {
	offset := int64(0)
	file, err := os.OpenFile(index.indexfile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	offset, _ = file.Seek(index.offset, 0)

	for k, v := range indexes {
		key := []byte(k)
		keySize := uint64(len(key))

		if err := binary.Write(writer, binary.BigEndian, keySize); err != nil {
			panic(err)
		}

		if _, err := writer.Write(key); err != nil {
			panic(err)
		}
		if err := binary.Write(writer, binary.BigEndian, uint64(v)); err != nil {
			panic(err)
		}
		offset += offset + 16 + int64(keySize)
		if err := writer.Flush(); err != nil {
			panic(err)
		}
		writer.Reset(file)
	}
	index.size = uint64(offset - index.offset)
}
