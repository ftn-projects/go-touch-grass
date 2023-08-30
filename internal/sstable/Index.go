package sstable

// Izmeni za cuvanje offseta
import (
	"bufio"
	"encoding/binary"
	"errors"
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
	// Funkcija vraca offset gde se nalazi u data segmetnu podatak
	// ili vraca -1 ako ne nadje podatak
	file, err := os.OpenFile(index.indexfile, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	defer file.Close()
	i := index.offset
	for i < index.offset+int64(index.size) {
		el, bytesRead := ReadNextIndexRecord(file)
		if el.Key == key {
			return el.Offset
		}
		i += bytesRead
		file.Seek(i, 0) // mora je izgleda da reader if bufio sam pozicionira file na kraj??
	}
	return -1
}

func (index *Index) PrintIndex() {
	file, err := os.OpenFile(index.indexfile, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	i, _ := file.Seek(index.offset, 0)

	for i < int64(index.size)+index.offset {
		el, bytesRead := ReadNextIndexRecord(file)
		fmt.Println(el.KeySize, "    ", string(el.Key), "    ", el.Offset)
		i += bytesRead
		file.Seek(i, 0)
	}
	file.Close()
}
func (index *Index) CreateIndexSegment(keys []string, offsets []uint) {
	offset := int64(0)
	file, err := os.OpenFile(index.indexfile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	offset, _ = file.Seek(index.offset, 0)

	for i := 0; i < len(keys); i++ {
		key := []byte(keys[i])
		keySize := uint64(len(keys[i]))

		if err := binary.Write(writer, binary.BigEndian, keySize); err != nil {
			panic(err)
		}

		if _, err := writer.Write(key); err != nil {
			panic(err)
		}
		if err := binary.Write(writer, binary.BigEndian, uint64(offsets[i])); err != nil {
			panic(err)
		}
		offset += 16 + int64(keySize)
		if err := writer.Flush(); err != nil {
			panic(err)
		}
		writer.Reset(file)
	}
	index.size = uint64(offset - index.offset)
}

func ReadNextIndexRecord(file *os.File) (*IndexElement, int64) {

	reader := bufio.NewReader(file)

	el := &IndexElement{}

	// reading key size
	temp := make([]byte, 8)
	_, err := reader.Read(temp)
	if err != nil {
		panic(err)
	}
	el.KeySize = binary.BigEndian.Uint64(temp)

	// reading key
	temp = make([]byte, el.KeySize)
	_, err = reader.Read(temp)
	if err != nil {
		panic(err)
	}
	el.Key = string(temp)

	// reading offset
	temp = make([]byte, 8)
	_, err = reader.Read(temp)
	if err != nil {
		panic(err)
	}
	i := 16
	i += int(el.KeySize)
	el.Offset = int64(binary.BigEndian.Uint64(temp))
	return el, int64(i)
}
func (index *Index) FindBetweenRange(key string, lower_bound int64, upper_bound int64) *IndexElement {
	// Funkcija koja se koristi za pretragu segmenta Index strukture
	file, err := os.OpenFile(index.indexfile, os.O_RDONLY, 0666)

	if err != nil {
		panic(err)
	}

	if lower_bound < index.offset || lower_bound > int64(index.size) {
		panic(errors.New("Out of range"))
	}

	if upper_bound > int64(index.size) || upper_bound < index.offset {
		panic(errors.New("Out of range"))
	}
	if lower_bound > upper_bound {
		panic(errors.New("Lower bound larger than upper bound"))
	}

	i, _ := file.Seek(lower_bound, 0)
	for i < upper_bound {
		el, bytesRead := ReadNextIndexRecord(file)
		if el.Key == key {
			return el
		}
		i += bytesRead
		file.Seek(i, 0)
	}

	return nil
}
