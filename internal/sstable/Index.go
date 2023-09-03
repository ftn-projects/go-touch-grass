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
	Indexfile string
	Offset    int64
	Size      uint64
}

type IndexElement struct {
	KeySize uint64
	Key     string
	Offset  int64
}

func NewIndex(filename string, offset int64, size uint64) *Index {
	i := &Index{}
	i.Indexfile = filename
	i.Offset = offset
	i.Size = size
	return i
}

func (index *Index) Find(key string) int64 {
	// Function used for searching key in index structure by iterating through
	// whole index strcture
	// Parameters:
	//	- key : A key that we are searching for
	// Return:
	//	- offset where should we seek for our data or -1 key was not found
	file, err := os.OpenFile(index.Indexfile, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	i := index.Offset
	for i < index.Offset+int64(index.Size) {
		file.Seek(i, 0) // mora je izgleda da reader if bufio sam pozicionira file na kraj??
		el, bytesRead := ReadNextIndexRecord(file)
		if el.Key == key {
			return el.Offset
		}
		i += bytesRead
	}
	return -1
}

func (index *Index) PrintIndex() {
	// Function used for testing
	// Iterating through index structure and printing all values
	file, err := os.OpenFile(index.Indexfile, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	i, _ := file.Seek(index.Offset, 0)

	for i < int64(index.Size)+index.Offset {
		el, bytesRead := ReadNextIndexRecord(file)
		fmt.Println(el.KeySize, "    ", string(el.Key), "    ", el.Offset)
		i += bytesRead
		file.Seek(i, 0)
	}
	file.Close()
}
func (index *Index) CreateIndexSegment(keys []string, offsets []uint64) []uint64 {
	// Function used for creating index structure/segment
	// It can be in same file as data or in different file
	// index structure attributes are used for getting file path, offset where to write
	// Parameters:
	//	- keys : sorted arrays of key that index should contains
	//	- offsets : arrays of number that correspond to a key at same postion and postion of data in data segment

	offset := int64(0)
	key_offsets := make([]uint64, len(keys))

	file, err := os.OpenFile(index.Indexfile, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := bufio.NewWriter(file)

	offset, _ = file.Seek(index.Offset, 0)

	for i := 0; i < len(keys); i++ {
		key := []byte(keys[i])
		keySize := uint64(len(keys[i]))
		key_offsets[i] = uint64(offset)
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
	index.Size = uint64(offset - index.Offset)
	return key_offsets
}

func ReadNextIndexRecord(file *os.File) (*IndexElement, int64) {
	// Utility function used for reading next key in index structure
	// Paramteres :
	//	-opened file already Seeked on position
	// Return value :
	// 	-pointer to index element and number of bytes that were read
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
	// Function used for scaning a part of index structure
	// Is used in combination with Summary structure
	// Parameters :
	//	- key : A key that we are searching for
	//	- lower_bound : offset in file from where we begin our scanning
	//	- upper_bound : offset where search would end if key was not found
	// Return Value : Index element which contains: Key, KeySize and Offset of that Record in Data Segment
	file, err := os.OpenFile(index.Indexfile, os.O_RDONLY, 0666)

	if err != nil {
		panic(err)
	}

	if lower_bound < index.Offset || lower_bound > int64(index.Size+uint64(index.Offset)) {
		panic(errors.New("Out of range"))
	}

	if upper_bound > int64(index.Size+uint64(index.Offset)) || upper_bound < index.Offset {
		panic(errors.New("Out of range"))
	}
	if lower_bound > upper_bound {
		panic(errors.New("Lower bound larger than upper bound"))
	}

	i, _ := file.Seek(lower_bound, 0)
	for i <= upper_bound {
		el, bytesRead := ReadNextIndexRecord(file)
		if el.Key == key {
			return el
		}
		i += bytesRead
		file.Seek(i, 0)
	}

	return nil
}
