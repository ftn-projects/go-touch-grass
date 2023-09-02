package sstable

// Aleksa Vukomanovic SV66/2021
// SSTable struktura - Data Segment i Index segment

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"go-touch-grass/config"
	"go-touch-grass/internal/bloom"
	"go-touch-grass/internal/memtable"
	"go-touch-grass/internal/summary"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

type TOC struct {
	DataPath      string
	DataSize      uint64
	FilterPath    string
	FilterOffset  int64
	FilterSize    uint64
	IndexPath     string
	IndexOffest   int64
	IndexSize     uint64
	SummaryPath   string
	SummaryOffset int64
	SummarySize   uint64
}

type SSTable struct {
	FilePathBase       string
	DataSegmentPath    string
	DataSegmentSize    uint64
	SummarySegmentPath string
	SummaryOffset      int64
	SummarySize        uint64
	FilterPath         string
	FilterOffset       int64
	FilterSize         uint64
	TOCFilePath        string
	Index              *Index
}

func NewSSTable(conf *config.Config, level string) *SSTable {
	// Creating file paths for new SSTable
	// Modify it for LSM Tree levels
	table := &SSTable{FilePathBase: "./data/" + level + "/usertable-"}
	gen := fmt.Sprintf("%03d", GetNextGeneration(level))
	if !conf.SSTableAllInOne {
		table.DataSegmentPath = table.FilePathBase + gen + "-data.db"
		table.FilterPath = table.FilePathBase + gen + "-filter.db"
		table.SummarySegmentPath = table.FilePathBase + gen + "-summary.db"
		table.TOCFilePath = table.FilePathBase + gen + "-TOC.yaml"
		table.Index = NewIndex(table.FilePathBase+gen+"-index.db", 0, 0)
	} else {
		temp := table.FilePathBase + gen + "-SSTable.db"
		table.DataSegmentPath = temp
		table.FilterPath = temp
		table.Index = NewIndex(temp, 0, 0)
		table.SummarySegmentPath = temp
		table.TOCFilePath = table.FilePathBase + gen + "-TOC.yaml"

	}

	return table
}

func (sstable *SSTable) WriteNewSSTable(data []memtable.Record, c config.Config) {
	// Function used for making SSTable on Disk
	// Getting data from memtable (traversing a BTree or SkipList)
	// Saving data in data segment, creating index and serializing it
	// Creating bloomfilter and summary structures and saving them
	// Parameters:
	//	- data : data from memtable
	//	- isOneFile : if we should save all structures in one file or separate
	file_offset := uint64(0)

	keys := make([]string, len(data))
	offsets := make([]uint64, len(data))

	bf := bloom.New(uint64(len(data)), c.FilterPrecision)

	data_file, err := os.Create(sstable.DataSegmentPath)
	if err != nil {
		panic(err)
	}

	defer data_file.Close()
	writer := bufio.NewWriter(data_file)

	for i, v := range data {
		// Adding a key to bloom filter
		bf.Add(v.Key)

		key := []byte(v.Key)
		keySize := uint64(len(key))
		value := v.Data
		valueSize := uint64(len(value))
		tombstone := v.Tombstone

		timestamp := v.Timestamp
		timestampBytes := make([]byte, 16)
		binary.BigEndian.PutUint64(timestampBytes[:8], uint64(timestamp.Unix()))
		binary.BigEndian.PutUint64(timestampBytes[8:], uint64(timestamp.Nanosecond()))

		WrittenBytes := 0

		keys[i] = v.Key
		offsets[i] = file_offset

		if err = binary.Write(writer, binary.BigEndian, v.Crc); err != nil {
			panic(err)
		}
		if err = binary.Write(writer, binary.BigEndian, timestampBytes); err != nil {
			panic(err)
		}

		if err = binary.Write(writer, binary.BigEndian, tombstone); err != nil {
			panic(err)
		}

		if err = binary.Write(writer, binary.BigEndian, keySize); err != nil {
			panic(err)
		}

		if err = binary.Write(writer, binary.BigEndian, valueSize); err != nil {
			panic(err)
		}

		if WrittenBytes, err = writer.Write(key); err != nil {
			panic(err)
		}

		file_offset += uint64(WrittenBytes)

		if WrittenBytes, err = writer.Write(value); err != nil {
			panic(err)
		}

		file_offset += uint64(WrittenBytes) + 37 // Broj zapisanih i 2 puta po 8 bajta za zapis velicine segmenata + Tombstone
		if err = writer.Flush(); err != nil {
			panic(err)
		}
		writer.Reset(data_file)

	}

	sstable.DataSegmentSize = uint64(file_offset)
	var key_offsets []uint64
	// Creating index segment
	sstable.Index.offset = 0
	if c.SSTableAllInOne {
		sstable.Index.offset = int64(file_offset)
		key_offsets = sstable.Index.CreateIndexSegment(keys, offsets)
		file_offset += sstable.Index.size
		data_file.Seek(int64(file_offset), 0)
	} else {
		key_offsets = sstable.Index.CreateIndexSegment(keys, offsets)
	}

	// Creating Summary
	s := summary.New(c.SummaryStep, keys, key_offsets)
	sstable.SummaryOffset = 0
	if c.SSTableAllInOne {
		sstable.SummaryOffset = int64(file_offset)
		sstable.SummarySize = uint64(s.Serialize(data_file))
		file_offset += sstable.SummarySize
		data_file.Seek(int64(file_offset), 0)
	} else {
		sfile, _ := os.Create(sstable.SummarySegmentPath)
		sstable.SummarySize = uint64(s.Serialize(sfile))
		sfile.Close()
	}

	// Creating filter segment
	sstable.FilterOffset = 0
	if c.SSTableAllInOne {
		sstable.FilterOffset = int64(file_offset)
		sstable.FilterSize = uint64(bf.Serialize(data_file))
		file_offset += sstable.FilterSize
		data_file.Seek(int64(file_offset), 0)
	} else {
		bffile, _ := os.Create(sstable.FilterPath)
		sstable.FilterSize = uint64(bf.Serialize(bffile))
		bffile.Close()
	}

	sstable.CreateTOC()
	return
}

func (sstable *SSTable) Read(offset int64) ([]byte, bool) {
	// Function used to read segment from DataSegment
	data_file, err := os.OpenFile(sstable.DataSegmentPath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	data_file.Seek(offset, 1)
	temp := ReadNextDataRecord(data_file)
	// Mozda je bolje vratiti value nazad funkciji koja poziva ovu funkciju
	return temp.Value, temp.Tombstone
}

func (sstable *SSTable) CreateTOC() {
	// Creating Table of Contents and saving it in file
	toc := &TOC{}
	toc.DataPath = sstable.DataSegmentPath
	toc.IndexPath = sstable.Index.indexfile
	toc.IndexOffest = int64(sstable.Index.offset)
	toc.IndexSize = uint64(sstable.Index.size)
	toc.FilterPath = sstable.FilterPath
	toc.FilterSize = sstable.FilterSize
	toc.FilterOffset = sstable.FilterOffset
	toc.SummaryOffset = sstable.SummaryOffset
	toc.SummarySize = sstable.SummarySize
	toc.SummaryPath = sstable.SummarySegmentPath
	toc.DataSize = sstable.DataSegmentSize
	toc.Save(sstable.TOCFilePath)
}

func GetNextGeneration(level string) int {
	// Utility function used for getting the next number for generation
	// Return:
	// 	- Last generation + 1
	file, err := os.Open("./data/" + level)
	if err != nil {
		panic(err)
	}

	fileinfo, err := file.ReadDir(0)
	if err != nil {
		panic(err)
	}

	if len(fileinfo) == 0 {
		return 1
	}

	max := 1
	for _, v := range fileinfo {
		filename := strings.Split(v.Name(), "-")
		if filename[len(filename)-1] == "TOC.yaml" {
			t, err := strconv.Atoi(filename[len(filename)-2])
			if err != nil {
				panic(err)
			}
			if t > max {
				max = t
			}
		}
	}
	return max + 1
}

func GetTOC(toc_path string) *TOC {
	// Loading a TOC
	// Parameters:
	//	- generation : selecting a sstable to pick, if it is 0 we return the last made SSTable
	// Return:
	//	- Pointer to TOC

	file, err := os.OpenFile(toc_path, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	scaner := bufio.NewScanner(file)
	files := make([]string, 0)
	for {
		scaner.Scan()
		if scaner.Text() == "" {
			break
		}
		files = append(files, scaner.Text())
	}
	toc, _ := tryLoad(file.Name())

	return toc
}

func (toc *TOC) Save(path string) {
	// Function used for saving Table of Contents file
	data, _ := yaml.Marshal(toc)
	os.WriteFile(path, data, 0644)
}

func tryLoad(path string) (*TOC, bool) {
	// Function used for loading Table of Contents file
	c := TOC{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	err = yaml.Unmarshal(data, &c)
	return &c, err == nil
}

func ReadNextDataRecord(file *os.File) DataElement {
	// Utility function used for reading next element in data segment
	// Parameters:
	//	- file : opened file that is already seeked on a corresponding postion
	// Return:
	//	- data record
	reader := bufio.NewReader(file)
	crc := make([]byte, 4)

	timestampBytes := make([]byte, 16)

	_, err := reader.Read(crc)
	crcv := binary.BigEndian.Uint32(crc)
	if err != nil {
		panic(err)
	}
	_, err = reader.Read(timestampBytes)
	if err != nil {
		panic(err)
	}

	timeSecond := int64(binary.BigEndian.Uint64(timestampBytes[:8]))
	timeNanoseconds := int64(binary.BigEndian.Uint64(timestampBytes[8:]))
	timestamp := time.Unix(timeSecond, timeNanoseconds)

	tombstoneB, err := reader.ReadByte()
	if err != nil {
		panic(err)
	}
	tombstone := tombstoneB != 0

	keySizeB := make([]byte, 8)
	_, err = reader.Read(keySizeB)

	if err != nil {
		panic(err)

	}
	keySize := binary.BigEndian.Uint64(keySizeB)

	valueSizeB := make([]byte, 8)
	_, err = reader.Read(valueSizeB)
	if err != nil {
		panic(err)
	}
	valueSize := binary.BigEndian.Uint64(valueSizeB)
	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	_, err = reader.Read(key)

	if err != nil {
		panic(err)
	}

	_, err = reader.Read(value)
	if err != nil {
		panic(err)
	}

	return DataElement{CRC: crcv, Timestamp: timestamp, Tombstone: tombstone, KeySize: keySize, ValueSize: valueSize, Key: string(key), Value: value}
}

func GetSSTable(toc *TOC) *SSTable {
	t := &SSTable{}
	t.DataSegmentSize = toc.DataSize
	t.DataSegmentPath = toc.DataPath
	t.FilterPath = toc.FilterPath
	t.FilterOffset = toc.FilterOffset
	t.FilterSize = toc.FilterSize
	t.SummarySegmentPath = toc.SummaryPath
	t.SummaryOffset = toc.SummaryOffset
	t.SummarySize = toc.SummarySize
	t.Index = NewIndex(toc.IndexPath, int64(toc.IndexOffest), uint64(toc.IndexSize))

	return t
}

func (t *SSTable) QueryBloomFilter(key string) bool {
	file, err := os.OpenFile(t.FilterPath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	file.Seek(t.FilterOffset, 0)
	bf := bloom.Deserialize(file)
	return bf.Has(key)
}

func (t *SSTable) QuerySummary(key string) (int64, int64) {
	summary_file, _ := os.Open(t.SummarySegmentPath)
	defer summary_file.Close()
	summary_file.Seek(t.SummaryOffset, 0)
	first_key, last_key, bytes_read := summary.DeserializeHeader(summary_file)
	if summary.IsBetweenKeys(first_key, last_key, key) {
		summary_file.Seek(t.SummaryOffset+int64(bytes_read), 0)
		s := summary.Deserialize(summary_file, int(t.SummarySize-uint64(bytes_read)))
		first, last := s.GetOffset(key)

		return int64(first), int64(last)
	}
	return -1, -1
}
