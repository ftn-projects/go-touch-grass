package sstable

// Aleksa Vukomanovic SV66/2021
// SSTable struktura - Data Segment i Index segment

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"go-touch-grass/config"
	"go-touch-grass/internal/memtable"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

type ISSTable interface {
	WriteNewSSTable()
}
type TOC struct {
	DataPath      string
	FilterPath    string
	FilterOffset  int
	FilterSize    uint
	IndexPath     string
	IndexOffest   int
	IndexSize     uint
	SummaryPath   string
	SummaryOffset int
	SummarySize   uint
}

type SSTable struct {
	FilePathBase       string
	DataSegmentPath    string
	SummarySegmentPath string
	FilterPath         string
	TOCFilePath        string
	Index              *Index
}

func NewSSTable(conf *config.Config) *SSTable {
	// Creating file paths for new SSTable
	table := &SSTable{FilePathBase: "./data/SSTables/usertable-"}
	gen := strconv.Itoa(GetNextGeneration())
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

func (sstable *SSTable) WriteNewSSTable(data []memtable.Record, isOneFile bool) {
	// Function used for making SSTable on Disk
	// Getting data from memtable (traversing a BTree or SkipList)
	// Saving data in data segment, creating index and serializing it
	// Creating bloomfilter and summary structures and saving them
	// Parameters:
	//	- data : data from memtable
	//	- isOneFile : if we should save all structures in one file or separate
	file_offset := uint(0)

	keys := make([]string, len(data))
	offsets := make([]uint, len(data))

	data_file, err := os.Create(sstable.DataSegmentPath)
	if err != nil {
		panic(err)
	}

	defer data_file.Close()
	writer := bufio.NewWriter(data_file)

	for i, v := range data {
		key := []byte(v.Key)
		keySize := uint64(len(key))
		value := v.Data
		valueSize := uint64(len(value))
		tombstone := v.Tombstone

		timestamp := time.Now()
		timestampBytes := make([]byte, 16)
		binary.BigEndian.PutUint64(timestampBytes[:8], uint64(timestamp.Unix()))
		binary.BigEndian.PutUint64(timestampBytes[8:], uint64(timestamp.Nanosecond()))

		WrittenBytes := 0

		keys[i] = v.Key
		offsets[i] = file_offset

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

		file_offset += uint(WrittenBytes)

		if WrittenBytes, err = writer.Write(value); err != nil {
			panic(err)
		}

		file_offset += uint(WrittenBytes) + 33 // Broj zapisanih i 2 puta po 8 bajta za zapis velicine segmenata + Tombstone
		if err = writer.Flush(); err != nil {
			panic(err)
		}
		writer.Reset(data_file)

	}

	// ****************************************************************
	// TO-DO:
	// Dodati funkcionalnost za kreiranje Bloomfiltera i kreiranje Index Summary
	// ****************************************************************
	sstable.Index.offset = 0
	if isOneFile {
		sstable.Index.offset = int64(file_offset)
	}
	sstable.Index.CreateIndexSegment(keys, offsets)
	sstable.CreateTOC()
	return
}

func (sstable *SSTable) Read(offset int64) {
	// Function used to read segment from DataSegment
	data_file, err := os.OpenFile(sstable.DataSegmentPath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	data_file.Seek(offset, 1)
	temp := ReadNextDataRecord(data_file)
	// Mozda je bolje vratiti value nazad funkciji koja poziva ovu funkciju
	fmt.Println(temp.Tombstone, "    ", temp.Timestamp.String(), "     ", temp.Key, "    ", string(temp.Value))
}

func (sstable *SSTable) CreateTOC() {
	// Creating Table of Contents and saving it in file
	toc := &TOC{}
	toc.DataPath = sstable.DataSegmentPath
	toc.IndexPath = sstable.Index.indexfile
	toc.IndexOffest = int(sstable.Index.offset)
	toc.IndexSize = uint(sstable.Index.size)
	toc.FilterPath = sstable.FilterPath
	toc.SummaryPath = sstable.SummarySegmentPath
	toc.Save(sstable.TOCFilePath)
}

func GetNextGeneration() int {
	// Utility function used for getting the next number for generation
	// Return:
	// 	- Last generation + 1
	file, err := os.Open("./data/SSTables")
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
		fmt.Println(filename)
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

func GetSSTable(generation int) *SSTable {
	// Loading a SSTable attributes such as TOC, FilePaths...
	// DataSegment/Index are never loaded fully after serializing them on Disk
	// Parameters:
	//	- generation : selecting a sstable to pick, if it is 0 we return the last made SSTable
	// Return:
	//	- Pointer to the loaded SSTable
	if generation <= 0 {
		generation = GetNextGeneration() - 1
	}
	if generation < 1 {
		fmt.Println("No data saved")
		return nil
	}
	gen := strconv.Itoa(generation)

	file, err := os.OpenFile("./data/SSTables/usertable-"+gen+"-TOC.yaml", os.O_RDONLY, 0666)
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
	table := &SSTable{FilePathBase: "./data/SSTables/usertable-"}
	toc, _ := tryLoad(file.Name())

	table.Index = NewIndex(toc.IndexPath, int64(toc.IndexOffest), uint64(toc.IndexSize))
	table.DataSegmentPath = toc.DataPath
	table.FilterPath = toc.FilterPath
	table.SummarySegmentPath = toc.SummaryPath
	table.TOCFilePath = file.Name()
	return table
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
	timestampBytes := make([]byte, 16)

	_, err := reader.Read(timestampBytes)
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

	return DataElement{Timestamp: timestamp, Tombstone: tombstone, KeySize: keySize, ValueSize: valueSize, Key: string(key), Value: value}
}

// func GetSSTableFromTOC(toc *TOC) *SSTable {

// }
