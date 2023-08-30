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
	// u 2 razlicita niza
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

	reader := bufio.NewReader(data_file)

	data_file.Seek(offset, 1)

	timestampBytes := make([]byte, 16)
	_, err = reader.Read(timestampBytes)
	if err != nil {
		panic(err)
	}

	var timeNanoseconds int64
	var timeSecond int64
	timeSecond = int64(binary.BigEndian.Uint64(timestampBytes[:8]))
	timeNanoseconds = int64(binary.BigEndian.Uint64(timestampBytes[8:]))
	timestamp := time.Unix(timeSecond, timeNanoseconds)
	tombstone, err := reader.ReadByte()

	if err != nil {
		panic(err)
	}

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
	// Mozda je bolje vratiti value nazad funkciji koja poziva ovu funkciju
	fmt.Println(tombstone, "    ", timestamp.String(), "     ", string(key), "    ", string(value))
}
func (sstable *SSTable) CreateTOCFile() {
	// OBRISI FUNCKIJU
	file, err := os.OpenFile(sstable.TOCFilePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	writer.WriteString(sstable.DataSegmentPath + "\n")
	writer.WriteString(sstable.Index.indexfile + "\n")
	writer.WriteString(sstable.FilterPath + "\n")
	writer.WriteString(sstable.SummarySegmentPath + "\n")
	writer.Flush()
	writer.Reset(file)
}
func (sstable *SSTable) CreateTOC() {
	// Funkcija radi kreiranje Table of Contents fajla koji se koristi za pozicioniranje
	// i citanje ostalih struktura
	toc := &TOC{}
	toc.DataPath = sstable.DataSegmentPath
	toc.IndexPath = sstable.Index.indexfile
	toc.IndexOffest = int(sstable.Index.offset)
	toc.IndexSize = uint(sstable.Index.size)
	toc.FilterPath = sstable.FilterPath
	toc.SummaryPath = sstable.SummarySegmentPath
	toc.Save(sstable.TOCFilePath)
}

func CreateDummyData() []*dummy {
	// BRISI
	d := make([]*dummy, 5)
	d[0] = &dummy{key: "Dimitrije", value: []byte("Gasic"), tombstone: 0}
	d[1] = &dummy{key: "Masha", value: []byte("Gasifgadsfsdgkjalkjgladshgasdc"), tombstone: 0}
	d[2] = &dummy{key: "Aleksa", value: []byte("Vukomanovic"), tombstone: 0}
	d[3] = &dummy{key: "Milica", value: []byte("Misic"), tombstone: 0}
	d[4] = &dummy{key: "Milan", value: []byte("Arezina"), tombstone: 0}

	return d
}

func GetNextGeneration() int {
	// Dobavlja koja je sledeca generacija sstabele (broj sstabele)
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
	data, _ := yaml.Marshal(toc)
	os.WriteFile(path, data, 0644)
}
func tryLoad(path string) (*TOC, bool) {
	c := TOC{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}

	err = yaml.Unmarshal(data, &c)
	return &c, err == nil
}
