package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := make([]byte, 108)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}
		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Fprintln(os.Stderr, "Logs from your program will appear here!")

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v", pageSize)

		var cellCount uint16
		if err := binary.Read(bytes.NewReader(header[103:105]), binary.BigEndian, &cellCount); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		fmt.Printf("number of tables: %v", cellCount)
	case ".tables":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		page := ReadPage(databaseFile, 0, 0)

		for _, cell := range page.Cells {
			fmt.Println(string(cell.Record[2].Value))
		}

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

type DBHeader struct {
	PageSize uint32
}

type Page struct {
	DbHeader DBHeader
	Header   PageHeader
	Cells    []Cell
}

type PageHeader struct {
	PageType   byte
	CellsCount uint16
}

type Cell struct {
	CellSize int64
	RowId    int64
	Record   []Column
}

type Column struct {
	SerialType int64
	Value      []byte
}

func ReadPage(file *os.File, fileOffset int64, pgSize uint32) Page {

	var pageOffset uint32 = 0
	var dbHeader DBHeader

	if fileOffset == 0 { //first page
		dbHeader = ReadDBHeader(file)
		pgSize = dbHeader.PageSize
		pageOffset += 100
	}

	file.Seek(fileOffset, 0)
	var page = make([]byte, pgSize)
	file.Read(page)

	//read header
	var header = PageHeader{
		PageType: page[pageOffset],
	}

	binary.Read(bytes.NewReader(page[pageOffset+3:pageOffset+5]), binary.BigEndian, &header.CellsCount)

	//read array pointer
	var headerSize uint32 = 12

	if header.PageType == 0x0a || header.PageType == 0x0d {
		headerSize = 8
	}

	pageOffset += uint32(headerSize)

	var pointers = make([]int16, header.CellsCount)

	for i := uint32(0); i < uint32(header.CellsCount); i++ {
		var p = pageOffset + i*2
		binary.Read(bytes.NewReader(page[p:p+2]), binary.BigEndian, &pointers[i])
	}

	var cells []Cell

	//read cells
	for i := 0; i < len(pointers); i++ {
		cell := ReadCell(page, int64(pointers[i])) //TODO check casting
		cells = append(cells, *cell)
	}

	return Page{
		DbHeader: dbHeader,
		Header:   header,
		Cells:    cells}
}

func ReadDBHeader(file *os.File) DBHeader {
	file.Seek(0, 0)
	var header = make([]byte, 100)
	file.Read(header)

	var size uint16
	binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &size)

	if size == 1 {
		return DBHeader{PageSize: 65536}
	}

	return DBHeader{PageSize: uint32(size)}
}

func ReadCell(byteArr []byte, off int64) *Cell {

	//read cell size
	recordSize, c1, _ := ReadVarint(byteArr[off:])
	//read rowid
	rowId, c2, _ := ReadVarint(byteArr[off+c1:])

	//read record
	recordBytes := byteArr[off+c1+c2 : off+c1+c2+recordSize]
	record := ReadRecord(recordBytes)

	return &Cell{CellSize: c1 + c2 + recordSize, RowId: rowId, Record: record}
}

func ReadRecord(byteArr []byte) []Column {
	headerSize, bytesRead, _ := ReadVarint(byteArr)

	var serialTypes []int64

	for bytesRead < headerSize {
		t, r, _ := ReadVarint(byteArr[bytesRead:])
		bytesRead += r
		serialTypes = append(serialTypes, t)
	}

	var columns []Column

	for _, tp := range serialTypes {
		var size int64
		switch {
		case tp > 12 && tp%2 == 0:
			size = (tp - 12) / 2 //string size
		case tp > 13:
			size = (tp - 13) / 2 //string size
		default:
			size = 1 // 1 byte size
		} //TODO add other type later

		columns = append(columns, Column{SerialType: tp, Value: byteArr[bytesRead : bytesRead+size]})
		bytesRead += size
	}

	return columns
}

// receive an array of 9 bytes and returns the integer 64 bits and the total size of bytes read
func ReadVarint(bytes []byte) (int64, int64, error) {
	var value int64
	var msbMask byte = 1 << 7

	for i := int64(0); i < 9; i++ {
		if i < 8 {

			value = value << 7
			value = value ^ int64(bytes[i]&127)

			if bytes[i]&msbMask == 0 {
				return value, i + 1, nil
			}
		} else {
			value = value << 8
			value = value ^ int64(bytes[i])
			return value, i + 1, nil
		}
	}

	return value, 0, nil
}
