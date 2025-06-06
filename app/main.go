package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/xwb1989/sqlparser"
)

var dbFile *os.File
var schema Schema

func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]
	var e error
	dbFile, e = os.Open(databaseFilePath)
	if e != nil {
		log.Fatal(e)
	}

	schema = LoadSchema(dbFile)
	stm, _ := sqlparser.Parse(command)
	slct, ok := stm.(*sqlparser.Select)

	if ok {
		var rws []Row
		rsl := Run(slct, rws)

		for _, r := range rsl {
			for i, c := range r.Metadata {
				if i > 0 {
					fmt.Print("|")
				}
				switch strings.ToLower(c.Type) {
				case "integer": //create enum of database types
					var val int64
					binary.Read(bytes.NewReader(r.Colums[i].Value), binary.BigEndian, &val)
					fmt.Print(val)
				case "text":
					fmt.Print(string(r.Colums[i].Value))
				}
			}
			fmt.Print("\n")
		}
	} else {
		switch command {
		case ".dbinfo":
			fmt.Printf("database page size: %v", schema.PageSize)
			fmt.Printf("number of tables: %v", len(schema.Tables))
		case ".tables":
			for _, s := range schema.Tables {
				fmt.Println(string(s.Name))
			}
		default:
			fmt.Println("Unknown command", command)
			os.Exit(1)
		}
	}
}

type DBHeader struct {
	PageSize int32
}

type Page struct {
	DbHeader   DBHeader
	PageHeader PageHeader
	Pointers   []int16
	RawPage    []byte
}

type PageHeader struct {
	PageType         byte
	CellsCount       uint16
	RightMostPointer uint32
}

type TableLeafCell struct {
	CellSize int64
	RowId    int64
	Record   []Column
}

type TableInteriorCell struct {
	LeftChildPointer uint32
	Key              int64 //change to uint64 later...
}

type Column struct {
	SerialType int64
	Value      []byte
}

func FullScan(file *os.File, fileOffset int64, pgSize int32) []TableLeafCell {
	page := ReadPage(file, fileOffset, pgSize)
	var cells []TableLeafCell

	if page.PageHeader.PageType == 0x0d { //leaf page
		//read cells
		for i := 0; i < len(page.Pointers); i++ {
			cell := ReadTableLeafCell(page.RawPage, int64(page.Pointers[i])) //TODO check casting
			cells = append(cells, *cell)
		}

		return cells
	}

	for i := 0; i < len(page.Pointers); i++ {
		interiorCell := ReadTableInteriorCell(page.RawPage, int64(page.Pointers[i])) //TODO check casting
		cells = append(cells, FullScan(file, int64(interiorCell.LeftChildPointer-1)*int64(pgSize), pgSize)...)
	}

	if page.PageHeader.RightMostPointer != 0 {

		cells = append(cells, FullScan(file, int64(page.PageHeader.RightMostPointer-1)*int64(pgSize), pgSize)...)
	}

	return cells
}

func ReadPage(file *os.File, fileOffset int64, pgSize int32) Page {
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

	//binary.Read(bytes.NewReader(page[pageOffset+3:pageOffset+5]), binary.BigEndian, &header.CellsCount) clean if works
	header.CellsCount = binary.BigEndian.Uint16(page[pageOffset+3 : pageOffset+5])

	//read array pointer
	var headerSize uint32 = 12

	if header.PageType == 0x0a || header.PageType == 0x0d {
		headerSize = 8
	}

	//read right most pointer
	if header.PageType == 0x02 || header.PageType == 0x05 {
		header.RightMostPointer = binary.BigEndian.Uint32(page[pageOffset+8 : pageOffset+12])
	}

	pageOffset += uint32(headerSize)

	var pointers = make([]int16, header.CellsCount)

	for i := uint32(0); i < uint32(header.CellsCount); i++ {
		var p = pageOffset + i*2
		binary.Read(bytes.NewReader(page[p:p+2]), binary.BigEndian, &pointers[i])
	}

	return Page{
		DbHeader:   dbHeader,
		PageHeader: header,
		Pointers:   pointers,
		RawPage:    page}
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

	return DBHeader{PageSize: int32(size)}
}

func ReadTableInteriorCell(byteArr []byte, off int64) *TableInteriorCell {
	leftChildPointer := binary.BigEndian.Uint32(byteArr[off : off+4])
	key, _, _ := ReadVarint(byteArr[off+4:])

	return &TableInteriorCell{LeftChildPointer: leftChildPointer, Key: key}
}

func ReadTableLeafCell(byteArr []byte, off int64) *TableLeafCell { //for Table B-Tree Leaf Cell only. treat overflow pages later!

	//read cell size
	recordSize, c1, _ := ReadVarint(byteArr[off:])
	//read rowid
	rowId, c2, _ := ReadVarint(byteArr[off+c1:])

	//read record
	recordBytes := byteArr[off+c1+c2 : off+c1+c2+recordSize]
	record := ReadRecord(recordBytes, rowId)

	return &TableLeafCell{CellSize: c1 + c2 + recordSize, RowId: rowId, Record: record}
}

func ReadRecord(byteArr []byte, rowId int64) []Column {
	headerSize, bytesRead, _ := ReadVarint(byteArr)

	var serialTypes []int64

	for bytesRead < headerSize {
		t, r, _ := ReadVarint(byteArr[bytesRead:])
		bytesRead += r
		serialTypes = append(serialTypes, t)
	}

	var columns []Column

	for i, tp := range serialTypes {
		var size int64
		switch {
		case tp == 1:
			size = 1
		case tp == 2:
			size = 2
		case tp == 3:
			size = 3
		case tp == 4:
			size = 4
		case tp == 5:
			size = 6
		case tp == 6:
			size = 8
		case tp == 7:
			size = 8
		case tp > 12 && tp%2 == 0:
			size = (tp - 12) / 2 //string size
		case tp > 13:
			size = (tp - 13) / 2 //string size
		default:
			size = 0 // data is NULL
		}

		var value []byte

		if size != 0 {
			value = byteArr[bytesRead : bytesRead+size]
		}

		if i == 0 && size == 0 {
			columns = append(columns, Column{SerialType: 6, Value: binary.BigEndian.AppendUint64(nil, uint64(rowId))}) //rowid is always the first column
		} else {
			columns = append(columns, Column{SerialType: tp, Value: value})
		}

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
