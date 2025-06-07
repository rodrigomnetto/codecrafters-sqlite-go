package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"strings"

	"github.com/xwb1989/sqlparser"
)

func LoadSchema(f *os.File) Schema {
	//load schema table in memory
	schemaPage := ReadPage(f, 0, 0) //fix this later to avoid double reading
	cells := FullScan(f, 0, 0)
	var tables []Table

	for _, cell := range cells {

		if string(cell.Record[0].Value) == "table" && string(cell.Record[1].Value) != "sqlite_sequence" { //check if type is table
			var rootPage int8 //why it doesnt work with int64 type?
			binary.Read(bytes.NewReader(cell.Record[3].Value), binary.BigEndian, &rootPage)

			sql := string(cell.Record[4].Value)
			sql = strings.Replace(sql, "autoincrement", "", 1) //necessary because sqlparser only parses mysql commands...
			stm, _ := sqlparser.ParseStrictDDL(sql)

			ddl := stm.(*sqlparser.DDL)

			var clms []ColumnMetadata
			for _, c := range ddl.TableSpec.Columns {
				clms = append(clms, ColumnMetadata{Name: c.Name.CompliantName(), Type: c.Type.Type})
			}

			tbl := Table{
				Name:       string(cell.Record[1].Value),
				RootPage:   rootPage,
				PageOffset: int64(rootPage-1) * int64(schemaPage.DbHeader.PageSize),
				Sql:        sql,
				Columns:    clms,
			}

			tables = append(tables, tbl)
		}
	}

	return Schema{PageSize: schemaPage.DbHeader.PageSize, Tables: tables}
}

func GetTableInfo(schema Schema, name string) Table {
	for _, s := range schema.Tables {
		if s.Name == name {
			return s
		}
	}

	return Table{}
}

type Schema struct {
	PageSize int32
	Tables   []Table
}

type Table struct {
	Name       string
	RootPage   int8
	PageOffset int64
	Sql        string
	Columns    []ColumnMetadata
}

type ColumnMetadata struct {
	Name string
	Type string
}
