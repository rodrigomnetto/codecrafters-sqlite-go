package main

import (
	"bytes"
	"encoding/binary"

	"github.com/xwb1989/sqlparser"
)

type Row struct {
	Metadata []ColumnMetadata
	Colums   []Column
}

func Run(node sqlparser.SQLNode, rws []Row) []Row {

	var result []Row
	slct, ok := node.(*sqlparser.Select)

	if ok {
		result = Run(slct.From, rws)

		if slct.Where != nil {
			result = Run(slct.Where, result)
		}

		result = Run(slct.SelectExprs, result)

		return result
	}

	//from
	tblExprs, ok := node.(sqlparser.TableExprs)

	if ok {
		t := tblExprs[0].(*sqlparser.AliasedTableExpr) //why i cant index * type?

		inf := GetTableInfo(schema, t.Expr.(sqlparser.TableName).Name.String())
		cells := FullScan(dbFile, inf.PageOffset, schema.PageSize)

		for _, v := range cells {
			result = append(result, Row{Metadata: inf.Columns, Colums: v.Record})
		}

		return result
	}

	//where
	whr, ok := node.(*sqlparser.Where)

	if ok {
		cmp, ok := whr.Expr.(*sqlparser.ComparisonExpr)

		if ok {

			lft, ok := cmp.Left.(*sqlparser.ColName)
			rht, ok2 := cmp.Right.(*sqlparser.SQLVal)

			if ok && ok2 {
				for _, r := range rws {
					for i, m := range r.Metadata {
						if m.Name == lft.Name.String() {
							if bytes.Equal(r.Colums[i].Value, rht.Val) {
								result = append(result, r)
							}
						}
					}
				}
			}
		}

		return result
	}

	//projection
	slcExprs, ok := node.(sqlparser.SelectExprs)

	if ok {
		for _, r := range rws {
			var metadata []ColumnMetadata
			var columns []Column
			m := make(map[string]struct {
				ColumnMetadata
				Column
			})

			for i, c := range r.Metadata {
				m[c.Name] = struct {
					ColumnMetadata
					Column
				}{c, r.Colums[i]}
			}

			for _, v := range slcExprs {

				alExp, ok := v.(*sqlparser.AliasedExpr)

				if ok {
					colExpr, ok := alExp.Expr.(*sqlparser.ColName)

					if ok {
						metadata = append(metadata, m[colExpr.Name.String()].ColumnMetadata)
						columns = append(columns, m[colExpr.Name.String()].Column)
					}

					fnExpr, ok := alExp.Expr.(*sqlparser.FuncExpr)

					if ok {
						if fnExpr.Name.Lowered() == "count" && len(slcExprs) == 1 { //doesnt support multiple aggregations
							cnt := len(rws)
							metadata = append(metadata, ColumnMetadata{Name: "counter", Type: "integer"}) //counter is a temporary name for this column
							cntBytes := make([]byte, 8)
							binary.BigEndian.PutUint64(cntBytes, uint64(cnt))
							columns = append(columns, Column{Value: cntBytes})
							return append(result, Row{Metadata: metadata, Colums: columns})
						}
					}
				}
			}

			result = append(result, Row{Metadata: metadata, Colums: columns})
		}
	}

	return result
}
