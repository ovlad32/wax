package dto

import (
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"github.com/cayleygraph/cayley/graph/iterator"
)

type CategorySplitType struct {
	Id nullable.NullInt64
	TableInfoId nullable.NullInt64
	Status string
	Table *TableInfoType
	CategorySplitColumns CategorySplitColumnListType
}
type CategorySplitListType []*CategorySplitType

type CategorySplitColumnType struct {
	Id nullable.NullInt64
	CategorySplitId nullable.NullInt64
	ColumnInfoId nullable.NullInt64
	Position int
	Column *ColumnInfoType
	CategorySplit *CategorySplitType
}
type CategorySplitColumnListType []*CategorySplitColumnType

type CategorySplitColumnDataType struct {
	 Id nullable.NullInt64
	 CategorySplitColumnId nullable.NullInt64
	 CategorySplitColumn *CategorySplitColumnType
	 Data string
}
type CategorySplitColumnDataListType []*CategorySplitColumnDataType

type CategorySplitRowDataType struct {
	Id nullable.NullInt64
	CategorySplitId nullable.NullInt64
	CategorySplit *CategorySplitType
	Data string
}
type CategorySplitRowDataListType []*CategorySplitRowDataType

type CategorySplitFileType struct {
	Id nullable.NullInt64
	CategorySplitRowDataId nullable.NullInt64
	CategorySplitRowData *CategorySplitRowDataType
	PathToFile string
	Temp bool
	Indexed bool
	Zipped bool
	RowCount nullable.NullInt64
}

type CategorySplitFileListType []*CategorySplitFileType

