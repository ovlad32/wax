package dto

import (
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type CategorySplitType struct {
	Id nullable.NullInt64
	TableInfoId nullable.NullInt64
	Built bool
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

type CategorySplitDataType struct {
	Id nullable.NullInt64
	CategorySplitId nullable.NullInt64
	CategorySplit *CategorySplitType
	Data string
}
type CategorySplitDataListType []*CategorySplitDataType


