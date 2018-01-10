package dto

import (
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type ColumnInfoType struct {
	Id              nullable.NullInt64  `json:"column-id"`
	TableInfoId     nullable.NullInt64  `json:"table-id"`
	ColumnName      nullable.NullString `json:"column-name"`
	Position        nullable.NullInt64  `json:"column-position"`
	DataType        nullable.NullString `json:"data-type"`
	DataPrecision   nullable.NullInt64  `json:"numeric-precision"`
	DataScale       nullable.NullInt64  `json:"numeric-scale"`
	DataLength      nullable.NullInt64  `json:"byte-length"`
	CharLength      nullable.NullInt64  `json:"character-length"`
	Nullable        nullable.NullString `json:"astra.nullable"`
	RealDataType    nullable.NullString `json:"java-data-type"`
	MinStringValue  nullable.NullString `json:"min-string-value"`
	MaxStringValue  nullable.NullString `json:"max-string-value"`
	CategoryCount   nullable.NullInt64
	HashUniqueCount nullable.NullInt64
	UniqueRowCount  nullable.NullInt64
	TotalRowCount   nullable.NullInt64
	MinStringLength nullable.NullInt64
	MaxStringLength nullable.NullInt64
	NumericCount    nullable.NullInt64
	MinNumericValue nullable.NullFloat64
	MaxNumericValue nullable.NullFloat64
	NonNullCount    nullable.NullInt64
	DistinctCount   nullable.NullInt64
	TableInfo       *TableInfoType
	ContentFeatures ContentFeatureMapType
}

type ColumnInfoListType []*ColumnInfoType

type ColumnListInterface interface {
	ColumnList() ColumnInfoListType
}
