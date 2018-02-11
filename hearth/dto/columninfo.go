package dto

import (
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type ColumnInfoType struct {
	Id              nullable.NullInt64  `json:"column-id"`
	ColumnName      nullable.NullString `json:"column-name"`
	Position        nullable.NullInt64  `json:"column-position"`
	DataType        nullable.NullString `json:"data-type"`
	DataPrecision   nullable.NullInt64  `json:"numeric-precision"`
	DataScale       nullable.NullInt64  `json:"numeric-scale"`
	DataLength      nullable.NullInt64  `json:"byte-length"`
	CharLength      nullable.NullInt64  `json:"character-length"`
	Nullable        nullable.NullString `json:"astra.nullable"`
	RealDataType    nullable.NullString `json:"java-data-type"`
	HashUniqueCount nullable.NullInt64
	UniqueRowCount  nullable.NullInt64
	TotalRowCount   nullable.NullInt64
	//---------------
	NumericCount    nullable.NullInt64
	MinNumericValue nullable.NullFloat64
	MaxNumericValue nullable.NullFloat64
	MinStringValue  nullable.NullString `json:"min-string-value"`
	MaxStringValue  nullable.NullString `json:"max-string-value"`
	NonNullCount    nullable.NullInt64
	//DistinctCount        nullable.NullInt64
	IntegerCount       nullable.NullInt64
	IntegerUniqueCount nullable.NullInt64
	MovingMean         nullable.NullFloat64
	MovingStdDev       nullable.NullFloat64
	//---------------
	PositionInPK nullable.NullInt64
	TotalInPK    nullable.NullInt64
	//---------------
	FusionSeparator          nullable.NullString
	SourceFusionColumnInfoId nullable.NullInt64
	PositionInFusion         nullable.NullInt64
	TotalInFusion            nullable.NullInt64
	FusionColumnGroupId      nullable.NullInt64
	//---------------
	TableInfoId nullable.NullInt64 `json:"table-id"`
	TableInfo   *TableInfoType
	//---------------
	ContentFeatures     ContentFeatureMapType
	ContentFeatureCount nullable.NullInt64
	//
	SourceSliceColumnInfoId nullable.NullInt64
}

type ColumnInfoListType []*ColumnInfoType

type ColumnListInterface interface {
	ColumnList() ColumnInfoListType
	TableInfoReference() *TableInfoType
}
