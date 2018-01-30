package dto

import (
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type TableInfoType struct {
	Id            nullable.NullInt64  `json:"table-id"`
	MetadataId    nullable.NullInt64  `json:"metadata-id"`
	DatabaseName  nullable.NullString `json:"database-name"`
	SchemaName    nullable.NullString `json:"schema-name"`
	TableName     nullable.NullString `json:"table-name"`
	RowCount      nullable.NullInt64  `json:"row-count"`
	Dumped        nullable.NullString `json:"data-dumped"`
	Indexed       nullable.NullString `json:"data-indexed"`
	PathToFile    nullable.NullString `json:"path-to-file"`
	PathToDataDir nullable.NullString `json:"path-to-data-dir"`
	//Chunk         nullable.NullString `json:"chunk"`
	SourceSliceTableInfoId nullable.NullInt64
	CategorySplitDataId nullable.NullInt64
	Metadata      *MetadataType
	Columns       []*ColumnInfoType `json:"columns"`
}

type TableInfoArrayType []*TableInfoType
