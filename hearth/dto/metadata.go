package dto

import (
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type MetadataType struct {
	Id               nullable.NullInt64
	Index            nullable.NullString
	Indexed          nullable.NullString
	Version          nullable.NullInt64
	DatabaseConfigId nullable.NullInt64
	IndexedKeys      nullable.NullString
}
