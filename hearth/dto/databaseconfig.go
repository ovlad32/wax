package dto

import "github.com/ovlad32/wax/hearth/handling/nullable"

type DatabaseConfigType struct {
	Id            nullable.NullInt64  `json:"database-config-id"`
	ServerHost    nullable.NullString `json:"server-host"`
	ServerPort    nullable.NullInt64  `json:"server-port"`
	DatabaseAlias nullable.NullString `json:"database-alias"`
	DatabaseName  nullable.NullString `json:"database-name"`
	DatabaseGroup nullable.NullString `json:"database-group"`
	ServerType    nullable.NullString `json:"server-type"`
	UserName      nullable.NullString `json:"user-name"`
	Password      nullable.NullString `json:"user-password"`
	TargetSchema  nullable.NullString `json:"target-schema-name"`
}
