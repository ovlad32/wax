package dto

import (
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type AppNodeType struct {
	Id            nullable.NullInt64
	Hostname      string
	Address       string
	LastHeartbeat string
	State         string
	Role          string
}
