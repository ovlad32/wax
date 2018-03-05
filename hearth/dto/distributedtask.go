package dto

import "github.com/ovlad32/wax/hearth/handling/nullable"

type DistTaskType struct {
	Id     nullable.NullInt64
	Task   string
	Status string
}


type DistTaskNodeType struct {
	Id          nullable.NullInt64
	DistTaskId  nullable.NullInt64
	NodeId      string
	Role        string
	WorkerId    string
	DataSubject string
}

