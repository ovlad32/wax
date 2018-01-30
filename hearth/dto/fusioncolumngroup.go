package dto

import (
	"strings"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"strconv"
)

type FusionColumnGroupType struct {
	Id          nullable.NullInt64
	TableInfoId nullable.NullInt64
	GroupKey    string
	RowCount    nullable.NullInt64
}

type FusionColumnGroupListType []*FusionColumnGroupType;



type FusionColumnType struct {
	SourceColumnPosition int
	ColumnCount int
}

type FusionColumnListType []*FusionColumnType


func (f FusionColumnListType) String() (result string)  {
	if f == nil || len(f) == 0 {
		return
	}

	temp := make([]string,0,len(f))
	for _,v := range f  {
		temp = append(temp,
			strconv.Itoa(v.SourceColumnPosition)+":"+strconv.Itoa(v.ColumnCount),
		)
	}
	return strings.Join(temp,"|")
}

/*
func (g FusionColumnGroupType) ToTuples(jsonData string) (result FusionColumnGroupDetailsType,err error){

	dec := json.NewDecoder(strings.NewReader(jsonData))
	err = dec.Decode(result)
	if err != nil {
		err = fmt.Errorf("could not parse json for '%v': %v",jsonData, err)
		return
	}
	return
}*/