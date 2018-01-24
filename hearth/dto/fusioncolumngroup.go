package dto

import (
	"encoding/json"
	"bytes"
	"fmt"
	"strings"
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type FusionColumnGroupType struct {
	Id nullable.NullInt64
	ColumnInfoId nullable.NullInt64
	GroupTuples string
}



type FusionColumnGroupDetailTupleType struct {
	Pos int `json:"p"`
	Count int `json:"c"`
}

type FusionColumnGroupDetailsType struct {
	group []*FusionColumnGroupDetailTupleType `json:"groups"`
}

func (f *FusionColumnGroupDetailsType) ToJSON() (result string,err error)  {

	enc := json.NewEncoder(bytes.NewBufferString(result))
	err = enc.Encode(f)
	if err != nil {
		err = fmt.Errorf("could not encode FusionColumnGroupTuples: %v",err)
		return
	}

	return
}
func (g FusionColumnGroupType) ToTuples(jsonData string) (result FusionColumnGroupDetailsType,err error){

	dec := json.NewDecoder(strings.NewReader(jsonData))
	err = dec.Decode(result)
	if err != nil {
		err = fmt.Errorf("could not parse json for '%v': %v",jsonData, err)
		return
	}
	return
}