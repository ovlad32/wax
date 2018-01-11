package dto

import (
	"fmt"
)

func (t TableInfoType) String() (result string) {
	result = t.TableName.Value()
	if t.SchemaName.Value() != "" {
		result = fmt.Sprintf("%v.%v", t.SchemaName.Value(), result)
	}
	return
}

func (t TableInfoType) GoString() (result string) {
	return fmt.Sprintf("TableInfo[id:%v,name:%v]", t.Id.Value(), t)
}

func (t TableInfoType) ColumnList() ColumnInfoListType {
	return t.Columns
}

func (t *TableInfoType) Reference() *TableInfoType {
	return t
}

type ColumnPositionBitType bool
const (
	ColumnPositionOn = true
	ColumnPositionOff ColumnPositionBitType = false
)

func (t *TableInfoType) ColumnPositionFlags(columnList []*ColumnInfoType,flag ColumnPositionBitType) (result []bool, err error) {
	result = make([]bool,len(t.Columns))
	for index := range result {
		result[index] = bool(!flag)
	}
nextColumn:
	for _, target := range columnList {
		if target == nil {
			continue
		}
		for position, column := range t.Columns {
			if column.Id.Value() == target.Id.Value() {
				result[position] = bool(flag)
				continue nextColumn
			}
		}
		err = fmt.Errorf("column %v has not been found in table %v", target, t)
		return nil, err
	}
	return
}



