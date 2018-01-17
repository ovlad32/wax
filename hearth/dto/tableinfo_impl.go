package dto

import (
	"fmt"
	"github.com/ovlad32/wax/hearth/misc"
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

func (t *TableInfoType) TableInfoReference() *TableInfoType {
	return t
}



