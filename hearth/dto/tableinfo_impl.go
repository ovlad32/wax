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
func (t TableInfoType) MaxColumnPosition() (result int) {
	for _, c := range t.ColumnList() {
		if c.Position.Valid() && int(c.Position.Value()) > result {
			result = int(c.Position.Value())
		}
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
