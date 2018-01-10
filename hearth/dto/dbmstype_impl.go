package dto

import "github.com/jinzhu/gorm"

func (DbmsTypeType) TableName() string {
	return "dbms_type"
}

func (DbmsTypeType) InitData(db gorm.DB) (err error) {
	//	db.Exec("upsert dbms_type(code,name)")
	return
}
