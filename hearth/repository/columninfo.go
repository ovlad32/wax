package repository

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
)

func columnInfo(ctx context.Context, where whereFunc, args []interface{}) (result []*dto.ColumnInfoType, err error) {

	tx, err := iDb.Conn(ctx)
	if err != nil {
		return
	}

	result = make([]*dto.ColumnInfoType, 0)

	query := "SELECT " +
		" ID" +
		" ,NAME" +
		" ,DATA_TYPE" +
		" ,REAL_TYPE" +
		" ,CHAR_LENGTH" +
		" ,DATA_PRECISION" +
		" ,DATA_SCALE" +
		" ,POSITION" +
		" ,TOTAL_ROW_COUNT" +
		" ,UNIQUE_ROW_COUNT" +
		" ,HASH_UNIQUE_COUNT" +
		" ,TABLE_INFO_ID" +
		" FROM COLUMN_INFO "

	if where != nil {
		query = query + where()
	}

	query = query + " ORDER BY POSITION"
	rws, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return
	}
	defer rws.Close()

	for rws.Next() {
		select {
		case <-ctx.Done():
			return
		default:
			var row dto.ColumnInfoType
			err = rws.Scan(
				&row.Id,
				&row.ColumnName,
				&row.DataType,
				&row.RealDataType,
				&row.CharLength,
				&row.DataPrecision,
				&row.DataScale,
				&row.Position,
				&row.TotalRowCount,
				&row.UniqueRowCount,
				&row.HashUniqueCount,
				&row.TableInfoId,
			)
			if err != nil {
				return
			}
			result = append(result, &row)
		}
	}
	return
}

func ColumnInfoByTable(ctx context.Context, tableInfo *dto.TableInfoType) (result []*dto.ColumnInfoType, err error) {
	where := MakeWhereFunc()
	args := MakeWhereArgs()
	whereString := " WHERE TABLE_INFO_ID = %v"

	if tableInfo != nil && tableInfo.Id.Valid() {
		switch  currentDbType {
		case H2:
			where = func() string {
				return fmt.Sprintf(whereString , tableInfo.Id)
			}
		default:
			where = func() string {
				return fmt.Sprintf(whereString , "?")
			}

			args = append(args, tableInfo.Id.Value())
		}
	}


	result, err = columnInfo(ctx, where, args)
	if err == nil {
		for index := range result {
			result[index].TableInfo = tableInfo
		}
	}
	return
}

func ColumnInfoById(ctx context.Context, Id int) (result *dto.ColumnInfoType, err error) {
	where := MakeWhereFunc()
	args := MakeWhereArgs()
	whereString := " WHERE ID = %v"
	switch currentDbType {
	case H2:
		where = func() string {
			return fmt.Sprintf(whereString, Id)
		}
	default:
		where = func() string {
			return fmt.Sprintf(whereString, "")
		}
		args = append(args,Id)
	}

	res, err := columnInfo(ctx, where, args)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}
