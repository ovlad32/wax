package repository

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
)

func columnInfo(ctx context.Context, whereFunc func() string) (result []*dto.ColumnInfoType, err error) {

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

	if whereFunc != nil {
		query = query + whereFunc()
	}

	query = query + " ORDER BY POSITION"
	rws, err := tx.QueryContext(ctx, query)
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
	whereFunc := func() string {
		if tableInfo != nil && tableInfo.Id.Valid() {
			return fmt.Sprintf(" WHERE TABLE_INFO_ID = %v", tableInfo.Id)
		}
		return ""
	}
	result, err = columnInfo(ctx, whereFunc)
	if err == nil {
		for index := range result {
			result[index].TableInfo = tableInfo
		}
	}
	return
}

func ColumnInfoById(ctx context.Context, Id int) (result *dto.ColumnInfoType, err error) {
	whereFunc := func() string {
		return fmt.Sprintf(" WHERE ID = %v", Id)
	}
	res, err := columnInfo(ctx, whereFunc)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}
