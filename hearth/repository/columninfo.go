package repository

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"strings"
)

func columnInfo(ctx context.Context, where whereFunc, args []interface{}) (result []*dto.ColumnInfoType, err error) {

	tx, err := iDb.Conn(ctx)
	if err != nil {
		return
	}

	result = make([]*dto.ColumnInfoType, 0)

	query := `SELECT  
		 ID 
		 ,NAME 
		 ,DATA_TYPE 
		 ,REAL_TYPE 
		 ,CHAR_LENGTH 
		 ,DATA_PRECISION 
		 ,DATA_SCALE 
		 ,POSITION 
		 ,TOTAL_ROW_COUNT 
		 ,UNIQUE_ROW_COUNT 
		 ,HASH_UNIQUE_COUNT 
		 ,TABLE_INFO_ID
         ,EMPTY_COUNT
	     ,NUMERIC_COUNT
	     ,MIN_FVAL
		 ,MAX_FVAL
		 ,MIN_SVAL
		 ,MAX_SVAL
		 ,INTEGER_COUNT
		 ,INTEGER_UNIQUE_COUNT
		 ,MOVING_MEAN
		 ,MOVING_STDDEV
		 ,POSITION_IN_PK
		 ,TOTAL_IN_PK
		 ,SOURCE_FUSION_COLUMN_ID
         ,POSITION_IN_FUSION
         ,TOTAL_IN_FUSION
		 ,FUSION_COLUMN_GROUP_ID
		 FROM COLUMN_INFO `

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
				&row.NonNullCount,
				&row.NumericCount,
				&row.MinNumericValue,
				&row.MaxNumericValue,
				&row.MinStringValue,
				&row.MaxStringValue,
				&row.IntegerCount,
				&row.IntegerUniqueCount,
				&row.MovingMean,
				&row.MovingStdDev,
				&row.PositionInPK,
				&row.TotalInPK,
				&row.SourceFusionColumnId,
				&row.PositionInFusion,
				&row.TotalInFusion,
				&row.FusionColumnGroupId,
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
		switch currentDbType {
		case H2:
			where = func() string {
				return fmt.Sprintf(whereString, tableInfo.Id)
			}
		default:
			where = func() string {
				return fmt.Sprintf(whereString, "?")
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
		args = append(args, Id)
	}

	res, err := columnInfo(ctx, where, args)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}

func ColumnInfoSeqId() (id int64, err error) {
	err = iDb.QueryRow("select nextval('COLUMN_INFO_SEQ')").Scan(&id)
	if err != nil {
		err = fmt.Errorf("could not get a sequential number from COLUMN_INFO_SEQ: %v", err)
	}
	return
}

func PutColumnInfo(ctx context.Context, columnInfo *dto.ColumnInfoType) (err error) {
	var newOne bool

	if newOne {
		columnInfo.Id = nullable.NullInt64{}
	}
	if columnInfo == nil {
		err = fmt.Errorf("column is not initialized")
		return
	}

	if !columnInfo.Id.Valid() {
		var id int64
		id, err = ColumnInfoSeqId()
		if err != nil {
			return
		}
		columnInfo.Id = nullable.NewNullInt64(id)
		newOne = true
	}

	var dml string
	dml = `
             merge into column_info (
		 ID 
		 ,NAME 
		 ,DATA_TYPE 
		 ,REAL_TYPE 
		 ,CHAR_LENGTH 
		 ,DATA_PRECISION 
		 ,DATA_SCALE 
		 ,POSITION 
         ,IS_NULLABLE
		 ,TOTAL_ROW_COUNT 
		 ,UNIQUE_ROW_COUNT 
		 ,HASH_UNIQUE_COUNT 
		 ,TABLE_INFO_ID
         ,EMPTY_COUNT
	     ,NUMERIC_COUNT
	     ,MIN_FVAL
		 ,MAX_FVAL
		 ,MIN_SVAL
		 ,MAX_SVAL
		 ,INTEGER_COUNT
		 ,INTEGER_UNIQUE_COUNT
		 ,MOVING_MEAN
		 ,MOVING_STDDEV
		 ,POSITION_IN_PK
		 ,TOTAL_IN_PK
		 ,SOURCE_FUSION_COLUMN_ID
         ,POSITION_IN_FUSION
         ,TOTAL_IN_FUSION
		 ,FUSION_COLUMN_GROUP_ID
       ) key(ID) values (
		%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v,
		%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v,
		%v, %v, %v, %v, %v, %v, %v, %v, %v, %v
		)`
	switch currentDbType {
	case H2:

		dml = fmt.Sprintf(dml,
			columnInfo.Id,
			columnInfo.ColumnName.SQLString(),
			columnInfo.DataType.SQLString(),
			columnInfo.RealDataType.SQLString(),
			columnInfo.DataLength,
			columnInfo.DataPrecision,
			columnInfo.DataScale,
			columnInfo.Position,
			columnInfo.Nullable,
			columnInfo.TotalRowCount,
			columnInfo.UniqueRowCount,
			columnInfo.HashUniqueCount,
			columnInfo.TableInfoId,
			columnInfo.NonNullCount,
			columnInfo.NumericCount,
			columnInfo.MinNumericValue,
			columnInfo.MaxNumericValue,
			columnInfo.MinStringValue.SQLString(),
			columnInfo.MaxStringValue.SQLString(),
			columnInfo.IntegerCount,
			columnInfo.IntegerUniqueCount,
			columnInfo.MovingMean,
			columnInfo.MovingStdDev,
			columnInfo.PositionInPK,
			columnInfo.TotalInPK,
			columnInfo.SourceFusionColumnId,
			columnInfo.PositionInFusion,
			columnInfo.TotalInFusion,
			columnInfo.FusionColumnGroupId,
		)

		_, err = iDb.Exec(dml)
	default:
		dml = strings.Replace(dml, "%v", "?", -1)

		_, err = iDb.Exec(dml,
			columnInfo.Id,
			columnInfo.ColumnName,
			columnInfo.DataType,
			columnInfo.RealDataType,
			columnInfo.DataLength,
			columnInfo.DataPrecision,
			columnInfo.DataScale,
			columnInfo.Position,
			columnInfo.Nullable,
			columnInfo.TotalRowCount,
			columnInfo.UniqueRowCount,
			columnInfo.HashUniqueCount,
			columnInfo.TableInfoId,
			columnInfo.NonNullCount,
			columnInfo.IntegerCount,
			columnInfo.IntegerUniqueCount,
			columnInfo.MovingMean,
			columnInfo.MovingStdDev,
			columnInfo.PositionInPK,
			columnInfo.TotalInPK,
			columnInfo.SourceFusionColumnId,
			columnInfo.PositionInFusion,
			columnInfo.TotalInFusion,
			columnInfo.FusionColumnGroupId,
		)
	}
	if err != nil {
		if newOne {
			err = fmt.Errorf("could not add a new column_info row: %v", err)
		} else {
			err = fmt.Errorf("could not update column_info row with id=%v: %v", columnInfo.Id.Value(), err)
		}
		return
	}

	return
}

func fusionColumnGroup(ctx context.Context, where whereFunc, args []interface{}) (result []*dto.FusionColumnGroupType, err error) {

	tx, err := iDb.Conn(ctx)
	if err != nil {
		return
	}

	result = make([]*dto.FusionColumnGroupType, 0)

	query := "SELECT " +
		" ID" +
		" ,COLUMN_INFO_ID" +
		" ,GROUP_TUPLES" +
		" FROM FUSION_COLUMN_GROUP"

	if where != nil {
		query = query + where()
	}

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
			var row dto.FusionColumnGroupType
			err = rws.Scan(
				&row.Id,
				&row.ColumnInfoId,
				&row.GroupTuples,
			)
			if err != nil {
				return
			}
			result = append(result, &row)
		}
	}
	return
}

func FusionColumnGroupByColumn(ctx context.Context, column *dto.ColumnInfoType) (result *dto.FusionColumnGroupType, err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("could not read Fusion Column Group :%v", err)
			result = nil
		}
	}()

	if column == nil {
		err = fmt.Errorf("column is null")
		return
	}
	if !column.Id.Valid() {
		err = fmt.Errorf("column id is not initialized")
		return
	}
	return fusionColumnGroupByColumnId(ctx, column.Id.Value())

}
func FusionColumnGroupByColumnId(ctx context.Context, Id int64) (result *dto.FusionColumnGroupType, err error) {
	result, err = fusionColumnGroupByColumnId(ctx, Id)
	if err != nil {
		err = fmt.Errorf("could not read Fusion Column Group :%v", err)
		result = nil
	}
	return
}

func fusionColumnGroupByColumnId(ctx context.Context, Id int64) (result *dto.FusionColumnGroupType, err error) {
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
		args = append(args, Id)
	}

	res, err := fusionColumnGroup(ctx, where, args)

	if err == nil && len(res) > 0 {
		result = res[0]
	}
	return
}
