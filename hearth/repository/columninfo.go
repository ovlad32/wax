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
         ,NON_NULL_COUNT
	     ,NUMERIC_COUNT
	     ,MIN_FVAL
		 ,MAX_FVAL
		 ,MIN_SVAL
		 ,MAX_SVAL
		 ,INTEGER_UNIQUE_COUNT
		 ,MOVING_MEAN
		 ,MOVING_STDDEV
		 ,HAS_FLOAT_CONTENT
		 ,POSITION_IN_PK
		 ,TOTAL_IN_PK
		 ,FUSION_PARENT_COLUMN_ID
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
			&row.,
			&row.MovingMean,
			&row.MovingStdDev,
			&row.HasFloatContent,
			&row.PositionInPK,
			&row.TotalInPK,
			&row.FusionParentColumnId,
			&row.FusionParentGroupId,
			)


			,HAS_NUMERIC_CONTENT
			,MIN_FVAL
			,MAX_FVAL
			,MIN_SVAL
			,MAX_SVAL
			,INTEGER_UNIQUE_COUNT
			,MOVING_MEAN
			,MOVING_STDDEV
			,HAS_FLOAT_CONTENT
			,POSITION_IN_PK
			,TOTAL_IN_PK
			,FUSION_PARENT_COLUMN_ID
			,FUSION_COLUMN_GROUP_ID

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

func PutColumnInfo (ctx context.Context, columnInfo *dto.ColumnInfoType) (err error){


 if columnInfo == nil {
 	err = fmt.Errorf("column is not initialized")
 	return
 }
 var newOne bool
 conn, err  := iDb.Conn(ctx)
 if err!= nil {
 	err = fmt.Errorf("could not obtain connection ")
 }
 if !columnInfo.Id.Valid() {
 }

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
func FusionColumnGroupByColumnId(ctx context.Context, column *dto.ColumnInfoType) (result *dto.FusionColumnGroupType, err error) {
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

