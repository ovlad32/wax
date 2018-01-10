package repository

import (
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth/dto"
)

func CreateContentFeatureTable(ctx context.Context) (err error){
	conn, err := iDb.Conn(ctx)
	if err != nil {
		err = fmt.Errorf("could not obtain database connection ::v ",err)
		return
	}

	_, err = conn.ExecContext(
		ctx,
	   "create table if not exists column_datacategory_stats(" +
		   " column_info_id bigint not null " +
		   " key varchar(20) not null " +
		   ", byte_length " +
		   ", is_numeric bool " +
		   ", is_negative bool" +
		   ", is_integer bool " +
		   ", non_null_count bigint" +
		   ", hash_unique_count bigint" +
		   ", item_unique_count bigint" +
		   ", min_sval varchar(4000)" +
		   ", max_sval varchar(4000)" +
		   ", min_fval float" +
		   ", max_fval float" +
		   ", constraint column_datacategory_stats_pk primary key(column_info_id, key) " +
		   " ) ",
	)
	if err != nil {
		err = fmt.Errorf("cound not execute table creation of column_category_table :v",err)
	}
	return
}


func PutContentFeature(ctx context.Context, feature *dto.ContentFeatureType) (err error) {
	funcName := "repository.PutContentFeature"
	tx, err := iDb.Conn(ctx)
	if err != nil {
		tracelog.Errorf(err, packageName, funcName, "Begin transaction...")
		return
	}
	_, err = tx.ExecContext(ctx,
		fmt.Sprintf("merge into column_datacategory_stats("+
			" column_info_id "+
			", key "+
			", byte_length "+
			", is_numeric "+
			", is_negative "+
			", is_integer "+
			", non_null_count "+
			", hash_unique_count "+
			", item_unique_count "+
			", min_sval "+
			", max_sval "+
			", min_fval "+
			", max_fval "+
			", moving_mean "+
			", moving_stddev "+
			" ) key(column_info_id, key) "+
			" values(%v, '%v', %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v) ",
			feature.Column.Id,
			feature.Key,
			feature.ByteLength,
			feature.IsNumeric,
			feature.IsNegative,
			feature.IsInteger,
			feature.NonNullCount,
			feature.HashUniqueCount,
			feature.ItemUniqueCount,
			feature.MinStringValue.SQLString(),
			feature.MaxStringValue.SQLString(),
			feature.MinNumericValue,
			feature.MaxNumericValue,
			feature.MovingMean,
			feature.MovingStandardDeviation,
		),
	)
	if err != nil {
		err = fmt.Errorf("could not persist column_datacategory_stats object: %v",err)
		tracelog.Error(err, packageName, funcName)
		return
	}

	/*
		_, err = tx.Exec(fmt.Sprintf(
			"update column_info c set "+
				" (has_numeric_content, min_fval, max_fval, min_sval, max_sval) = ("+
				" select max(is_numeric) as has_numeric_content "+
				", min(min_fval)  as min_fval "+
				", max(max_fval)  as max_fval"+
				", min(min_sval)  as min_sval "+
				", max(max_sval)  as max_sval "+
				" from column_datacategory_stats s "+
				"  where s.column_id = c.id " +
				" ) where c.id = %v ", dataCategory.Column.Id.Value(),
		))

		if err != nil {
			tracelog.Error(err, packageName, funcName)
			return
		}

		_, err = tx.Exec(fmt.Sprintf("update column_info c set " +
			"   integer_unique_count = %v" +
			"   , moving_mean = %v" +
			"   , moving_stddev = %v" +
			"  where c.id = %v",
				column.IntegerUniqueCount,
			column.MovingMean,
			column.MovingStandardDeviation,
			column.Id))*/

	if err != nil {
		tracelog.Error(err, packageName, funcName)
		return
	}
	return
}
