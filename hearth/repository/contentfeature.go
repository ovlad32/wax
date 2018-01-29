package repository

import (
	"context"
	"fmt"
	"github.com/ovlad32/wax/hearth/dto"
)


func PutContentFeature(ctx context.Context, feature *dto.ContentFeatureType) (err error) {
	data :=varray{
		feature.Column.Id,
		feature.Key,
		feature.ByteLength,
		feature.IsNumeric,
		feature.IsNegative,
		feature.IsInteger,
		feature.TotalCount,
		feature.HashUniqueCount,
		feature.ItemUniqueCount,
		feature.MinStringValue,
		feature.MaxStringValue,
		feature.MinNumericValue,
		feature.MaxNumericValue,
		feature.MovingMean,
		feature.MovingStandardDeviation,
		}

	ExecContext(ctx,
		`merge into column_feature_stats(
				 column_info_id 
				, key 
				, byte_length 
				, is_numeric 
				, is_negative 
				, is_integer 
				, total_count 
				, hash_unique_count 
				, item_unique_count 
				, min_sval 
				, max_sval 
				, min_fval 
				, max_fval 
				, moving_mean 
				, moving_stddev 
			 ) 
             key (column_info_id, key) 
			 values(`+ParamPlaces(len(data))+`)`,

		)

	if err != nil {
		err = fmt.Errorf("could not persist column_feature_stats object: %v",err)
		return
	}

	return
}
