package dto

import (
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/handling/nullable"
)

type ContentFeatureType struct {
	Column          *ColumnInfoType
	Key             string
	ByteLength      int
	IsNumeric       bool
	IsNegative      bool
	IsInteger       bool
	HashUniqueCount nullable.NullInt64
	ItemUniqueCount nullable.NullInt64
	NonNullCount    nullable.NullInt64
	MinStringValue  nullable.NullString
	MaxStringValue  nullable.NullString
	MinNumericValue nullable.NullFloat64
	MaxNumericValue nullable.NullFloat64
	/*Stats           struct {
		MinStringValue          string
		MaxStringValue          string
		MinNumericValue         float64
		MaxNumericValue         float64
		NonNullCount            uint64
		MovingMean              float64
		MovingStandardDeviation float64

		ItemBitset              *sparsebitset.BitSet
		ItemBitsetCardinality   uint64
		HashBitset              *sparsebitset.BitSet
		HashBitsetCardinality   uint64
	}*/
	MovingMean              nullable.NullFloat64
	MovingStandardDeviation nullable.NullFloat64
	stats                   struct {
		minStringValue      string
		maxStringValue      string
		minNumericValue     float64
		maxNumericValue     float64
		nonNullCount        uint64
		bitsets             []handling.BitsetInterface
		bitsetCardinalities []uint64
	}
}

type ContentFeatureArrayType []*ContentFeatureType
type ContentFeatureMapType map[string]*ContentFeatureType
