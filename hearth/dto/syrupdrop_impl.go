package dto

import (
	"github.com/ovlad32/wax/hearth/handling"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
)

func NewSyrupDrop(column *ColumnInfoType, rawData []byte) (drop *SyrupDropType) {
	rawDataLength := len(rawData)
	if rawDataLength > 0 {
		drop = &SyrupDropType{
			RawData:       rawData,
			RawDataLength: rawDataLength,
			Column:        column,
		}
	}
	return drop
}

func (drop *SyrupDropType) DiscoverContentFeature(buildContentBitset bool) {

	stringValue := strings.Trim(string(drop.RawData), " ")

	var floatValue, truncatedFloatValue float64 = 0, 0
	var parseError error

	isNumeric, isInteger, isNegative := false, false, false
	if len(stringValue) > 0 {
		floatValue, parseError = strconv.ParseFloat(stringValue, 64)
		if isNumeric = parseError == nil; isNumeric {
			drop.HashData = math.Float64bits(floatValue)
			truncatedFloatValue = math.Trunc(floatValue)
			isInteger = truncatedFloatValue == floatValue
			isNegative = floatValue < float64(0)
		}
	}

	featureKey := NewSyrupFeatureStringKey(isNumeric, isInteger, isNegative, drop.RawDataLength)

	drop.ContentFeature = drop.Column.FindContentFeatureByStringKey(
		featureKey,
		func() (result *ContentFeatureType) {
			result = NewContentFeature(drop)
			result.Key = featureKey
			result.IsNumeric = isNumeric
			result.IsInteger = isInteger
			result.IsNegative = isNegative
			return
		},
	)

	drop.ContentFeature.stats.nonNullCount++

	if isNumeric {
		if drop.ContentFeature.stats.maxNumericValue < floatValue {
			drop.ContentFeature.stats.maxNumericValue = floatValue
		}
		if drop.ContentFeature.stats.minNumericValue > floatValue {
			drop.ContentFeature.stats.minNumericValue = floatValue
		}
		if buildContentBitset && isInteger {
			pureContentIndex := PureContent.Index()
			if drop.ContentFeature.stats.bitsets[pureContentIndex] == nil {
				drop.ContentFeature.stats.bitsets[pureContentIndex] = handling.NewSparseBitset()
			}
			if isNegative {
				if !drop.ContentFeature.stats.bitsets[pureContentIndex].Set(uint64(-truncatedFloatValue)) {
					drop.ContentFeature.stats.bitsetCardinalities[pureContentIndex]++
				}
			} else {
				if !drop.ContentFeature.stats.bitsets[pureContentIndex].Set(uint64(truncatedFloatValue)) {
					drop.ContentFeature.stats.bitsetCardinalities[pureContentIndex]++
				}
			}
		}
	} else {
		if buildContentBitset {
			//if columnData.DataCategory.Stats.ItemBitset == nil {
			//	columnData.DataCategory.Stats.ItemBitset = sparsebitset.New(0)
			//}
			//for _, charValue := range stringValue {
			//	columnData.DataCategory.Stats.ItemBitset.Set(uint64(charValue))
			//}
		}
		if drop.ContentFeature.stats.maxStringValue == "" ||
			drop.ContentFeature.stats.maxStringValue < stringValue {
			drop.ContentFeature.stats.maxStringValue = stringValue
		}
		if drop.ContentFeature.stats.minStringValue == "" ||
			drop.ContentFeature.stats.minStringValue > stringValue {
			drop.ContentFeature.stats.minStringValue = stringValue
		}
	}
	return

}

func (drop *SyrupDropType) Hash(buildHashBitset bool) (err error) {
	if drop.RawDataLength > 0 {
		if !drop.ContentFeature.IsNumeric {
			if drop.RawDataLength > handling.HashLength {
				var hashMethod = fnv.New64()
				hashMethod.Write(drop.RawData)
				drop.HashData = hashMethod.Sum64()
			} else {
				drop.HashData = 0
				for dPos, dByte := range drop.RawData {
					drop.HashData = drop.HashData | uint64(dByte<<(uint64(dPos)))
				}
			}
		}
	}

	bitset := drop.ContentFeature.stats.bitsets[HashContent.Index()]
	bitset.Set(drop.HashData)

	return
}
