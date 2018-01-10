package dto

import (
	"context"
	"fmt"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"math"
	"strconv"
	"sync"
)

const VarcharMax = 4000

type BitsetContentType int

const (
	HashContent BitsetContentType = iota
	PureContent
	TotalCount
)

func (value BitsetContentType) String() string {
	switch value {
	case HashContent:
		return "hash"
	case PureContent:
		return "pure"
	default:
		panic(fmt.Sprintf("BitsetContentType value [%v] has not been recognized!", int(value)))
	}
}

func (value BitsetContentType) Index() int {
	if value >= 0 && value < TotalCount {
		return int(value)
	} else {
		panic(fmt.Sprintf("BitsetContentType value [%v] has not been recognized!", int(value)))
	}
}

type BitsetContentArrayType []BitsetContentType

func NewBitSetContents(b ...BitsetContentType) BitsetContentArrayType {
	return b
}

func (b BitsetContentArrayType) IsHashContent() bool {
	for _, v := range b {
		if v == HashContent {
			return true
		}
	}
	return false
}

func (b BitsetContentArrayType) IsPureContent() bool {
	for _, v := range b {
		if v == PureContent {
			return true
		}
	}
	return false
}

type featureKeyStorageType struct {
	codes map[uint16]string
	sync.Mutex
}

var featureKeyStorage = featureKeyStorageType{
	codes: make(map[uint16]string),
}

func NewContentFeature(drop *SyrupDropType) (result *ContentFeatureType) {
	result = &ContentFeatureType{
		Column: drop.Column,
	}
	result.stats.minNumericValue = math.MaxFloat64
	result.stats.maxNumericValue = -math.MaxFloat64
	result.stats.nonNullCount = 0
	result.stats.bitsets = make([]handling.BitsetInterface, TotalCount)
	result.stats.bitsetCardinalities = make([]uint64, TotalCount)
	for index := range result.stats.bitsets {
		result.stats.bitsets[index] = handling.NewSparseBitset()
	}
	return
}

func NewSyrupFeatureStringKey(isNumeric bool, isInteger bool, isNegative bool, byteLength int) (result string) {
	if !isNumeric {
		vLen16 := uint16(byteLength)
		featureKeyStorage.Lock()
		if code, found := featureKeyStorage.codes[vLen16]; !found {
			code = "C" + strconv.FormatInt(int64(byteLength), 36)
			featureKeyStorage.codes[vLen16] = code
			result = code
		} else {
			result = code
		}
		featureKeyStorage.Unlock()
	} else if isNegative {
		if isInteger {
			result = "N"
		} else {
			result = "n"
		}
	} else {
		if isInteger {
			result = "P"
		} else {
			result = "p"
		}
	}
	return result
}

func (feature *ContentFeatureType) BinaryKey() (result []byte) {
	result = make([]byte, 2, 2)

	if feature.IsNumeric {
		result[0] = byte(1 << 7)
		if feature.IsInteger {
			result[0] = result[0] | byte(1<<6)
		}
		if feature.IsNegative {
			result[0] = result[0] | byte(1<<5)
		}
	} else {
		bl := uint16(feature.ByteLength)
		result[1] = byte(bl & 0xFF)
		result[0] = byte((bl >> 8) & 0x7F)
	}
	return result
}

func (feature ContentFeatureType) String() (result string) {
	result = fmt.Sprintf("feature(Key:%v) on %v.%v.", feature.Key, feature.Column.TableInfo, feature.Column)
	return
}

func (feature ContentFeatureType) BitsetFileName(suffix BitsetContentType) (fileName string, err error) {
	funcName := "featureType.HashBitsetFileName"
	tracelog.Started(packageName, funcName)
	fileName = fmt.Sprintf("%v.%v.%v.bitset",
		feature.Column.Id.String(),
		feature.Key,
		suffix,
	)

	tracelog.Completed(packageName, funcName)

	return fileName, nil
}

func (feature *ContentFeatureType) WriteBitsetToDisk(ctx context.Context, pathToDir string, contentType BitsetContentType) (err error) {

	return handling.WriteBitsetToFile(
		ctx,
		pathToDir,
		&contentFeatureBitSetWrapperType{
			ContentFeatureType: feature,
			bitsetContentType:  contentType,
		},
	)
}

type contentFeatureBitSetWrapperType struct {
	*ContentFeatureType
	bitsetContentType BitsetContentType
}

func (w contentFeatureBitSetWrapperType) Description() string {
	return fmt.Sprintf("%v", w.bitsetContentType)
}
func (w contentFeatureBitSetWrapperType) BitSet() (handling.BitsetInterface, error) {
	index := w.bitsetContentType.Index()
	return w.stats.bitsets[index], nil
}

func (w contentFeatureBitSetWrapperType) FileName() (string, error) {
	return fmt.Sprintf("%v.%v.%v.bitset",
		w.Column.Id.String(),
		w.Key,
		w.bitsetContentType,
	), nil
}

func (feature *ContentFeatureType) ReadBitsetFromDisk(ctx context.Context, pathToDir string, bitsetContentType BitsetContentType) (err error) {
	wrapper := &contentFeatureBitSetWrapperType{
		ContentFeatureType: feature,
		bitsetContentType:  bitsetContentType,
	}

	err = handling.ReadBitsetFromFile(
		ctx,
		pathToDir,
		wrapper,
	)

	if err != nil {
		fmt.Errorf("reading %v bitset from file:%v", wrapper, err)
		return

	}
	index := bitsetContentType.Index()
	bitset := feature.stats.bitsets[index]
	feature.stats.bitsetCardinalities[index] = bitset.Cardinality()
	return err
}

func (feature *ContentFeatureType) ResetBitset(bitsetContentType BitsetContentType) {
	index := bitsetContentType.Index()
	feature.stats.bitsets[index] = nil
	feature.stats.bitsetCardinalities[index] = 0
}

func (feature *ContentFeatureType) UpdateStatistics(runContext context.Context) (err error) {

	if feature.IsNumeric {
		feature.MaxNumericValue = nullable.NewNullFloat64(feature.stats.maxNumericValue)
		feature.MinNumericValue = nullable.NewNullFloat64(feature.stats.minNumericValue)
		feature.MaxStringValue = nullable.NullString{}
		feature.MinStringValue = nullable.NullString{}
	} else {
		if len(feature.stats.maxStringValue) > VarcharMax {
			feature.stats.maxStringValue = feature.stats.maxStringValue[:VarcharMax]
		}
		feature.MaxStringValue = nullable.NewNullString(feature.stats.maxStringValue)

		if len(feature.stats.minStringValue) > VarcharMax {
			feature.stats.minStringValue = feature.stats.minStringValue[:VarcharMax]
		}
		feature.MinStringValue = nullable.NewNullString(feature.stats.minStringValue)
	}
	feature.NonNullCount = nullable.NewNullInt64(int64(feature.stats.nonNullCount))

	index := HashContent.Index()
	if feature.stats.bitsets[index] != nil {
		feature.HashUniqueCount =
			nullable.NewNullInt64(int64(feature.stats.bitsets[index].Cardinality()))
	}

	index = PureContent.Index()
	if feature.stats.bitsets[index] != nil {
		feature.ItemUniqueCount =
			nullable.NewNullInt64(int64(feature.stats.bitsets[index].Cardinality()))
	}
	bitset := feature.stats.bitsets[index]

	if feature.IsNumeric && bitset != nil {

		var count uint64 = 0
		var meanValue, cumulativeDeviation float64 = 0, 0

		increasingOrder := context.WithValue(runContext, "sort", true)

		var prevValue = uint64(0)
		var gotPrevValue = false
		for value := range bitset.BitChan(increasingOrder) {
			if !gotPrevValue {
				prevValue = value
				gotPrevValue = true
			} else {
				count++
				cumulativeDeviation += float64(value) - float64(prevValue)
				prevValue = value
			}
		}
		if count > 0 {
			var countInDeviation uint64 = 0
			meanValue = cumulativeDeviation / float64(count)
			var totalDeviation = float64(0)

			var prevValue = uint64(0)
			var gotPrevValue = false
			for value := range bitset.BitChan(increasingOrder) {
				if !gotPrevValue {
					prevValue = value
					gotPrevValue = true
				} else {
					countInDeviation++
					totalDeviation = totalDeviation + math.Pow(meanValue-(float64(value)-float64(prevValue)), 2)
					prevValue = value
				}
			}
			if countInDeviation > 1 {
				stdDev := math.Sqrt(totalDeviation / float64(countInDeviation-1))
				feature.MovingStandardDeviation = nullable.NewNullFloat64(stdDev)
			}
			feature.MovingMean = nullable.NewNullFloat64(meanValue)
		}
	}

	return err
}
