package dto

type SyrupDropType struct {
	Column         *ColumnInfoType
	ContentFeature *ContentFeatureType
	LineNumber     uint64
	RawData        []byte
	RawDataLength  int
	HashData       uint64
}
