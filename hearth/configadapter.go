package hearth

import (
	"github.com/ovlad32/wax/hearth/handling"
	"github.com/ovlad32/wax/hearth/process/dump"
	"github.com/ovlad32/wax/hearth/repository"
)

func AdaptRepositoryConfig(astra *handling.AstraConfigType) (result *repository.RepositoryConfigType) {
	result = &repository.RepositoryConfigType{
		Host:         astra.AstraH2Host,
		Port:         astra.AstraH2Port,
		Password:     astra.AstraH2Password,
		Login:        astra.AstraH2Login,
		DatabaseName: astra.AstraH2Database,
	}
	return
}

func AdaptDataReaderConfig(astra *handling.AstraConfigType) (result *dump.DumperConfigType) {

	result = &dump.DumperConfigType{
		LineSeparator:   astra.AstraLineSeparator,
		ColumnSeparator: astra.AstraColumnSeparator,
		GZip:            astra.AstraDataGZip,
		BufferSize:      astra.AstraReaderBufferSize,
	}

	return result
}
