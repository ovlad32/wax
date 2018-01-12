package handling

import (
	"encoding/json"
	"github.com/goinggo/tracelog"
	"github.com/ovlad32/wax/hearth/handling/sparsebitset"
	"os"
	"path/filepath"
)
type BackendDatabaseType struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     string `json:"port"`
	Login    string `json:"login"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type AstraConfigType struct {
	BackendDatabases        []BackendDatabaseType `json:"be-databases"`
	AstraH2Host             string `json:"astra-h2-host"`
	AstraH2Port             string `json:"astra-h2-port"`
	AstraH2Login            string `json:"astra-h2-login"`
	AstraH2Password         string `json:"astra-h2-password"`
	AstraH2Database         string `json:"astra-h2-database"`
	AstraDumpPath           string `json:"astra-dump-path"`
	AstraDataGZip           bool   `json:"astra-data-gzip"`
	AstraColumnSeparator    byte   `json:"astra-column-byte-separator"`
	AstraLineSeparator      byte   `json:"astra-line-byte-separator"`
	BitsetPath              string `json:"bitset-path"`
	KVStorePath             string `json:"kv-store-path"`
	AstraReaderBufferSize   int    `json:"astra-reader-buffer-size"`
	TableWorkers            int    `json:"table-workers"`
	CategoryWorkersPerTable int    `json:"category-worker-per-table"`
	CategoryDataChannelSize int    `json:"category-data-channel-size"`
	RawDataChannelSize      int    `json:"raw-data-channel-size"`
	EmitRawData             bool   `json:"emit-raw-data"`
	EmitHashValues          bool   `json:"emit-hash-data"`
	BuildBinaryDump         bool   `json:"build-binary-dump"`
	SpeedTickTimeSec        int    `json:"speed-tick-time-sec"`
	MemUsageTickTimeSec     int    `json:"memory-usage-tick-time-sec"`
	LogBaseFile             string `json:"log-base-file"`
	LogBaseFileKeepDay      int    `json:"log-base-file-keep-day"`
}

func ReadConfig() (result *AstraConfigType, err error) {
	funcName := "handling::ReadConfig"
	ex, err := os.Executable()
	exPath := filepath.Dir(ex)

	pathToConfigFile := filepath.Join(exPath, "config.json")

	if _, err := os.Stat(pathToConfigFile); os.IsNotExist(err) {
		tracelog.Error(err, funcName, "Specify correct path to config.json")
		return nil, err
	}

	conf, err := os.Open(pathToConfigFile)
	if err != nil {
		tracelog.Errorf(err, funcName, "Opening config file %v", pathToConfigFile)
		return nil, err
	}
	jd := json.NewDecoder(conf)
	result = new(AstraConfigType)
	err = jd.Decode(result)
	if err != nil {
		tracelog.Errorf(err, funcName, "Decoding config file %v", pathToConfigFile)
		return nil, err
	}

	return result, nil
}

func NewSparseBitset() BitsetInterface {
	return sparsebitset.New(0)
}
