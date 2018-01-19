package repository

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var packageName string = "repository"

type whereFunc func() string
type BEDBType string
var (
	H2 BEDBType = "H2"
	CC BEDBType = "CC"
	PG BEDBType = "PG"
)

var iDb *sql.DB

var currentDbType BEDBType = H2

var iConfig *RepositoryConfigType

type RepositoryConfigType struct {
	Login        string
	Password     string
	DatabaseName string
	Host         string
	Port         string
	Logger       logrus.Logger
}

func InitRepository(conf *RepositoryConfigType) (idb *sql.DB, err error) {
	func() {
		idb, err = sql.Open(
			"postgres",
			fmt.Sprintf(
				"user=%v password=%v dbname=%v host=%v port=%v timeout=10 sslmode=disable ",
				conf.Login, conf.Password, conf.DatabaseName, conf.Host, conf.Port,
			),
		)

		if err != nil {
			err = fmt.Errorf("could not connect to backend DB: %v", err)
			return
		}
		iDb = idb
	}()

	if err != nil {
		conf.Logger.Errorf("could not initialize backend DB Repository: %v",err)
	}

	return
}

func MakeWhereArgs() (result []interface{}){
	return make([]interface{},0,1)
}

func MakeWhereArgsNum(n int) (result []interface{}){
	return make([]interface{},0,n)

}

func MakeWhereFunc() (result whereFunc) {
	return func() string { return "" }
}
