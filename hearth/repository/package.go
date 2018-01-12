package repository

import (
	"database/sql"
	"fmt"
	"github.com/goinggo/tracelog"
	_ "github.com/lib/pq"
	"log"
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
}

func InitRepository(conf *RepositoryConfigType) (idb *sql.DB, err error) {
	var funcName = "InitRepository"
	tracelog.Started(packageName, funcName)

	idb, err = sql.Open(
		"postgres",
		fmt.Sprintf(
			"user=%v password=%v dbname=%v host=%v port=%v timeout=10 sslmode=disable ",
			conf.Login, conf.Password, conf.DatabaseName, conf.Host, conf.Port,
		),
	)

	if err != nil {
		err = fmt.Errorf("preparing connectivity to backend DB: %v", err)
		tracelog.Error(err, packageName, funcName)
		return
	}

	//ctx := context.Background()
	//c,err := idb2.Conn(ctx)
	if err!=nil {
		log.Fatal(err)
	}

	//c.ExecContext(ctx,"set mode POSTGRESQL")
	if err!=nil{
		log.Fatal(err)
	}

	iDb = idb
	tracelog.Completed(packageName, funcName)
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

/*
func db() *sql.DB {

	idb2, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"user=edm password=edmedm dbname=edm host=localhost port=26257 sslmode=disable "),
	)


}
*/
