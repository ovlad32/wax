package repository

import (
	"database/sql"
	"fmt"
	"github.com/goinggo/tracelog"
	_ "github.com/lib/pq"
	"log"
)

var packageName string = "wax.hearth.repository.meta"

var iDb *sql.DB
var iConfig *RepositoryConfigType

type RepositoryConfigType struct {
	Login        string
	Password     string
	DatabaseName string
	Host         string
	Port         string
}

func InitRepository(conf *RepositoryConfigType) (err error) {
	var funcName = "InitRepository"
	tracelog.Started(packageName, funcName)

	idb, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"user=%v password=%v dbname=%v host=%v port=%v timeout=10 sslmode=disable ",
			conf.Login, conf.Password, conf.DatabaseName, conf.Host, conf.Port,
		),
	)

	idb2, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"user=edm password=edmedm dbname=edm host=localhost port=26257 sslmode=disable "),
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

	result,err  := idb2.Query("select $1::text from test ","b")
	if err!=nil{
		log.Fatal(err)
	}
	for result.Next(){
		var s string
		result.Scan(&s)
		fmt.Printf("%s\n",s)
	}


	iDb = idb
	iConfig = conf
	//os.Exit(1)

	tracelog.Completed(packageName, funcName)
	return
}
