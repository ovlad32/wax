package repository

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/ovlad32/wax/hearth/handling/nullable"
	"context"
	"strings"
)

var packageName string = "repository"

type varray []interface{}

func(v varray ) valuePlaceholders() string{
	a:=make([]string,len(v))
	for index := range v {
		a[index] = `?`
	}
	return `values (`+strings.Join(a,`,`)+`)`
}


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

/*
func MakeWhereArgs() (result []interface{}){
	return make([]interface{},0,1)
}

func MakeWhereArgsNum(n int) (result []interface{}){
	return make([]interface{},0,n)

}

func MakeWhereFunc() (result whereFunc) {
	return func() string { return "" }
}
*/



func h2ArgValues(in... interface{}) (out[]interface{}) {
	out = make([]interface{},0,len(in))
	for _, iValue := range in {
		switch rValue := iValue.(type) {
		case string:
			if rValue == "" {
				out = append(out, nullable.NullString{})
			} else{
				out = append(out, nullable.NewNullString(rValue))
			}
		default:
			out = append(out,rValue)
		}
	}
	return out
}

func convert2H2(statement string, args []interface{}) (string, []interface{}) {
	const NullString = "null"
	maskQuotes := func(s string) string {
		s = " '" + strings.Replace(s, "'", "''", -1) + "'"
		return s
	}

	if currentDbType != H2 {
		return statement,args
	}
	statement = strings.Replace(statement,"?","%v",-1)
	for index, iValue := range args {
		switch rValue := iValue.(type) {
		case string:
			if rValue == "" {
				args[index] = NullString
			} else{
				args[index] = maskQuotes(rValue)
			}
		case nullable.NullString:
			if !rValue.Valid() {
				args[index] = NullString
			} else{
				//args[index] = maskQuotes(rValue.InternalValue.String)
				args[index] = rValue.InternalValue.String
			}
		default:
			args[index] = iValue
		}
	}
	statement = fmt.Sprintf(statement,args...)
	return statement,args[0:0]
}

func ExecContext(ctx context.Context, statement string,args...interface{}) (result sql.Result, err error) {
	statement, args = convert2H2(statement,args)
	return iDb.ExecContext(ctx, statement, args...)
}

func QueryContext(ctx context.Context, statement string,args...interface{}) (result *sql.Rows, err error) {
	statement, args = convert2H2(statement,args)
	return iDb.QueryContext(ctx, statement, args...)
}

func QueryRowContext(ctx context.Context, statement string,args...interface{}) (result *sql.Row) {
	statement, args = convert2H2(statement,args)
	return iDb.QueryRowContext(ctx, statement, args...)
}


