package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-martini/martini"
)

import _ "github.com/go-sql-driver/mysql"

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

//import _ "github.com/go-sql-driver/mysql"

type QueryRunner struct {
	hosts map[string]string
}

func (qr *QueryRunner) Execute(query *Query) {

	for name, dsn := range qr.hosts {
		log.Print(name, " ", dsn)
		qr.runQueryOnHost(dsn, query)
	}

}

func (qr *QueryRunner) runQueryOnHost(dsn string, query *Query) {

	db, err := sql.Open("mysql", dsn)
	checkError(err)
	defer db.Close()

	rows, err := db.Query("SELECT * FROM test1")
	checkError(err)
	defer rows.Close()

	columns, err := rows.Columns()
	checkError(err)

	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err = rows.Scan(scanArgs...)
		checkError(err)

		record := make(map[string]interface{})

		for i, col := range values {
			if col != nil {
				record[columns[i]] = fmt.Sprintf("%s", string(col.([]byte)))
			}
		}

		s, _ := json.Marshal(record)
		fmt.Printf("%s\n", s)
	}

}

type Query struct {
	Sql string
}

func main() {

	hosts := make(map[string]string)
	hosts["localhost"] = "root:@/xquery"

	qr := QueryRunner{hosts: hosts}

	m := martini.Classic()

	m.Post("/query", func(req *http.Request) (int, string) {

		decoder := json.NewDecoder(req.Body)

		var query Query
		err := decoder.Decode(&query)
		if err != nil {
			log.Panic(err)
		}

		qr.Execute(&query)

		return 200, query.Sql
	})

	m.Run()
}
