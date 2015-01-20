package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

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
	Hosts map[string]string
}

func (qr *QueryRunner) Execute(query *Query, responseWriter io.Writer) {

	resultChannel := make(chan []string)

	for name, dsn := range qr.Hosts {
		go qr.runQueryOnHost(name, dsn, query, resultChannel)
	}

	timeout := false
	c := csv.NewWriter(responseWriter)

	todo := len(qr.Hosts)
	for timeout != true && todo > 0 {
		select {
		case row := <-resultChannel:
			if row == nil {
				todo--
			} else {
				c.Write(row)
			}
		case <-time.After(time.Second):
			timeout = true
		default:
			time.Sleep(time.Millisecond)
		}
	}
	c.Flush()
}

func (qr *QueryRunner) runQueryOnHost(name string, dsn string, query *Query, resultChannel chan []string) {

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

		record := make([]string, len(columns))

		for i, col := range values {
			if col != nil {
				record[i] = fmt.Sprintf("%s", string(col.([]byte)))
			}
		}

		resultChannel <- record
	}

	resultChannel <- nil
}

type Query struct {
	Sql string
}

func main() {

	hosts := make(map[string]string)
	hosts["localhost1"] = "root:@/xquery"
	hosts["localhost2"] = "root:@/xquery"
	hosts["localhost3"] = "root:@/xquery"
	hosts["localhost4"] = "root:@/xquery"

	m := martini.Classic()

	//services
	m.Map(&QueryRunner{Hosts: hosts})

	//routes
	m.Post("/query", func(res http.ResponseWriter, req *http.Request, qr *QueryRunner) {

		decoder := json.NewDecoder(req.Body)

		var query Query
		err := decoder.Decode(&query)
		if err != nil {
			log.Panic(err)
		}

		qr.Execute(&query, res)
	})

	m.Run()
}
