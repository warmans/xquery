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
	"github.com/spf13/viper"
)

import _ "github.com/go-sql-driver/mysql"

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type QueryRunner struct {
	Hosts map[string]string
}

type Result struct {
	Host   string
	Record []string
	Err    error
	Done   bool
}

func (qr *QueryRunner) Execute(query *Query, responseWriter io.Writer) {

	resultChannel := make(chan Result)

	for name, dsn := range qr.Hosts {
		go qr.runQueryOnHost(name, dsn, query, resultChannel)
	}

	//write CSV to response stream
	c := csv.NewWriter(responseWriter)

	//capture errors and emit only after all data has been returned
	errorResults := []Result{}

	timeout := false
	todo := len(qr.Hosts)

	for timeout != true && todo > 0 {
		select {
		case row := <-resultChannel:
			if row.Err != nil || row.Done == true {

				todo-- //failed or complete so stop waiting

				if row.Err != nil {
					errorResults = append(errorResults, row)
				}
			} else {
				c.Write(append([]string{row.Host}, row.Record...))
			}
		case <-time.After(time.Second * 10): //todo: implement actual timeout member of Query
			timeout = true
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if len(errorResults) > 0 {
		c.Write([]string{""})
		c.Write([]string{"DATA ENDS! ERRORS FOLLOW:"})
		for _, errorRow := range errorResults {
			c.Write([]string{errorRow.Host, fmt.Sprintf("%s", errorRow.Err)})
		}
	}

	c.Flush()
}

func (qr *QueryRunner) runQueryOnHost(name string, dsn string, query *Query, resultChannel chan Result) {

	//open connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		resultChannel <- Result{Host: name, Err: err}
		return
	}
	defer db.Close()

	//run query
	rows, err := db.Query(query.Sql)
	if err != nil {
		resultChannel <- Result{Host: name, Err: err}
		return
	}
	defer rows.Close()

	//get result columns
	columns, err := rows.Columns()
	if err != nil {
		resultChannel <- Result{Host: name, Err: err}
		return
	}

	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	//start processing result
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			resultChannel <- Result{Host: name, Err: err}
			return
		}

		record := make([]string, len(columns))

		for i, col := range values {
			if col != nil {
				record[i] = fmt.Sprintf("%s", string(col.([]byte)))
			}
		}

		resultChannel <- Result{Host: name, Record: record}
	}

	//done
	resultChannel <- Result{Done: true}
}

type Query struct {
	Sql     string
	Timeout int
}

func main() {

	m := martini.Classic()

	//config

	config := viper.New()
	config.SetConfigName("xquery")
	config.AddConfigPath("./config")
	config.ReadInConfig()

	m.Map(&config)

	//query runner
	m.Map(&QueryRunner{Hosts: config.GetStringMapString("dbs")})

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
