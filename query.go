package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/Knetic/go-namedParameterQuery"
)

// Query represents a pending query
type Query struct {
	Sql       string
	Params    map[string]interface{}
	AuthKey   string
	HostMatch string
}

// ResultRow represents a single row of a result
type ResultRow struct {
	Host    string
	Columns []string
	Record  []string
	Err     error
	Done    bool
}

//QueryRunner runs queries
type QueryRunner struct {
	Hosts   map[string]string
	Timeout int
}

//Execute a query across all configured hosts
func (qr *QueryRunner) Execute(query *Query, responseWriter io.Writer) error {

	resultChannel := make(chan ResultRow)
	todo := 0

	for name, dsn := range qr.Hosts {
		//allow filtering of hosts with regexp
		if query.HostMatch != "" {
			r, regexpErr := regexp.Compile(query.HostMatch)
			if regexpErr != nil {
				return regexpErr
			}
			if !r.MatchString(name) {
				continue
			}
		}

		go qr.executeHost(name, dsn, query, resultChannel)
		todo++
	}

	//write CSV to response stream
	c := csv.NewWriter(responseWriter)

	//capture errors and emit only after all data has been returned
	errorResults := []ResultRow{}

	first := true

	for todo > 0 {
		select {
		case row := <-resultChannel:
			if row.Err != nil || row.Done == true {

				todo-- //failed or complete so stop waiting

				if row.Err != nil {
					errorResults = append(errorResults, row)
				}
			} else {
				//output columns on first record only
				if first {
					c.Write(append([]string{"host"}, row.Columns...))
					first = false
				}
				c.Write(append([]string{row.Host}, row.Record...))
			}
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

	return nil
}

//executeHost executes query on a single host
func (qr *QueryRunner) executeHost(name string, dsn string, query *Query, resultChannel chan ResultRow) {

	//open connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		resultChannel <- ResultRow{Host: name, Err: err}
		return
	}
	defer db.Close()

	//create query
	q := namedParameterQuery.NewNamedParameterQuery(query.Sql)
	q.SetValuesFromMap(query.Params)

	//run query
	rows, err := db.Query(q.GetParsedQuery(), (q.GetParsedParameters())...)
	if err != nil {
		resultChannel <- ResultRow{Host: name, Err: err}
		return
	}
	defer rows.Close()

	//get ResultRow columns
	columns, err := rows.Columns()
	if err != nil {
		resultChannel <- ResultRow{Host: name, Err: err}
		return
	}

	scanArgs := make([]interface{}, len(columns))
	values := make([]interface{}, len(columns))

	for i := range values {
		scanArgs[i] = &values[i]
	}

	//start processing ResultRow
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			resultChannel <- ResultRow{Host: name, Err: err}
			return
		}

		record := make([]string, len(columns))

		for i, col := range values {
			if col != nil {
				record[i] = fmt.Sprintf("%s", string(col.([]byte)))
			}
		}

		resultChannel <- ResultRow{Host: name, Columns: columns, Record: record}
	}

	//done
	resultChannel <- ResultRow{Done: true}
}
