package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-martini/martini"
	"github.com/spf13/viper"
)

import _ "github.com/go-sql-driver/mysql"

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
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
		if err := decoder.Decode(&query); err != nil {
			res.WriteHeader(400)
			fmt.Fprintf(res, "Bad Request: %s", err)
			return
		}

		//authorize user
		valid := false
		for _, key := range config.GetStringMapString("authkeys") {
			if key == query.AuthKey {
				valid = true
				break
			}
		}
		if !valid {
			res.WriteHeader(403)
			fmt.Fprintf(res, "Auth Failed")
			return
		}

		err := qr.Execute(&query, res)
		if err != nil {
			res.WriteHeader(400)
			fmt.Fprintf(res, "Request Failed: %s", err)
		}

	})

	m.Run()
}
