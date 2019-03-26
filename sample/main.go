package main

import (
	"database/sql"
	"log"
	"sqlhooks"
	loghooks "sqlhooks/sample/log"
	sqlconnection "sqlhooks/sample/sql"

	"github.com/go-sql-driver/mysql"
)

func main() {
	sql.Register("mysqllog", sqlhooks.Wrap(&mysql.MySQLDriver{}, loghooks.New()))
	db, err := sql.Open("mysqllog", sqlconnection.NewDatabaseServerName(sqlconnection.Connection{
		Host:         "localhost",
		Port:         3306,
		DatabaseName: "TEST_DATABASE",
		User:         "root",
		Password:     "123456",
		Protocol:     "tcp",
	}))
	if err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec("CREATE TABLE USERS(ID int, name text)"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(`INSERT INTO USERS (id, name) VALUES(?, ?)`, 1, "Hello World"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Query(`SELECT id, name FROM USERS`); err != nil {
		log.Fatal(err)
	}
}
