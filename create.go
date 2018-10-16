package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

func getCli() *sql.DB {
	dbAddr := "127.0.0.1:4000"
	dbDSN := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, "test")
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		fmt.Println("can not connect to database.")
		os.Exit(1)
	}
	db.SetMaxOpenConns(1)
	return db
}

func main() {
	//	transaction()
	//create()
	create2()
	// execSqlFromFile()
}

func create() {
	db := getCli()

	numRow := 10000
	startNum := 0

	sqls := []string{
		"drop table if exists t1,t2",
		"drop table if exists tid1,tid2",

		"create table t1 (id int, name varchar(30),addr varchar(30), course varchar(30))",
		"create table t2 (id int, name varchar(30),addr varchar(30), course varchar(30))",
		"create table tid1 (id int)",
		"create table tid2 (id int)",
	}

	for _, sql := range sqls {
		_, err := db.Exec(sql)
		if err != nil {
			return
		}
	}

	// insert
	for i := startNum; i < numRow; i++ {
		sql := fmt.Sprintf("insert into t1 values (%d, \"name_abcd_%d\", \"address_abcd_%d\" , \"course_abcd_%d\"); insert into t2 values (%d, \"name_abcd_%d\", \"address_abcd_%d\" , \"course_abcd_%d\");", i, i, i, i, i, i, i, i)
		_, err := db.Exec(sql)
		if err != nil {
			return
		}
	}

	// insert
	for i := startNum; i < numRow; i++ {
		sql := fmt.Sprintf("insert into tid1 set id=%d;insert into tid2 set id=%d", i, i+numRow-1)
		_, err := db.Exec(sql)
		if err != nil {
			return
		}
	}

}

func create2() {
	db := getCli()

	sqls := []string{
		"drop table if exists t",
		"create table t (a tinyint, b tinyint, primary key(a), index idx(a, b))",
	}
	for _, sql := range sqls {
		_, err := db.Exec(sql)
		if err != nil {
			return
		}
	}

	for i := 0; i < 20; i++ {
		sql := fmt.Sprintf("insert into t values (%d, %d)", i, i)
		_, err := db.Exec(sql)
		if err != nil {
			return
		}
	}

	// sql := "analyze table t with 3 buckets;"
	// _, err := db.Exec(sql)
	// if err != nil {
	//     return
	// }
	//
	// for i := 30; i < 40; i++ {
	//     sql := fmt.Sprintf("insert into t values (%d, %d)", i, i)
	//     _, err := db.Exec(sql)
	//     if err != nil {
	//         return
	//     }
	// }
}

func transaction() {
	db1 := getCli()
	db2 := getCli()
	db3 := getCli()
	sql := `drop table if exists t;
	CREATE TABLE t (
  a int(11) NOT NULL AUTO_INCREMENT,
  b int(11) DEFAULT NULL,
  PRIMARY KEY (a)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;
insert into t values(1, 0);`

	db3.Exec(sql)
	fmt.Println("initial")
	selectAndPrint(db3, "select b from t where a = 1")
	fmt.Println("-------------")
	for i := 0; i < 10; i++ {
		db1.Exec("SET autocommit=0")
		db2.Exec("begin")
		db1.Exec("begin")
		db2.Exec("update t set b=2 where a=1")
		selectAndPrint(db1, "select b from t where a = 1")
		db1.Exec("update t set b=0 where a=1")
		db1.Exec("commit")
		db2.Exec("commit")
	}
}

func selectAndPrint(db *sql.DB, sql string) {
	// execute
	rows, err := db.Query(sql)

	if err == nil {
		defer rows.Close()
	}
	// When column is removed, SELECT statement may return error so that we ignore them here.

	if err != nil {
		return
	}

	// Read all rows.
	var actualRows [][]interface{}
	for rows.Next() {
		cols, err1 := rows.Columns()
		if err1 != nil {
			return
		}

		// See https://stackoverflow.com/questions/14477941/read-select-columns-into-string-in-go
		rawResult := make([][]byte, len(cols))
		result := make([]interface{}, len(cols))
		dest := make([]interface{}, len(cols))
		for i := range rawResult {
			dest[i] = &rawResult[i]
		}

		err1 = rows.Scan(dest...)
		if err1 != nil {
			return
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {
				val := string(raw)

				result[i] = val
			}
		}

		actualRows = append(actualRows, result)
	}
	if rows.Err() != nil {
		return
	}

	for _, row := range actualRows {
		rowString := ""
		for _, col := range row {
			rowString += fmt.Sprintf("%v,", col)
		}
		fmt.Println(rowString)
	}
}

func execSqlFromFile() {
	file, err := os.Open("sqls.sql")
	if err != nil {
		return
	}
	defer file.Close()
	db := getCli()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		sql := scanner.Text()
		_, err = db.Exec(sql)
		if err != nil {
			fmt.Printf("\n\nexec sql: %s, error: %#v", sql, err)
			return
		}
		fmt.Println(sql)
	}
	fmt.Printf("\n\n finish execute all sql in file")

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
