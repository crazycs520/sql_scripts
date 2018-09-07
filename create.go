package main

import (
	"fmt"

	"github.com/pingcap/schrodinger-test/util"
)

func main() {
	dbAddr := "127.0.0.1:4000"
	dbDSN := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, "test")
	db, err := util.OpenDB(dbDSN, 8)
	if err != nil {
		return
	}

	numRow := 100
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
