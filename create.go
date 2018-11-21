package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

func getCli() *sql.DB {
	//dbAddr := "172.16.30.34:4001"
	dbAddr := "127.0.0.1:4000"
	//dbAddr := "192.168.197.180:4000"
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
	// create2()
	// execSqlFromFile()

	// multiTransaction()
	//addIndex(10, "t_wide")
	addIndexUpdate("t_wide",20,800000,10*time.Millisecond,)
	//createData(100)
	//createDataSlim(10)
	// selectCount(db, "select count(*) from t1 where a=1 and b=3;")
}

func addIndexUpdate(t string, updateNum , rowLen int, sleep time.Duration) {
	done := make(chan struct{})
	for i:=0;i<updateNum;i++{
		go updateWhenAddindex(t, rowLen,sleep,  done)
	}
	addIndex(20, t)
	close(done)
	time.Sleep(100 * time.Millisecond)
}

func addIndex(num int, tName string) {
	if num < 4 {
		num = 4
	}
	db := getCli()
	_, err := db.Exec("set @@tidb_ddl_reorg_worker_cnt=1;")
	checkErr(err)
	_, err = db.Exec("set @@global.tidb_ddl_reorg_worker_cnt=1;")
	checkErr(err)
	times := make([]float64, 0, num)
	checkErr(err)
	for i := 0; i < num; i++ {
		start := time.Now()
		sql := fmt.Sprintf("alter table %s add index idx_cs_%v (a)", tName, i+600)
		_, err = db.Exec(sql)
		checkErr(err)
		pass := time.Since(start).Seconds()
		times = append(times, pass)
		fmt.Println(pass)
	}
	sort.Float64s(times)
	avgTime := float64(0)
	for _, v := range times {
		//fmt.Println(v)
		avgTime += v
	}
	avgTime = avgTime - times[0] - times[len(times)-1]
	avgTime = avgTime / float64((len(times) - 2))
	fmt.Printf("avg: %vs\n", avgTime)

	_, err = db.Exec("commit")
	checkErr(err)
}

func updateWhenAddindex(tName string, rowLen int, sleep time.Duration, done chan struct{}) {
	db := getCli()
	num := 0
	for {
		select {
		case <-done:
			fmt.Printf("\nupdate %d rows\n", num)
			return
		default:
		}
		n := rand.Intn(rowLen)

		sql := fmt.Sprintf(" update %s set a=%d where a=%d;", tName, n, n+1)
		_, err := db.Exec(sql)
		checkErr(err)
		num++
	}
}

func createDataSlim(num int) {
	db := getCli()
	tableName := "t_slim"
	sql := fmt.Sprintf("drop table if exists %s", tableName)
	_, err := db.Exec(sql)
	checkErr(err)
	sql = fmt.Sprintf("create table %s (a int, b int);", tableName)
	_, err = db.Exec(sql)
	checkErr(err)
	batchCnt := 2048
	_, err = db.Exec("begin")
	checkErr(err)
	for i := 0; i < num; i++ {
		if i%batchCnt == 0 {
			_, err = db.Exec("commit")
			checkErr(err)
			_, err = db.Exec("begin")
			checkErr(err)
		}
		sql = fmt.Sprintf("insert into %s values (%v,%v)", tableName, i, i+1)
		_, err = db.Exec(sql)
		checkErr(err)

	}
	_, err = db.Exec("commit")
	checkErr(err)
}

func createData(num int) {
	db := getCli()
	tableName := "t_wide"
	sql := fmt.Sprintf("drop table if exists %s", tableName)
	_, err := db.Exec(sql)
	checkErr(err)
	sql = fmt.Sprintf("create table %s (a int, b int, c int, d int,e int, f int, g varchar(100), h varchar(100) , i varchar(100) , j double,k decimal(30,10));", tableName)
	_, err = db.Exec(sql)
	checkErr(err)
	batchCnt := 2048
	_, err = db.Exec("begin")
	checkErr(err)
	for i := 0; i < num; i++ {
		if i%batchCnt == 0 {
			_, err = db.Exec("commit")
			checkErr(err)
			_, err = db.Exec("begin")
			checkErr(err)
		}
		c := fmt.Sprintf("abcdefghijklmopqrstuvwxyz1234567890987654321abcdefghijklmnopqrstuvwxyz--%v", i)
		d := 1.0 + float64(i)
		sql = fmt.Sprintf("insert into %s values (%v,%v, %v,%v, %v, %v,'%v','%v','%v',%v,%v)", tableName, i, i+1, i+2, i+3, i+4, i+5, c, c, c, d, d+1)
		_, err = db.Exec(sql)
		checkErr(err)

	}
	_, err = db.Exec("commit")
	checkErr(err)
}

func multiTransaction() {
	sessionNum := 2
	dbs := make([]*sql.DB, sessionNum)
	for i := range dbs {
		dbs[i] = getCli()
	}

	sqls := []struct {
		Sql string
		Se  int
	}{
		{"drop table if exists t", 0},
		{"create table t (i int)", 0},
		{"insert into t values (1)", 0},
		{"begin", 0},
		{"insert into t values (10)", 0},
		{"update t set i = i + row_count();", 0},
		{"update t set i = 0 where i=1;", 1},
		{"commit", 0},
	}
	for _, s := range sqls {
		_, err := dbs[s.Se].Exec(s.Sql)
		if err != nil {
			fmt.Printf("err: %s\n", err.Error())
			return
		}
	}
	selectAndPrint(dbs[0], "select * from t")
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

func selectCount(db *sql.DB, sql string) {
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
	if len(actualRows) < 1 || len(actualRows[0]) < 1 {
		return
	}
	num, err := strconv.Atoi(actualRows[0][0].(string))
	if err != nil {
		return
	}
	fmt.Println(num)
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

func checkErr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
