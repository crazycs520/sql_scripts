package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
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
	//	create()
	// create2()
	//	execSqlFromFile()
	//	testInsertBigJson()
	go prepareData(200000)
	go update("t_slim", "c0",0,10000,time.Minute)

	time.Sleep(time.Minute)
	// fixTableWide(2000*10000 - 6977899,4000,200,"t_wide")
	// fmt.Printf("sleeping...\n\n")
	// time.Sleep(60 * time.Second)
	//	time.Sleep(60 * time.Second)
	// testAddIndexByCnt(0, 2)
	//	testAddIndexByBatch(0,5)
	// cleanIndex("t_wide")
	// cleanIndex("t_slim")
	// multiTransaction()
	//addIndex(10, "t_wide")
	//createData(100)
	//createDataSlim(10)
	// selectCount(db, "select count(*) from t1 where a=1 and b=3;")
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randSeq(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func prepareData(num int) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		testCreateTable(num, 4000, 200, "t_wide", false)
	}()

	go func() {
		defer wg.Done()
		testCreateTable(num, 4000, 10, "t_slim",false)
	}()
	wg.Wait()
}


func update(tName , colName string, small, max int, t time.Duration) {
	db := getCli()
	tick := time.After(t)
	num := 0
	for {
		select {
		case <- tick:
			fmt.Printf("\nupdate %d rows\n", num)
			return
		default:
		}
		n := rand.Intn(max-small) + small

		sql := fmt.Sprintf(" update %s set %[2]s=%d where %[2]s=%d;", tName, colName, n, n+1)
		_, err := db.Exec(sql)
		checkErr(err)
		num++
	}
}

func testCreateTable(num, batchCnt, colNum int, tableName string, create bool) {
	fmt.Printf("------\nstart to create table: %v, insert data: %v, column number: %v\n", tableName, num, colNum)
	startTime := time.Now()
	db := getCli()
	var err error
	intColNum := colNum / 3
	varCharColNum := (colNum - intColNum) / 2
	dateColNum := colNum - intColNum - varCharColNum
	if create {
		sql := fmt.Sprintf("drop table if exists %s", tableName)
		_, err := db.Exec(sql)
		checkErr(err)

		_, err = db.Exec("set @@tidb_wait_table_split_finish=1")
		checkErr(err)
		sql = fmt.Sprintf("create table %s (", tableName)
		intColNum := colNum / 3
		varCharColNum := (colNum - intColNum) / 2
		dateColNum := colNum - intColNum - varCharColNum

		i := 0
		for ; i < intColNum; i++ {
			if i > 0 {
				sql += ", "
			}
			sql = sql + fmt.Sprintf("c%d int", i)
		}
		ColNum := intColNum + varCharColNum
		for ; i < ColNum; i++ {
			sql = sql + fmt.Sprintf(", c%d varchar(200)", i)
		}
		ColNum = intColNum + varCharColNum + dateColNum
		for ; i < ColNum; i++ {
			sql = sql + fmt.Sprintf(", c%d timestamp", i)
		}
		sql += ")"
		//sql += " SHARD_ROW_ID_BITS = 3, PRE_SPLIT_REGIONS = 3;"

		_, err = db.Exec(sql)
		checkErr(err)
		fmt.Printf("create table spend %v s\n----------------->\n\n", time.Since(startTime).Seconds())
	}


	startTime = time.Now()
	defer func() {
		fmt.Printf("insert %v spend %v s\n----------------->\n\n", num, time.Since(startTime).Seconds())
	}()

	insertFunc := func(start, end, batchCnt int) {
		db := getCli()
		sql1 := ""
		ColNum := 0
		_, err = db.Exec("begin")
		checkErr(err)
		for value := start; value < end; value++ {
			if value%batchCnt == 0 {
				_, err = db.Exec("commit")
				checkErr(err)
				_, err = db.Exec("begin")
				checkErr(err)
			}

			sql1 = fmt.Sprintf("insert into %s values (", tableName)
			i := 0
			for ; i < intColNum; i++ {
				if i > 0 {
					sql1 += ", "
				}
				sql1 = sql1 + fmt.Sprintf("%d", value)
			}
			ColNum = intColNum + varCharColNum
			for ; i < ColNum; i++ {
				sql1 = sql1 + fmt.Sprintf(`, "abcdefgabcdefgabcdefgabcdefgabcdefgabcdefghijklmnopqrstuvwxyz-%d"`, value)
			}
			ColNum = intColNum + varCharColNum + dateColNum
			now := time.Unix(time.Now().Unix()+rand.Int63n(int64(value)+24*60*60*30), 0)
			for ; i < ColNum; i++ {
				sql1 = sql1 + fmt.Sprintf(`, "%s"`, now.Format("2006-01-02 15:04:05"))
			}
			sql1 += ")"
			_, err = db.Exec(sql1)
			checkErr(err)
		}
		_, err = db.Exec("commit")
		checkErr(err)
	}

	parallel := 10
	avgNum := num / 10
	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		start := i * avgNum
		end := (i + 1) * avgNum
		wg.Add(1)
		go func(start, end int, batch int) {
			batchSize := batchCnt/2 + rand.Intn(batchCnt/2)
			insertFunc(start, end, batchSize)
			wg.Done()
		}(start, end, batchCnt)
	}
	wg.Wait()
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

	sqls := []string{
		//"drop table if exists t1,t2",
		"drop table if exists tid1,tid2",

		//"create table t1 (id int, name varchar(30),addr varchar(30), course varchar(30))",
		//"create table t2 (id int, name varchar(30),addr varchar(30), course varchar(30))",
		"create table tid1 (id int) charset=utf8",
		//"create table tid2 (id int)",
	}

	for _, sql := range sqls {
		_, err := db.Exec(sql)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}
	/*
		numRow := 10000
		startNum := 0
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
	*/
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
	fmt.Println(sql)
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
			//		return
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
