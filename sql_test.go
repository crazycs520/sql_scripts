package main

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/pingcap/schrodinger-test/util"
)

func getDBCli() *sql.DB {
	dbAddr := "127.0.0.1:4000"
	dbDSN := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, "test")
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	db.SetMaxIdleConns(8)
	return db
}

func BenchmarkSelect(b *testing.B) {
	db := getDBCli()
	if db == nil {
		return
	}
	sql := "select t1.* from t1 inner join tid1 where t1.id=tid1.id and tid1.id < 1"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(sql)
		if err == nil {
			rows.Close()
		}
	}
}

func TestSimpleSQL(t *testing.T) {

	dbAddr := "127.0.0.1:4000"
	dbDSN := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, "test")
	db, err := util.OpenDB(dbDSN, 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	db.SetMaxIdleConns(8)

	sql := "alter table t2 rename t1"
	// execute SQL
	result, err := db.Exec(sql)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(result)
	fmt.Println(sql)

}

func TestExecuteSQL(t *testing.T) {

	dbAddr := "127.0.0.1:4000"
	dbDSN := fmt.Sprintf("root:@tcp(%s)/%s", dbAddr, "test")
	db, err := util.OpenDB(dbDSN, 10)
	db.SetMaxIdleConns(8)

	// build SQL
	sql := "select * from t_json"
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
				result[i] = trimValue(0, raw)
			}
		}

		actualRows = append(actualRows, result)
	}
	if rows.Err() != nil {
		return
	}

	actualRowsMap := make(map[string]int)
	for _, row := range actualRows {
		rowString := ""
		for _, col := range row {
			rowString += fmt.Sprintf("%v,", col)
		}
		_, ok := actualRowsMap[rowString]
		if !ok {
			actualRowsMap[rowString] = 0
		}
		actualRowsMap[rowString]++
		fmt.Println(rowString)
	}
	fmt.Printf("%v\n", actualRowsMap)
}

func trimValue(tp int, val []byte) string {
	// a='{"DnOJQOlx":52,"ZmvzPtdm":82}'
	// eg: set a={"a":"b","b":"c"}
	//     get a={"a": "b", "b": "c"} , so have to remove the space

	for i := 1; i < len(val)-2; i++ {
		if val[i-1] == '"' && val[i] == ':' && val[i+1] == ' ' {
			val = append(val[:i+1], val[i+2:]...)
		}
		if val[i-1] == ',' && val[i] == ' ' && val[i+1] == '"' {
			val = append(val[:i], val[i+1:]...)
		}
	}

	return string(val)
}
