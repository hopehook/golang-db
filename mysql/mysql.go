package mysql

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/arnehormann/sqlinternals/mysqlinternals"
	// import and init
	_ "github.com/go-sql-driver/mysql"
)

// SQLConnPool is for DB fd
type SQLConnPool struct {
	DriverName     string
	DataSourceName string
	MaxOpenConns   int64
	MaxIdleConns   int64
	SQLDB          *sql.DB
}

// Init func create DB fd by MYSQL configration map:
//	var MYSQL = map[string]string{
//		"host":         "127.0.0.1:3306",
//		"database":     "",
//		"user":         "",
//		"password":     "",
//		"maxOpenConns": "0",
//		"maxIdleConns": "0",
//	}
func Init(MYSQL map[string]string) *SQLConnPool {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s", MYSQL["user"], MYSQL["password"], MYSQL["host"], MYSQL["database"])
	maxOpenConns, _ := strconv.ParseInt(MYSQL["maxOpenConns"], 10, 64)
	maxIdleConns, _ := strconv.ParseInt(MYSQL["maxIdleConns"], 10, 64)

	DB := &SQLConnPool{
		DriverName:     "mysql",
		DataSourceName: dataSourceName,
		MaxOpenConns:   maxOpenConns,
		MaxIdleConns:   maxIdleConns,
	}
	if err := DB.open(); err != nil {
		panic("init db failed")
	}
	return DB
}

func (p *SQLConnPool) open() error {
	var err error
	p.SQLDB, err = sql.Open(p.DriverName, p.DataSourceName)
	p.SQLDB.SetMaxOpenConns(int(p.MaxOpenConns))
	p.SQLDB.SetMaxIdleConns(int(p.MaxIdleConns))
	return err
}

// Close pool
func (p *SQLConnPool) Close() error {
	return p.SQLDB.Close()
}

// Query via pool
func (p *SQLConnPool) Query(queryStr string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := p.SQLDB.Query(queryStr, args...)
	defer rows.Close()
	if err != nil {
		return []map[string]interface{}{}, err
	}
	columns, err := mysqlinternals.Columns(rows)
	scanArgs := make([]interface{}, len(columns))
	values := make([]sql.RawBytes, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	rowsMap := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		rows.Scan(scanArgs...)
		rowMap := make(map[string]interface{})
		for i, value := range values {
			rowMap[columns[i].Name()] = bytes2RealType(value, columns[i].MysqlType())
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	return rowsMap, nil
}

func (p *SQLConnPool) execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return p.SQLDB.Exec(sqlStr, args...)
}

// Update via pool
func (p *SQLConnPool) Update(updateStr string, args ...interface{}) (int64, error) {
	result, err := p.execute(updateStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// Insert via pool
func (p *SQLConnPool) Insert(insertStr string, args ...interface{}) (int64, error) {
	result, err := p.execute(insertStr, args...)
	if err != nil {
		return 0, err
	}
	lastid, err := result.LastInsertId()
	return lastid, err

}

// Delete via pool
func (p *SQLConnPool) Delete(deleteStr string, args ...interface{}) (int64, error) {
	result, err := p.execute(deleteStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// SQLConnTransaction is for transaction connection
type SQLConnTransaction struct {
	SQLTX *sql.Tx
}

// Begin transaction
func (p *SQLConnPool) Begin() (*SQLConnTransaction, error) {
	var oneSQLConnTransaction = &SQLConnTransaction{}
	var err error
	if pingErr := p.SQLDB.Ping(); pingErr == nil {
		oneSQLConnTransaction.SQLTX, err = p.SQLDB.Begin()
	}
	return oneSQLConnTransaction, err
}

// Rollback transaction
func (t *SQLConnTransaction) Rollback() error {
	return t.SQLTX.Rollback()
}

// Commit transaction
func (t *SQLConnTransaction) Commit() error {
	return t.SQLTX.Commit()
}

// Query via transaction
func (t *SQLConnTransaction) Query(queryStr string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := t.SQLTX.Query(queryStr, args...)
	defer rows.Close()
	if err != nil {
		return []map[string]interface{}{}, err
	}
	columns, err := mysqlinternals.Columns(rows)
	scanArgs := make([]interface{}, len(columns))
	values := make([]sql.RawBytes, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	rowsMap := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		rows.Scan(scanArgs...)
		rowMap := make(map[string]interface{})
		for i, value := range values {
			rowMap[columns[i].Name()] = bytes2RealType(value, columns[i].MysqlType())
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	return rowsMap, nil
}

func (t *SQLConnTransaction) execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return t.SQLTX.Exec(sqlStr, args...)
}

// Update via transaction
func (t *SQLConnTransaction) Update(updateStr string, args ...interface{}) (int64, error) {
	result, err := t.execute(updateStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// Insert via transaction
func (t *SQLConnTransaction) Insert(insertStr string, args ...interface{}) (int64, error) {
	result, err := t.execute(insertStr, args...)
	if err != nil {
		return 0, err
	}
	lastid, err := result.LastInsertId()
	return lastid, err

}

// Delete via transaction
func (t *SQLConnTransaction) Delete(deleteStr string, args ...interface{}) (int64, error) {
	result, err := t.execute(deleteStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// bytes2RealType is to convert db type to code type
func bytes2RealType(src []byte, columnType string) interface{} {
	srcStr := string(src)
	var result interface{}
	switch columnType {
	case "BIT":
		fallthrough
	case "TINYINT":
		fallthrough
	case "SMALLINT":
		fallthrough
	case "INT":
		fallthrough
	case "BIGINT":
		result, _ = strconv.ParseInt(srcStr, 10, 64)
	case "CHAR":
		fallthrough
	case "VARCHAR":
		fallthrough
	case "TINY TEXT":
		fallthrough
	case "TEXT":
		fallthrough
	case "MEDIUM TEXT":
		fallthrough
	case "LONG TEXT":
		fallthrough
	case "TINY BLOB":
		fallthrough
	case "MEDIUM BLOB":
		fallthrough
	case "BLOB":
		fallthrough
	case "LONG BLOB":
		fallthrough
	case "JSON":
		fallthrough
	case "ENUM":
		fallthrough
	case "SET":
		fallthrough
	case "YEAR":
		fallthrough
	case "DATE":
		fallthrough
	case "TIME":
		fallthrough
	case "TIMESTAMP":
		fallthrough
	case "DATETIME":
		result = srcStr
	case "FLOAT":
		fallthrough
	case "DOUBLE":
		fallthrough
	case "DECIMAL":
		result, _ = strconv.ParseFloat(srcStr, 64)
	default:
		result = nil
	}
	return result
}
