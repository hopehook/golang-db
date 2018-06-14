package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
)

// SQLConnPool is DB pool struct
type SQLConnPool struct {
	DriverName     string
	DataSourceName string
	MaxOpenConns   int
	MaxIdleConns   int
	SQLDB          *sql.DB
}

// InitMySQLPool func init DB pool
func InitMySQLPool(host, database, user, password, charset string, maxOpenConns, maxIdleConns int) *SQLConnPool {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=%s&autocommit=true", user, password, host, database, charset)
	db := &SQLConnPool{
		DriverName:     "mysql",
		DataSourceName: dataSourceName,
		MaxOpenConns:   maxOpenConns,
		MaxIdleConns:   maxIdleConns,
	}
	if err := db.Open(); err != nil {
		log.Panicln("Init mysql pool failed.", err.Error())
	}
	return db
}

func (p *SQLConnPool) Open() error {
	var err error
	p.SQLDB, err = sql.Open(p.DriverName, p.DataSourceName)
	if err != nil {
		return err
	}
	if err = p.SQLDB.Ping(); err != nil {
		return err
	}
	p.SQLDB.SetMaxOpenConns(p.MaxOpenConns)
	p.SQLDB.SetMaxIdleConns(p.MaxIdleConns)
	return nil
}

// Close pool
func (p *SQLConnPool) Close() error {
	return p.SQLDB.Close()
}

// Get via pool
func (p *SQLConnPool) Get(queryStr string, args ...interface{}) (map[string]interface{}, error) {
	results, err := p.Query(queryStr, args...)
	if err != nil {
		return map[string]interface{}{}, err
	}
	if len(results) <= 0 {
		return map[string]interface{}{}, sql.ErrNoRows
	}
	if len(results) > 1 {
		return map[string]interface{}{}, errors.New("sql: more than one rows")
	}
	return results[0], nil
}

// Query via pool
func (p *SQLConnPool) Query(queryStr string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := p.SQLDB.Query(queryStr, args...)
	if err != nil {
		log.Println(err)
		return []map[string]interface{}{}, err
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
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
			rowMap[columns[i].Name()] = bytes2RealType(value, columns[i])
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	return rowsMap, nil
}

func (p *SQLConnPool) Exec(sqlStr string, args ...interface{}) (sql.Result, error) {
	return p.SQLDB.Exec(sqlStr, args...)
}

// Update via pool
func (p *SQLConnPool) Update(updateStr string, args ...interface{}) (int64, error) {
	result, err := p.Exec(updateStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// Insert via pool
func (p *SQLConnPool) Insert(insertStr string, args ...interface{}) (int64, error) {
	result, err := p.Exec(insertStr, args...)
	if err != nil {
		return 0, err
	}
	lastId, err := result.LastInsertId()
	return lastId, err

}

// Delete via pool
func (p *SQLConnPool) Delete(deleteStr string, args ...interface{}) (int64, error) {
	result, err := p.Exec(deleteStr, args...)
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

// Get via transaction
func (t *SQLConnTransaction) Get(queryStr string, args ...interface{}) (map[string]interface{}, error) {
	results, err := t.Query(queryStr, args...)
	if err != nil {
		return map[string]interface{}{}, err
	}
	if len(results) <= 0 {
		return map[string]interface{}{}, sql.ErrNoRows
	}
	if len(results) > 1 {
		return map[string]interface{}{}, errors.New("sql: more than one rows")
	}
	return results[0], nil
}

// Query via transaction
func (t *SQLConnTransaction) Query(queryStr string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := t.SQLTX.Query(queryStr, args...)
	if err != nil {
		log.Println(err)
		return []map[string]interface{}{}, err
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
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
			rowMap[columns[i].Name()] = bytes2RealType(value, columns[i])
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	return rowsMap, nil
}

func (t *SQLConnTransaction) Exec(sqlStr string, args ...interface{}) (sql.Result, error) {
	return t.SQLTX.Exec(sqlStr, args...)
}

// Update via transaction
func (t *SQLConnTransaction) Update(updateStr string, args ...interface{}) (int64, error) {
	result, err := t.Exec(updateStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// Insert via transaction
func (t *SQLConnTransaction) Insert(insertStr string, args ...interface{}) (int64, error) {
	result, err := t.Exec(insertStr, args...)
	if err != nil {
		return 0, err
	}
	lastId, err := result.LastInsertId()
	return lastId, err

}

// Delete via transaction
func (t *SQLConnTransaction) Delete(deleteStr string, args ...interface{}) (int64, error) {
	result, err := t.Exec(deleteStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// bytes2RealType is to convert db type to code type
func bytes2RealType(src []byte, column *sql.ColumnType) interface{} {
	srcStr := string(src)
	var result interface{}
	switch column.DatabaseTypeName() {
	case "BIT", "TINYINT", "SMALLINT", "INT":
		result, _ = strconv.ParseInt(srcStr, 10, 64)
	case "BIGINT":
		result, _ = strconv.ParseUint(srcStr, 10, 64)
	case "CHAR", "VARCHAR",
		"TINY TEXT", "TEXT", "MEDIUM TEXT", "LONG TEXT",
		"TINY BLOB", "MEDIUM BLOB", "BLOB", "LONG BLOB",
		"JSON", "ENUM", "SET",
		"YEAR", "DATE", "TIME", "TIMESTAMP", "DATETIME":
		result = srcStr
	case "FLOAT", "DOUBLE", "DECIMAL":
		result, _ = strconv.ParseFloat(srcStr, 64)
	default:
		result = nil
	}
	return result
}
