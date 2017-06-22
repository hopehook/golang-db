package mysql

import (
	"database/sql"
	"fmt"
	"strconv"

	"github.com/arnehormann/sqlinternals/mysqlinternals"
	_ "github.com/go-sql-driver/mysql"
)

var MYSQL map[string]string = map[string]string{
	"host":         "127.0.0.1:3306",
	"database":     "",
	"user":         "",
	"password":     "",
	"maxOpenConns": "0",
	"maxIdleConns": "0",
}

type SqlConnPool struct {
	DriverName     string
	DataSourceName string
	MaxOpenConns   int64
	MaxIdleConns   int64
	SqlDB          *sql.DB // 连接池
}

var DB *SqlConnPool

func init() {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s)/%s", MYSQL["user"], MYSQL["password"], MYSQL["host"], MYSQL["database"])
	maxOpenConns, _ := strconv.ParseInt(MYSQL["maxOpenConns"], 10, 64)
	maxIdleConns, _ := strconv.ParseInt(MYSQL["maxIdleConns"], 10, 64)

	DB = &SqlConnPool{
		DriverName:     "mysql",
		DataSourceName: dataSourceName,
		MaxOpenConns:   maxOpenConns,
		MaxIdleConns:   maxIdleConns,
	}
	if err := DB.open(); err != nil {
		panic("init db failed")
	}
}

// 封装的连接池的方法
func (p *SqlConnPool) open() error {
	var err error
	p.SqlDB, err = sql.Open(p.DriverName, p.DataSourceName)
	p.SqlDB.SetMaxOpenConns(int(p.MaxOpenConns))
	p.SqlDB.SetMaxIdleConns(int(p.MaxIdleConns))
	return err
}

func (p *SqlConnPool) Close() error {
	return p.SqlDB.Close()
}

func (p *SqlConnPool) Query(queryStr string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := p.SqlDB.Query(queryStr, args...)
	defer rows.Close()
	if err != nil {
		return []map[string]interface{}{}, err
	}
	// 返回属性字典
	columns, err := mysqlinternals.Columns(rows)
	// 获取字段类型
	scanArgs := make([]interface{}, len(columns))
	values := make([]sql.RawBytes, len(columns))
	for i, _ := range values {
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

func (p *SqlConnPool) execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return p.SqlDB.Exec(sqlStr, args...)
}

func (p *SqlConnPool) Update(updateStr string, args ...interface{}) (int64, error) {
	result, err := p.execute(updateStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

func (p *SqlConnPool) Insert(insertStr string, args ...interface{}) (int64, error) {
	result, err := p.execute(insertStr, args...)
	if err != nil {
		return 0, err
	}
	lastid, err := result.LastInsertId()
	return lastid, err

}

func (p *SqlConnPool) Delete(deleteStr string, args ...interface{}) (int64, error) {
	result, err := p.execute(deleteStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

type SqlConnTransaction struct {
	SqlTx *sql.Tx // 单个事务连接
}

//// 开启一个事务
func (p *SqlConnPool) Begin() (*SqlConnTransaction, error) {
	var oneSqlConnTransaction = &SqlConnTransaction{}
	var err error
	if pingErr := p.SqlDB.Ping(); pingErr == nil {
		oneSqlConnTransaction.SqlTx, err = p.SqlDB.Begin()
	}
	return oneSqlConnTransaction, err
}

// 封装的单个事务连接的方法
func (t *SqlConnTransaction) Rollback() error {
	return t.SqlTx.Rollback()
}

func (t *SqlConnTransaction) Commit() error {
	return t.SqlTx.Commit()
}

func (t *SqlConnTransaction) Query(queryStr string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := t.SqlTx.Query(queryStr, args...)
	defer rows.Close()
	if err != nil {
		return []map[string]interface{}{}, err
	}
	// 返回属性字典
	columns, err := mysqlinternals.Columns(rows)
	// 获取字段类型
	scanArgs := make([]interface{}, len(columns))
	values := make([]sql.RawBytes, len(columns))
	for i, _ := range values {
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

func (t *SqlConnTransaction) execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	return t.SqlTx.Exec(sqlStr, args...)
}

func (t *SqlConnTransaction) Update(updateStr string, args ...interface{}) (int64, error) {
	result, err := t.execute(updateStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

func (t *SqlConnTransaction) Insert(insertStr string, args ...interface{}) (int64, error) {
	result, err := t.execute(insertStr, args...)
	if err != nil {
		return 0, err
	}
	lastid, err := result.LastInsertId()
	return lastid, err

}

func (t *SqlConnTransaction) Delete(deleteStr string, args ...interface{}) (int64, error) {
	result, err := t.execute(deleteStr, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

// others
func bytes2RealType(src []byte, columnType string) interface{} {
	srcStr := string(src)
	var result interface{}
	switch columnType {
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
	case "BLOB":
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
