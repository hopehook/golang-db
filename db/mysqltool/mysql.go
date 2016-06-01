package mysqltool

import (
	"database/sql"
	//"database/sql/driver"
	"errors"

	"github.com/arnehormann/sqlinternals/mysqlinternals"
	_ "github.com/go-sql-driver/mysql"
	//"reflect"
	"strconv"
)

type SqlConn struct {
	DriverName     string
	DataSourceName string
	MaxOpenConns   int64
	MaxIdleConns   int64
	ManulCommit    bool
	SqlDb          *sql.DB
	SqlTx          *sql.Tx
}

var MySQLConn *SqlConn

func InitMysql(dataSourceName string, maxIdleConns, maxOpenConns int64) error {
	driverName := "mysql"
	MySQLConn = &SqlConn{DriverName: driverName, DataSourceName: dataSourceName,
		MaxOpenConns: maxOpenConns, MaxIdleConns: maxIdleConns, ManulCommit: false}
	err := MySQLConn.open()
	return err
}

func (p *SqlConn) open() error {
	// 默认是autocommit方式
	var err error
	p.SqlDb, err = sql.Open(p.DriverName, p.DataSourceName)
	p.SqlDb.SetMaxOpenConns(int(p.MaxOpenConns))
	p.SqlDb.SetMaxIdleConns(int(p.MaxIdleConns))
	return err
}

func (p *SqlConn) Transaction() error {
	if p.ManulCommit {
		return errors.New("Operate Invalid! Reason:you are trying create the second transaction.")
	}
	if pingErr := p.SqlDb.Ping(); pingErr != nil {
		return pingErr
	}
	var err error
	p.SqlTx, err = p.SqlDb.Begin()
	if err == nil {
		p.ManulCommit = true
	}
	return err
}

func (p *SqlConn) Query(query string, args ...interface{}) ([]map[string]interface{}, error) {

	if pingErr := p.SqlDb.Ping(); pingErr != nil {
		return []map[string]interface{}{}, pingErr
	}

	rows, err := p.SqlDb.Query(query, args...)
	if err != nil {
		return []map[string]interface{}{}, err
	}
	defer rows.Close()
	// 返回属性字典
	columns, err := mysqlinternals.Columns(rows)
	if err != nil {
		return []map[string]interface{}{}, err
	}
	// 获取字段类型
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i, _ := range values {
		scanArgs[i] = &values[i]
	}
	rowsMap := make([]map[string]interface{}, 0, 10)
	for rows.Next() {
		rows.Scan(scanArgs...)
		rowMap := make(map[string]interface{})
		for i, value := range values {
			rowMap[columns[i].Name()] = bytes2RealType(value, columns[i].MysqlType())
			//fmt.Println(columns[i].Name(), columns[i].MysqlType(), reflect.ValueOf(rowMap[columns[i].Name()]), rowMap[columns[i].Name()])
		}
		rowsMap = append(rowsMap, rowMap)
	}
	if err = rows.Err(); err != nil {
		return []map[string]interface{}{}, err
	}
	return rowsMap, nil
}

func (p *SqlConn) execute(sqlStr string, args ...interface{}) (sql.Result, error) {
	if pingErr := p.SqlDb.Ping(); pingErr != nil {
		return nil, pingErr
	}
	var err error
	var result sql.Result
	if p.ManulCommit {
		result, err = p.SqlTx.Exec(sqlStr, args...)
	} else {
		result, err = p.SqlDb.Exec(sqlStr, args...)
	}
	return result, err
}

func (p *SqlConn) Update(update string, args ...interface{}) (int64, error) {

	result, err := p.execute(update, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

func (p *SqlConn) Insert(insert string, args ...interface{}) (int64, error) {
	result, err := p.execute(insert, args...)
	if err != nil {
		return 0, err
	}
	lastid, err := result.LastInsertId()
	return lastid, err

}

func (p *SqlConn) Delete(delete string, args ...interface{}) (int64, error) {
	result, err := p.execute(delete, args...)
	if err != nil {
		return 0, err
	}
	affect, err := result.RowsAffected()
	return affect, err
}

func (p *SqlConn) Rollback() error {
	if !p.ManulCommit {
		return errors.New("Rollback Invalid! Reason:you didn't create a transaction.")
	}
	if pingErr := p.SqlDb.Ping(); pingErr != nil {
		return pingErr
	}
	err := p.SqlTx.Rollback()
	if err == nil {
		p.ManulCommit = false
	}
	return err
}

func (p *SqlConn) Commit() error {
	if !p.ManulCommit {
		return errors.New("Commit Invalid! Reason:you didn't create a transaction.")
	}
	if pingErr := p.SqlDb.Ping(); pingErr != nil {
		return pingErr
	}
	err := p.SqlTx.Commit()
	if err == nil {
		p.ManulCommit = false
	}
	return err

}

func (p *SqlConn) Close() error {
	var err error
	if err = p.SqlDb.Ping(); err == nil {
		if p.ManulCommit {
			p.Commit()
		}
		err = p.SqlDb.Close()
	}
	return err
}

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
