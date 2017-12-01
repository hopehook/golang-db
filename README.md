golang-db mysql 工具使用简介

1 go get github.com/hopehook/golang-db/mysql

2 github.com/go-sql-driver/mysql 请使用 Version 1.3

3 使用
import (
	"github.com/hopehook/golang-db/mysql"
)

DB := mysql.InitMySQLPool(host, database, user, password, charset, maxOpenConns, maxIdleConns)
