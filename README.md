# golang-db

### mysql tool introduction

1 go get github.com/hopehook/golang-db/mysql

2 please use go-sql-driver Version 1.3 (github.com/go-sql-driver/mysql)

3 code snippet
```go
import "github.com/hopehook/golang-db/mysql"

DB := mysql.InitMySQLPool(host, database, user, password, charset, maxOpenConns, maxIdleConns)
```
