# golang-db

db pool helper for golang, welcome everyone contribute code, your name will
in CONTRIBUTORS.


### TODO

 * memcache db pool helper
 * mongo db pool helper

  ... ... and more


### how to use mysql pool helper

1 go get github.com/hopehook/golang-db/mysql

2 go get github.com/go-sql-driver/mysql

Note: please use go-sql-driver Version >= 1.4

3 code snippet
```go
import "github.com/hopehook/golang-db/mysql"

// get a mysql db pool as global variable
DB := mysql.InitMySQLPool(host, database, user, password, charset, maxOpenConns, maxIdleConns)

// use helper function
data, err := DB.Query(`select * from table limit 10`)
... ...

// use transaction
TX, _ := DB.Begin()
defer TX.Rollback()
TX.Exec(`delete from table where id = 1`)
TX.Commit()

// if you want to use golang own function, please get DB.SQLDB as your db pool variable
SQLDB := DB.SQLDB
SQLDB.Exec(`delete from table where id = 1`)

// use golang own transaction
TX, _ := SQLDB.Begin()
defer TX.Rollback()
TX.Exec(`delete from table where id = 1`)
TX.Commit()
```
