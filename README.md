# golang-db
db helper for golang.

support list:
  - mysql pool
  - redis pool

redis pool use demo
---
```
package main

import (
	"fmt"

	"github.com/hopehook/golang-db/redis"
)

func main() {
	var REDIS = map[string]string{
		"host":         "127.0.0.1:6379",
		"database":     "0",
		"password":     "",
		"maxOpenConns": "0",
		"maxIdleConns": "0",
	}
	Cache := redis.Init(REDIS)
	Cache.SetString("key", "value")
	str, _ := Cache.GetString("key")
	fmt.Println(str)
}
```

mysql pool use demo
---
```
package main

import (
	"fmt"

	"github.com/hopehook/golang-db/mysql"
)

func main() {
	var MYSQL = map[string]string{
		"host":         "112.74.114.233:3306",
		"database":     "ijuyin_news",
		"user":         "ijuyin",
		"password":     "ijuyin@2015",
		"maxOpenConns": "20",
		"maxIdleConns": "10",
	}
	db := mysql.Init(MYSQL)
	db.Update(`update users set is_deleted = 1 where id = 1`)
	userInfo, _ := db.Query(`select * from users where id = 1`)
	fmt.Println(userInfo)
}
```
