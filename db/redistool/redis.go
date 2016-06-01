package redistool

import (
	"errors"
	"github.com/garyburd/redigo/redis"
	"time"
)

var RedisConn *RedisPool

type RedisPool struct {
	pool *redis.Pool
}

// redis连接池，10个redis连接对象
func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func InitRedis(server, password string) error {
	RedisConn = &RedisPool{}
	RedisConn.pool = newPool(server, password)
	if RedisConn.pool == nil {
		return errors.New("redis初始化失败！")
	}
	return nil
}

func CloseRedis() error {
	err := RedisConn.pool.Close()
	return err
}

func (p *RedisPool) GetString(db int, key string) (string, error) {
	conn := p.pool.Get()
	defer conn.Close()
	conn.Do("select", db)
	return redis.String(conn.Do("GET", key))
}

func (p *RedisPool) GetInt(db int, key string) (int, error) {
	conn := p.pool.Get()
	defer conn.Close()
	conn.Do("select", db)
	return redis.Int(conn.Do("GET", key))
}

func (p *RedisPool) GetInt64(db int, key string) (int64, error) {
	conn := p.pool.Get()
	defer conn.Close()
	conn.Do("select", db)
	return redis.Int64(conn.Do("GET", key))
}

func (p *RedisPool) Do(db int, command string, args ...interface{}) (interface{}, error) {
	conn := p.pool.Get()
	defer conn.Close()
	conn.Do("select", db)
	return conn.Do(command, args...)
}
