package dumpling

import (
	"fmt"
)

type Config struct {
	Database string
	Host     string
	User     string
	Port     int
	Password string
	Threads  int
}

func (conf *Config) getDSN(db string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", conf.User, conf.Password, conf.Host, conf.Port, db)
}
