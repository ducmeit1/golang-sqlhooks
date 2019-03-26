package sql

import "fmt"

type Connection struct {
	Host         string
	Port         int
	Protocol     string
	User         string
	Password     string
	DatabaseName string
}

func NewDatabaseServerName(conn Connection) string {
	return fmt.Sprintf("%s:%s@%s(%s:%d)/%s",
		conn.User, conn.Password,
		conn.Protocol,
		conn.Host, conn.Port,
		conn.DatabaseName,
	)
}
