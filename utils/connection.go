package utils

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
)

func newDBConn(host string, port int, user, password string) (*sql.DB, error) {
	cfg := mysql.Config{
		User:                    user,
		Passwd:                  password,
		Net:                     "tcp",
		Addr:                    fmt.Sprintf("%s:%d", host, port),
		DBName:                  "mysql",
		Params:                  nil,
		Collation:               "",
		Loc:                     nil,
		MaxAllowedPacket:        25 << 20,
		ServerPubKey:            "",
		TLSConfig:               "",
		Timeout:                 0,
		ReadTimeout:             0,
		WriteTimeout:            0,
		AllowAllFiles:           false,
		AllowCleartextPasswords: true,
		AllowNativePasswords:    true,
		AllowOldPasswords:       true,
		ClientFoundRows:         false,
		ColumnsWithAlias:        false,
		InterpolateParams:       false,
		MultiStatements:         false,
		ParseTime:               false,
		RejectReadOnly:          false,
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(1)
	return db, nil
}
