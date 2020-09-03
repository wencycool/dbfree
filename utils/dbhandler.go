package utils

import "database/sql"

type DBHandler struct {
	conn *sql.DB
}
