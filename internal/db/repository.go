package db

import (
	"context"
	"database/sql"
	"errors"
	"github.com/sirupsen/logrus"
)

var (
	ErrNoRows = errors.New("sql: no rows in result set")
)

type Repository struct {
	DB *client
}

func (rep Repository) GetDB() *sql.DB {
	return rep.DB.DB
}


func (rep Repository) GetUserEmail(userID int) ([]string, error) {
	if err := rep.DB.PingContext(context.Background()); err != nil {
		logrus.Panic(err)
	}

	rows, err := rep.DB.Query("sellout.GetUserEmail", sql.Named("userID", userID))
	if err != nil {
		return nil, err
	}

	var emails []string

	for rows.Next() {
		var email string
		if err = rows.Scan(&email); err != nil {
			return nil, err
		}
		emails = append(emails, email)
	}

	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return emails, nil
}
