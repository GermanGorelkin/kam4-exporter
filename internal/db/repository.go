package db

import (
	"context"
	"database/sql"
	"errors"
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
		return nil, err
	}

	rows, err := rep.DB.Query("sellout.GetUserEmail", sql.Named("userID", userID))
	if err != nil {
		return nil, err
	}

	var emails []string

	for rows.Next() {
		var email string
		if err = rows.Scan(&email); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				err = ErrNoRows
			}
			return nil, err
		}
		emails = append(emails, email)
	}

	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(emails) == 0 {
		return nil, ErrNoRows
	}

	return emails, nil
}

func (rep Repository) GetClientName(id int) (string, error) {
	if err := rep.DB.PingContext(context.Background()); err != nil {
		return "", err
	}

	var name string
	if err := rep.DB.QueryRow("[sellout].[GetClientNameByID]", sql.Named("id", id)).Scan(&name); err != nil {
		return "", err
	}

	return name, nil
}
