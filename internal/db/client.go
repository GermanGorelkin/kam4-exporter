package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
	"github.com/sirupsen/logrus"

	_ "github.com/denisenkom/go-mssqldb"
)

type client struct {
	*sql.DB
}

func (c *client) PingContext(ctx context.Context) error {
	// попытки переподключится
	return retry.Retry(func(attempt uint) error {
		err := c.DB.PingContext(ctx)
		if err != nil {
			err = fmt.Errorf("Error pinging database[%d]: %s\n", attempt, err)
			logrus.Errorln(err)
		}
		return err
	},
		// пауза между попытками
		strategy.Wait(time.Second*5),
		// кол-во попыток
		//strategy.Limit(10)
	)
}

func NewRepository(connString string) Repository {
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		err = fmt.Errorf("Error open conn to database: %w\n", err)
		logrus.Panic(err)
	}
	return Repository{DB: &client{db}}
}
