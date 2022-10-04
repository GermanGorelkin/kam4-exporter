package db

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/Rican7/retry"
	"github.com/Rican7/retry/strategy"
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
			err = fmt.Errorf("failed to ping database[%d]: %w", attempt, err)
			log.Println(err)
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
		err = fmt.Errorf("failed to open conn to database: %w", err)
		log.Panic(err)
	}
	return Repository{DB: &client{db}}
}
