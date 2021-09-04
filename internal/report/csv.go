package report

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/germangorelkin/sql2csv"
	"go.uber.org/zap"
)

type Repository interface {
	GetDB() *sql.DB
}

type CSVReport struct {
	DB Repository

	logger *zap.SugaredLogger
}

type CSVReportConfig struct {
	DB     Repository
	Logger *zap.SugaredLogger
}

func NewCSVReport(cfg CSVReportConfig) CSVReport {
	return CSVReport{
		DB:     cfg.DB,
		logger: cfg.Logger,
	}
}

func (srv CSVReport) FileExtension() string {
	return ".csv"
}

func (srv CSVReport) Build(ctx context.Context, filePath string, sqlQuery string) (err error) {
	rd := sql2csv.SQLReader{DB: srv.DB.GetDB()}
	rd.Columns = true
	srv.logger.Info("Init SQLReader")

	fd, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}
	srv.logger.Infof("Created file %s", filePath)
	defer func() {
		if cErr := fd.Close(); cErr != nil {
			err = cErr
		}
	}()

	csvWriter := sql2csv.NewCSVWriter([]byte(";"), []byte("\r\n"), fd)
	srv.logger.Info("Init NewCSVWriter")
	if err := csvWriter.AddBOM(); err != nil {
		return fmt.Errorf("failed to AddBOM: %w", err)
	}
	srv.logger.Info("Added BOM")

	srv.logger.Infof("Export query: %s", sqlQuery)
	err = rd.Read(ctx, sqlQuery, csvWriter)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", sqlQuery, err)
	}

	return nil
}
