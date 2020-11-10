package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path/filepath"
	"time"

	"github.com/germangorelkin/sql2csv"
)

type PubSub interface {
	Subscribe(func(b []byte) error)
}
type EmailSender interface {
	Send(receivers []string, msg string) error
}
type Repository interface {
	GetDB() *sql.DB
	GetUserEmail(userID int) ([]string, error)
}

type SelloutService struct {
	DB        Repository
	MQ        PubSub
	Email     EmailSender
	FileStore fileStore

	logger *zap.SugaredLogger
}

type fileStore struct {
	path string
	link string
}

type SelloutServiceConfig struct {
	DB       Repository
	MQ       PubSub
	Email    EmailSender
	FilePath string
	FileLink string
	Logger *zap.SugaredLogger
}

func NewSelloutService(cfg SelloutServiceConfig) SelloutService {
	return SelloutService{
		DB:    cfg.DB,
		MQ:    cfg.MQ,
		Email: cfg.Email,
		FileStore: fileStore{
			path: cfg.FilePath,
			link: cfg.FileLink,
		},
		logger: cfg.Logger,
	}
}

type SelloutRequest struct {
	UserId int         `json:"user_id"`
	Param  interface{} `json:"param"`
}

func (srv SelloutService) Run(ctx context.Context) {
	srv.MQ.Subscribe(func(b []byte) error {
		srv.logger.Infof("Received request from MQ: %s", string(b))
		err := srv.handleSellout(ctx, b)
		if err != nil {
			srv.logger.Errorw("Got an error from handleSellout", "err", err)
		}
		srv.logger.Infof("Request processing completed: %s", string(b))
		return nil
	})
}

func (srv SelloutService) handleSellout(ctx context.Context, b []byte) error {
	var req SelloutRequest
	if err := json.Unmarshal(b, &req); err != nil {
		return fmt.Errorf("failed to unmarshal %s: %w", string(b), err)
	}

	email, err := srv.DB.GetUserEmail(req.UserId)
	if err != nil {
		return fmt.Errorf("failed to GetUserEmail(%s): %w", email, err)
	}
	srv.logger.Info("GetUserEmail completed successfully")

	fileName := srv.genUniqueFileName()
	if err := srv.exportData(ctx, req, fileName); err != nil {
		return fmt.Errorf("failed to exportData(%s): %w", fileName, err)
	}
	srv.logger.Info("ExportData completed successfully")

	flink := fmt.Sprintf("%s/%s", srv.FileStore.link, fileName)
	if err = srv.Email.Send(email, flink); err != nil {
		return fmt.Errorf("failed to EmailSend(%s,%s): %w", email, flink, err)
	}
	srv.logger.Info("EmailSend completed successfully")

	return nil
}

func (srv SelloutService) genUniqueFileName() string {
	return fmt.Sprintf("%d.csv", time.Now().UnixNano())
}

func (srv SelloutService) exportData(ctx context.Context, req SelloutRequest, fileName string) (err error) {
	rd := sql2csv.SQLReader{DB: srv.DB.GetDB()}
	rd.Columns = true
	srv.logger.Info("Init SQLReader")

	fpath := filepath.Join(srv.FileStore.path, fileName)
	fd, err := os.Create(fpath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fpath, err)
	}
	srv.logger.Infof("Created file %s", fpath)
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

	param, err := json.Marshal(req.Param)
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %w", req.Param, err)
	}

	query := fmt.Sprintf("exec [api].[Sellout_Export] @userID=%d, @data=N'%s';", req.UserId, string(param))
	srv.logger.Infof("Export query: %s", query)
	err = rd.Read(ctx, query, csvWriter)
	if err != nil {
		return fmt.Errorf("failed to read %s: %w", query, err)
	}

	return nil
}
